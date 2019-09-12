package br.ufmg.cs.systems.fractal.computation

import br.ufmg.cs.systems.fractal.aggregation._
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

trait SparkMasterEngine [E <: Subgraph]
    extends CommonMasterExecutionEngine with Logging {

  import SparkMasterEngine._

  /** Initial configurations
   *  The following describe general engine parameters
   */
  
  lazy val step: Int = parentOpt.map(_.step + 1).getOrElse(0)

  var sc: SparkContext = _

  def config: SparkConfiguration[E]

  var mutableConfigBc: Broadcast[SparkConfiguration[E]] = _

  def configBc: Broadcast[SparkConfiguration[E]] = {
    if (mutableConfigBc == null) {
      mutableConfigBc = sc.broadcast(config)
    }
    mutableConfigBc
  }

  def parentOpt: Option[SparkMasterEngine[E]]

  var masterComputation: MasterComputation = _

  var isComputationHalted: Boolean = false

  /* */

  /** Computation State
   *  The following fields are set by specialized engines as an output state
   *  w.r.t a step
   */

  var storageLevel: StorageLevel = StorageLevel.NONE

  var stepRDD: RDD[Unit] = _

  var aggAccums: Map[String,LongAccumulator] = _

  var previousAggregationsBc: Broadcast[_] = _

  var aggregations
    : Map[String,AggregationStorage[_ <: Writable, _ <: Writable]] = _

  /* */

  def init(): Unit = {
    if (!config.isInitialized()) {
      config.initialize(isMaster = true)
    }

    // set log level
    logInfo (s"Setting log level to ${config.getLogLevel}")
    setLogLevel (config.getLogLevel)
    sc.setLogLevel (config.getLogLevel.toUpperCase)
    logInfo (s"Setting num_partitions to " +
      s"${config.confs.get("num_partitions").getOrElse(sc.defaultParallelism)}")
    config.setIfUnset ("num_partitions", sc.defaultParallelism)
    config.setHadoopConfig (sc.hadoopConfiguration)

    // garantees that outputPath does not exist
    if (config.isOutputActive) {
      val fs = FileSystem.get(sc.hadoopConfiguration)
      val outputPath = new Path(s"${config.getOutputPath}/*")
      if (fs.exists (outputPath))
        throw new RuntimeException (
          s"Output path ${config.getOutputPath} exists. Choose another one."
          )
    }

    // master computation
    masterComputation = config.createMasterComputation()
    masterComputation.setUnderlyingExecutionEngine(this)
    masterComputation.init()

    // master must know aggregators metadata
    val computation = config.createComputation [E]
    var currComp = computation
    while (currComp != null) {
      currComp.initAggregations(config)
      currComp = currComp.nextComputation()
    }

    // default accumulators
    aggAccums = Map.empty
    aggAccums.update (AGG_SUBGRAPHS_OUTPUT,
      sc.longAccumulator (AGG_SUBGRAPHS_OUTPUT))

    // set initial state
    parentOpt match {
      case Some(parent) =>
        // start with parent's state
        stepRDD = parent.stepRDD
        previousAggregationsBc = parent.previousAggregationsBc
        aggregations = Map() ++ parent.aggregations

      case None =>
        // step rdd
        stepRDD = sc.makeRDD(
          Seq.empty[Unit], numPartitions
        ).persist(storageLevel)

        // previous aggregation
        previousAggregationsBc = sc.broadcast (
          Map.empty[String,AggregationStorage[_ <: Writable, _ <: Writable]]
        )

        aggregations = Map()
    }
  }

  override def haltComputation() = {
    logInfo ("Halting master computation")
    isComputationHalted = true
  }

  /**
   * Computation cleaning. It does nothing by default.
   */
  def finalizeComputation() = {}

  /**
   * Select a storage level for this computation
   */
  def persist(sl: StorageLevel): this.type = {
    storageLevel = sl
    this
  }

  /**
   * Master's computation takes place here, step by step
   */
  def compute(): SparkMasterEngine[E] = {
    logInfo (s"Computing remaining steps of computation ${this}")
    var currMasterEngine: SparkMasterEngine[E] = this
    while (currMasterEngine.next) {
      currMasterEngine = SparkMasterEngine [E] (sc, config, currMasterEngine)
    }
    currMasterEngine
  }

  /**
   * Compute the step referring to this computation
   *
   * @return true if there are more steps with this computation or false
   * otherwise
   */
  def next: Boolean

  def numPartitions: Int = config.numPartitions

  def getNumberPartitions: Int = numPartitions

  override def getStep(): Long = step

  /**
   * Merges or replaces the aggregations for the next step. We can have one
   * of the following scenarios:
   * (1) In any step we are interested in all aggregations seen so far.
   *     Thus, the aggregations are incrementally composed.
   * (2) In any step we are interested only in the previous
   *     aggregations. Thus, we discard the old aggregations and replace it with
   *     the new aggregations for the next step.
   *
   *  @param aggregations current aggregations
   *  @param previousAggregations aggregations found in the step that just
   *  finished
   *
   *  @return the new choice for aggregations obtained by composing or replacing
   */
  def mergeOrReplaceAggregations (
      aggregations: Map[String,
        AggregationStorage[_ <: Writable, _ <: Writable]],
      previousAggregations: Map[String,
        AggregationStorage[_ <: Writable, _ <: Writable]])
    : Map[String,AggregationStorage[_ <: Writable,_ <: Writable]] = {
    def aggregate[K <: Writable, V <: Writable](agg1: AggregationStorage[K,V],
      agg2: AggregationStorage[_,_]) = {
        agg1.aggregate (agg2.asInstanceOf[AggregationStorage[K,V]])
        agg1
    }
    logInfo (s"CurrentAggregations = ${aggregations};" +
      s" PreviousAggregations=${previousAggregations}")
    // we compose all entries
    previousAggregations.foreach { case (name, agg) =>
      aggregations.get(name) match {
        case Some(_agg) if _agg.isIncremental =>
          aggregate(_agg, agg)
        case _ =>
          aggregations.update (name, agg)
      }
    }
    aggregations
  }

  /**
   * Extracts and aggregate AggregationStorages from executionEngines
   *
   * @param execEngines rdd of spark execution engines
   * @param numPartitions based on the number of partitions, we decide the
   * depth of the aggregation tree
   * @return a future with a map (name -> aggregationStorage) as entries
   *
   */
  def getAggregations(
    execEngines: RDD[SparkEngine[E]],
    numPartitions: Int) = Future {

    def reduce[K <: Writable, V <: Writable](
        name: String,
        metadata: AggregationStorageMetadata[K,V])
      (implicit kt: ClassTag[K], vt: ClassTag[V]) =
        Future[AggregationStorage[_ <: Writable, _ <: Writable]] {

      import SparkConfiguration._

      val keyValues = execEngines.flatMap (
        execEngine => execEngine.flushAggregationsByName[K,V](name)
      ).partitionBy(new HashPartitioner(execEngines.partitions.length)).values

      val emptyAggregation = {
        val _configBc = configBc
        val factory = new AggregationStorageFactory(_configBc.value)
        factory.createAggregationStorage(name).
          asInstanceOf[AggregationStorage[K,V]]
      }

      val aggStorage = keyValues.mapPartitions { aggBinIter =>
        if (aggBinIter.hasNext) {
          var aggStorage = deserialize[AggregationStorage[K,V]](aggBinIter.next)
          while (aggBinIter.hasNext) {
            val agg = deserialize[AggregationStorage[K,V]](aggBinIter.next)
            aggStorage.aggregate(agg)
          }
          Iterator(aggStorage)
        } else {
          Iterator.empty
        }
      }.fold (emptyAggregation) { (agg1, agg2) =>
        agg1.aggregate(agg2)
        agg1
      }

      aggStorage.endedAggregation
      aggStorage
    }

    val aggregations = Map.empty[
      String,AggregationStorage[_ <: Writable, _ <: Writable]
    ]

    val future = Future.sequence (
      config.getAggregationsMetadata.map { case (name, metadata) =>
        reduce (name, metadata)
      }
    )

    Await.ready (future, Duration.Inf)
    future.value.get match {
      case Success(aggStorages) =>
        aggStorages.foreach (aggStorage =>
            aggregations.update (aggStorage.getName, aggStorage))
      case Failure(e) =>
        throw e
    }

    aggregations
  }

  override def getAggregatedValue[T <: Writable](name: String) = {
    aggregations.get(name) match {
      case Some(aggStorage) => aggStorage.asInstanceOf[T]
      case None =>
        logWarning (s"AggregationStorage $name not found")
        null.asInstanceOf[T]
    }
  }

  override def setAggregatedValue[T <: Writable](name: String, value: T) = {
    logWarning ("Setting aggregated value has no effect in spark engine")
  }

  /**
   * Functions that retrieve the results of this computation.
   * Current fields:
   *  - Subgraphs if the output is enabled. Our choice is to read the results
   *  produced by the steps from external storage. We avoid memory issues
   *  by not keeping all the Subgraphs in memory.
   */

  def getSubgraphs: RDD[ResultSubgraph[_]] = {
    if (!config.isOutputActive) {
      logWarning (s"Trying to get Subgraphs but output is not enabled")
      return sc.emptyRDD[ResultSubgraph[_]]
    }

    val subgraphsPath = s"${config.getOutputPath}"
    val fs = FileSystem.get (sc.hadoopConfiguration)
    val outputPath = new Path(subgraphsPath)

    if (!fs.exists(outputPath)) {
      logWarning (s"Trying to get Subgraphs" +
        s" but output path does not exist: ${subgraphsPath}")
      return sc.emptyRDD[ResultSubgraph[_]]
    }

    logInfo (s"Reading subgraph words from: ${config.getOutputPath}")

    config.getOutputFormat match {
      case SparkConfiguration.OUTPUT_PLAIN_TEXT =>
        sc.textFile (s"${subgraphsPath}/*").map (ResultSubgraph(_))

      case SparkConfiguration.OUTPUT_SEQUENCE_FILE =>
        // we must decide at runtime the concrete Writable to be used
        val SubgraphClass = config.getSubgraphClass
        val resSubgraphClass = {
          if (SubgraphClass == classOf[EdgeInducedSubgraph])
            classOf[ESubgraph]
          else if (SubgraphClass == classOf[VertexInducedSubgraph])
            classOf[VSubgraph]
          else if (SubgraphClass == classOf[PatternInducedSubgraph])
            classOf[VESubgraph]
          else
            classOf[ResultSubgraph[_]]
        }

        sc.sequenceFile (s"${subgraphsPath}/*",
          classOf[NullWritable], resSubgraphClass).map {
            case (_,e: ESubgraph) => e.copy()
            case (_,e: VSubgraph) => e.copy()
          }. // writables are reused, workaround on that
          asInstanceOf[RDD[ResultSubgraph[_]]]
    }
  }

  override def toString: String = {
    s"${this.getClass.getName}(${step})"
  }
}

object SparkMasterEngine {
  import Configuration._
  import SparkConfiguration._

  // macros for spark accumulators
  val AGG_SUBGRAPHS_OUTPUT = "subgraphs_output"

  def apply[E <: Subgraph] (sc: SparkContext, config: SparkConfiguration[E])
      : SparkMasterEngine[E] = {
    apply(sc, config, null)
  }
  
  def apply[E <: Subgraph] (sc: SparkContext, config: SparkConfiguration[E],
      parent: SparkMasterEngine[E]): SparkMasterEngine[E] =
      config.getString(CONF_COMM_STRATEGY, CONF_COMM_STRATEGY_DEFAULT) match {

    case COMM_FROM_SCRATCH =>
      new SparkFromScratchMasterEngine [E] (sc, config, parent)
    
    case COMM_GRAPH_RED =>
      new SparkGraphRedMasterEngine [E] (sc, config, parent)
  }
}
