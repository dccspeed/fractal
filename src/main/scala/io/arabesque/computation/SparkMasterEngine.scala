package io.arabesque.computation

import io.arabesque.aggregation.{AggregationStorage, AggregationStorageMetadata}
import io.arabesque.cache.LZ4ObjectCache
import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding._
import io.arabesque.odag._
import io.arabesque.utils.Logging

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

trait SparkMasterEngine [E <: Embedding]
    extends CommonMasterExecutionEngine with Logging {

  import SparkMasterEngine._

  /** Initial configurations
   *  The following describe general engine parameters
   */
  
  lazy val superstep: Int = parentOpt.map(_.superstep + 1).getOrElse(0)

  var sc: SparkContext = _

  def config: SparkConfiguration[E]
  
  lazy val configBc: Broadcast[SparkConfiguration[E]] = sc.broadcast(config)
  
  def parentOpt: Option[SparkMasterEngine[E]]
  
  var masterComputation: MasterComputation = _
  
  /* */

  /** Computation State
   *  The following fields are set by specialized engines as an output state
   *  w.r.t a superstep
   */

  var storageLevel: StorageLevel = StorageLevel.NONE

  var superstepRDD: RDD[LZ4ObjectCache] = _

  var aggAccums: Map[String,Accumulator[_]] = _

  var previousAggregationsBc: Broadcast[_] = _
  
  var aggregations
    : Map[String,AggregationStorage[_ <: Writable, _ <: Writable]] = _

  /* */

  def init(): Unit = {
    if (!config.isInitialized()) {
      config.initialize()
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
    computation.initAggregations(config)

    // default accumulators
    aggAccums = Map.empty
    aggAccums.update (AGG_EMBEDDINGS_GENERATED,
      sc.accumulator [Long] (0L, AGG_EMBEDDINGS_GENERATED))
    aggAccums.update (AGG_EMBEDDINGS_PROCESSED,
      sc.accumulator [Long] (0L, AGG_EMBEDDINGS_PROCESSED))
    aggAccums.update (AGG_EMBEDDINGS_OUTPUT,
      sc.accumulator [Long] (0L, AGG_EMBEDDINGS_OUTPUT))

    // set initial state
    parentOpt match {
      case Some(parent) =>
        logInfo(s"${this} Setting from parent ${parent} ${parent.previousAggregationsBc.value.asInstanceOf[Map[String,AggregationStorage[_,_]]].get("support")}")
        // start with parent's state
        superstepRDD = parent.superstepRDD
        previousAggregationsBc = parent.previousAggregationsBc
        aggregations = Map() ++ parent.aggregations

      case None =>
        // superstep rdd to simulate parallel computation
        superstepRDD = sc.makeRDD(
          Seq.empty[LZ4ObjectCache], numPartitions
        ).persist(storageLevel)

        // previous aggregation
        previousAggregationsBc = sc.broadcast (
          Map.empty[String,AggregationStorage[_ <: Writable, _ <: Writable]]
        )
        
        aggregations = Map()
    }
  }

  override def haltComputation() = {
    //logInfo ("Halting master computation")
    //sc.stop()
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
   * Master's computation takes place here, superstep by superstep
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
   * @return true if there are more supersteps with this computation or false
   * otherwise
   */
  def next: Boolean

  def numPartitions: Int = config.numPartitions
  
  // for compatibility with the previous version (Giraph)
  def getNumberPartitions: Int = numPartitions
  
  override def getSuperstep(): Long = superstep

  /**
   * Merges or replaces the aggregations for the next superstep. We can have one
   * of the following scenarios:
   * (1) In any superstep we are interested in all aggregations seen so far.
   *     Thus, the aggregations are incrementally composed.
   * (2) In any superstep we are interested only in the previous
   *     aggregations. Thus, we discard the old aggregations and replace it with
   *     the new aggregations for the next superstep.
   *
   *  @param aggregations current aggregations
   *  @param previousAggregations aggregations found in the superstep that just
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
    if (config.isAggregationIncremental) {
      def aggregate[K <: Writable, V <: Writable](agg1: AggregationStorage[K,V],
        agg2: AggregationStorage[_,_]) = {
          agg1.aggregate (agg2.asInstanceOf[AggregationStorage[K,V]])
          agg1
      }
      // we compose all entries
      previousAggregations.foreach { case (name, agg) =>
        aggregations.get(name) match {
          case Some(_agg) =>
            aggregate(_agg, agg)
          case None =>
            aggregations.update (name, agg)
        }
      }
      aggregations
    } else {
      // we replace with new entries
      previousAggregations
    }
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

      val keyValues = execEngines.flatMap (execEngine =>
          execEngine.flushAggregationsByName(name).
            asInstanceOf[Iterator[AggregationStorage[K,V]]]
          )
      val aggStorage = keyValues.reduce { (agg1,agg2) =>
        agg1.aggregate (agg2)
        agg1
      }

      aggStorage.endedAggregation
      aggStorage
    }

    val future = Future.sequence (
      config.getAggregationsMetadata.map { case (name, metadata) =>
        reduce (name, metadata)
      }
    )

    val aggregations = Map.empty[
      String,AggregationStorage[_ <: Writable, _ <: Writable]
    ]

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
   *  - Embeddings if the output is enabled. Our choice is to read the results
   *  produced by the supersteps from external storage. We avoid memory issues
   *  by not keeping all the embeddings in memory.
   */

  def getEmbeddings: RDD[ResultEmbedding[_]] = {

    val embeddPath = s"${config.getOutputPath}"
    val fs = FileSystem.get (sc.hadoopConfiguration)

    if (config.isOutputActive && fs.exists (new Path (embeddPath))) {
      logInfo (s"Reading embedding words from: ${config.getOutputPath}")
      config.getOutputFormat match {
        case SparkConfiguration.OUTPUT_PLAIN_TEXT =>
          sc.textFile (s"${embeddPath}/*").map (ResultEmbedding(_))

        case SparkConfiguration.OUTPUT_SEQUENCE_FILE =>
          // we must decide at runtime the concrete Writable to be used
          val embeddingClass = config.getEmbeddingClass
          val resEmbeddingClass = {
            if (embeddingClass == classOf[EdgeInducedEmbedding])
              classOf[EEmbedding]
            else if (embeddingClass == classOf[VertexInducedEmbedding])
              classOf[VEmbedding]
            else
              classOf[ResultEmbedding[_]]
          }

          sc.sequenceFile (s"${embeddPath}/*",
            classOf[NullWritable], resEmbeddingClass).map {
              case (_,e: EEmbedding) => e.copy()
              case (_,e: VEmbedding) => e.copy()
            }. // writables are reused, workaround on that
            asInstanceOf[RDD[ResultEmbedding[_]]]
      }
    } else {
      sc.emptyRDD[ResultEmbedding[_]]
    }
  }

  override def toString: String = {
    s"${this.getClass.getName}(${superstep})"
  }
}

object SparkMasterEngine {
  import Configuration._
  import SparkConfiguration._

  // macros for spark accumulators
  val AGG_EMBEDDINGS_PROCESSED = "embeddings_processed"
  val AGG_EMBEDDINGS_GENERATED = "embeddings_generated"
  val AGG_EMBEDDINGS_OUTPUT = "embeddings_output"

  def apply[E <: Embedding] (sc: SparkContext, config: SparkConfiguration[E])
      : SparkMasterEngine[E] = {
    apply(sc, config, null)
  }
  
  def apply[E <: Embedding] (sc: SparkContext, config: SparkConfiguration[E],
      parent: SparkMasterEngine[E]): SparkMasterEngine[E] =
      config.getString(CONF_COMM_STRATEGY, CONF_COMM_STRATEGY_DEFAULT) match {
    case COMM_ODAG_SP =>
      new ODAGMasterEngineSP [E] (sc, config, parent)

    case COMM_ODAG_MP =>
      new ODAGMasterEngineMP [E] (sc, config, parent)

    case COMM_EMBEDDING =>
      new SparkEmbeddingMasterEngine [E] (sc, config, parent)
    
    case COMM_GTAG =>
      new SparkGtagMasterEngine [E] (sc, config, parent)
  }
}
