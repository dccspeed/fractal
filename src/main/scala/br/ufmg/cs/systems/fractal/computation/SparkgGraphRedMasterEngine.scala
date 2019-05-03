package br.ufmg.cs.systems.fractal.computation

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor._
import br.ufmg.cs.systems.fractal.aggregation._
import br.ufmg.cs.systems.fractal.aggregation.reductions._
import br.ufmg.cs.systems.fractal.conf.SparkConfiguration
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray
import br.ufmg.cs.systems.fractal.util.{Logging, ProcessComputeFunc, WordFilterFunc}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.util.{LongAccumulator, SizeEstimator}

import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * Underlying engine that runs the fractal master.
 * It interacts directly with the RDD interface in Spark by handling the
 * SparkContext.
 */
class SparkGraphRedMasterEngine[S <: Subgraph](
    _config: SparkConfiguration[S],
    _parentOpt: Option[SparkMasterEngine[S]]) extends SparkMasterEngine [S] {

  import SparkFromScratchMasterEngine._
  import SparkGraphRedMasterEngine._

  def config: SparkConfiguration[S] = _config

  def parentOpt: Option[SparkMasterEngine[S]] = _parentOpt

  var masterActorRef: ActorRef = _

  def this(_sc: SparkContext, config: SparkConfiguration[S]) {
    this (config, None)
    sc = _sc
    init()
  }

  def this(_sc: SparkContext, config: SparkConfiguration[S],
      parent: SparkMasterEngine[S]) {
    this (config, Option(parent))
    sc = _sc
    init()
  }

  override def init(): Unit = {
    val start = System.currentTimeMillis

    super.init()

    // gtag computations must have incremental aggregations because we compute
    // from scratch all the steps, then if one of those depends on any previous
    // aggregation (e.g., fsm computation) we are secured
    config.set("incremental_aggregation", true)

    // gtag actor
    masterActorRef = ActorMessageSystem.createActor(this)

    logInfo(s"Started gtag-master-actor(step=${step}):" +
      s" ${masterActorRef}")

    val end = System.currentTimeMillis

    logInfo(s"${this} took ${(end - start)}ms to initialize.")
  }

  /**
   * Master's computation takes place here, superstep by superstep
   */
  lazy val next: Boolean = {
    computeNext()
  }

  def computeNext(): Boolean = {

    val superstepStart = System.currentTimeMillis

    logInfo (s"${this} Computation starting from ${stepRDD}," +
      s", StorageLevel=${stepRDD.getStorageLevel}")

    logInfo(s"Computation step=${step} configId=${this.config.getId}")

    // save original container, i.e., without parents' computations
    val originalContainer = config.computationContainer[S]
    logInfo (s"From scratch computation (${this})." +
      s" Original computation: ${originalContainer}")

    // find out how many computations are pipelined
    val (numComputations, numComputationsLastStep) = {
      var cc = originalContainer
      var curr: SparkMasterEngine[S] = this
      val numComputationsLastStep = curr.config.
        computationContainer[S].setDepth(0)
      while (curr.parentOpt.isDefined) {
        curr = curr.parentOpt.get
        cc = curr.config.computationContainer[S].withComputationAppended(cc)
      }
      (cc.setDepth(0), numComputationsLastStep)
    }

    // adding accumulators to each computation
    val veAccums = new Array[LongAccumulator](numComputations)
    val ceAccums = new Array[LongAccumulator](numComputations)
    val eAccums = new Array[LongAccumulator](numComputations)

    var i = 0
    while (i < numComputations) {
      val veKey = s"${VALID_SUBGRAPHS}_${i}"
      val ceKey = s"${CANONICAL_SUBGRAPHS}_${i}"
      val eKey = s"${NEIGHBORHOOD_LOOKUPS}_${i}"

      veAccums(i) = sc.longAccumulator(veKey)
      ceAccums(i) = sc.longAccumulator(ceKey)
      eAccums(i) = sc.longAccumulator(eKey)

      aggAccums.update (veKey, veAccums(i))
      aggAccums.update (ceKey, ceAccums(i))
      aggAccums.update (eKey, eAccums(i))

      logInfo(s"Added accumulators (${veKey},${ceKey},${eKey})")

      i += 1
    }

    // we will contruct the pipeline in this var
    var cc = originalContainer.withComputationLabel("last_step_begins")

    // accumulate parents' computations
    var curr: SparkMasterEngine[S] = this
    while (curr.parentOpt.isDefined) {
      curr = curr.parentOpt.get
      cc = curr.config.computationContainer[S].withComputationAppended(cc)
    }

    // configure custom WordFilterFunc, except for computations of the last step
    val wordFilterFuncs = new Array[WordFilterFunc[S]](numComputations)

    // configure custom ProcessComputeFunc and aggregations
    val processComputeFunc = getProcessComputeFunc(veAccums, ceAccums)
    cc = {
      def withCustomFuncs(cc: ComputationContainer[S], depth: Int)
        : ComputationContainer[S] = cc.nextComputationOpt match {
        case Some(c) =>
          val ncc = withCustomFuncs(c.asInstanceOf[ComputationContainer[S]],
            depth + 1)
          val _cc = cc.shallowCopy(
            processComputeOpt = Option(processComputeFunc),
            initAggregationsOpt = Option(aggregationRegister(cc, depth)),
            nextComputationOpt = Option(ncc),
            wordFilterOpt = Option(wordFilterFuncs(depth)))
          _cc.initAggregations(this.config)
          _cc

        case None =>
          val _cc = cc.shallowCopy(
            processComputeOpt = Option(processComputeFunc),
            initAggregationsOpt = Option(aggregationRegister(cc, depth)),
            wordFilterOpt = Option(wordFilterFuncs(depth)))
          _cc.initAggregations(this.config)
          _cc
      }

      cc = withCustomFuncs(cc, 0).withComputationLabel("first_computation")
      cc.setDepth(0)
      cc
    }

    // set the modified pipelined computation
    this.config.set(SparkConfiguration.COMPUTATION_CONTAINER, cc)

    logInfo (s"From scratch computation (${this}). Final computation: ${cc}")

    logInfo (s"SparkConfiguration estimated size = " +
      s"${SizeEstimator.estimate(config)} bytes")
    logInfo (s"HadoopConfiguration estimated size = " +
      s"${SizeEstimator.estimate(config.hadoopConf)} bytes")

    val initStart = System.currentTimeMillis
    val _configBc = configBc
    val _previousAggregationsBc = previousAggregationsBc
    stepRDD.mapPartitions { iter =>
      val prevAggs = _previousAggregationsBc.value.asInstanceOf[Map[String,_]]
      (prevAggs.get(VPREV_ENUM), prevAggs.get(EPREV_ENUM)) match {
        case (Some(vagg), Some(eagg)) =>

          val vtag = vagg.
            asInstanceOf[AggregationStorage[NullWritable,AtomicBitSetArray]].
            getValue(NullWritable.get())

          val etag = eagg.
            asInstanceOf[AggregationStorage[NullWritable,AtomicBitSetArray]].
            getValue(NullWritable.get())

          _configBc.value.initializeWithTag(false)

        case (None, None) =>
          _configBc.value.initializeWithTag(false)

        case tags =>
          throw new RuntimeException(s"Not allowed, missing tag: ${tags}")
      }
      iter
    }.foreachPartition(_ => {})

    val initElapsed = System.currentTimeMillis - initStart

    logInfo (s"Initialization took ${initElapsed} ms")

    val _aggAccums = aggAccums

    val execEngines = getExecutionEngines (
      superstepRDD = stepRDD,
      superstep = step,
      configBc = configBc,
      aggAccums = _aggAccums,
      previousAggregationsBc = previousAggregationsBc)

    execEngines.persist(MEMORY_ONLY_SER)
    execEngines.foreachPartition (_ => {})

    val enumerationElapsed = System.currentTimeMillis - superstepStart

    logInfo(s"Enumeration step=${step} took ${enumerationElapsed} ms")

    /** [1] We extract and aggregate the *aggregations* globally.
     */
    val aggregationsFuture = getAggregations (execEngines, numPartitions)
    // aggregations
    Await.ready (aggregationsFuture, atMost = Duration.Inf)
    aggregationsFuture.value.get match {

      case Success(previousAggregations) =>
        aggregations = mergeOrReplaceAggregations (aggregations,
          previousAggregations)

        aggregations.foreach { case (name, agg) =>
          val mapping = agg.getMapping
          val numMappings = agg.getNumberMappings
          logInfo (s"Aggregation[${name}][numMappings=${numMappings}]\n" +
            s"${mapping.map(t => s"Aggregation[${name}][${step}]" +
            s" ${t._1}: ${t._2}").mkString("\n")}")
        }

        previousAggregationsBc = sc.broadcast (aggregations)

      case Failure(e) =>
        logError (s"Error in collecting aggregations: ${e.getMessage}")
        throw e
    }

    execEngines.unpersist()

    // whether the user chose to customize master computation, executed every
    // superstep
    masterComputation.compute()

    // print stats
    aggAccums.foreach { case (name, accum) =>
      logInfo (s"Accumulator[${step}][${name}]: ${accum.value}")
      accum.reset()
    }

    // master will send poison pills to all executor actors of this step
    masterActorRef ! Reset

    val superstepFinish = System.currentTimeMillis
    logInfo (
      s"Superstep $step finished in ${superstepFinish - superstepStart} ms"
    )

    // make sure we maintain the engine's original state
    this.config.set(SparkConfiguration.COMPUTATION_CONTAINER, originalContainer)

    !sc.isStopped && !isComputationHalted
  }

  override def finalizeComputation(): Unit = {
    val vagg = getAggregatedValue [
      AggregationStorage[NullWritable,AtomicBitSetArray]] (VPREV_ENUM)
    if (vagg != null) {
      aggregations.remove(VPREV_ENUM)
      config.set(VPREV_ENUM, vagg.getValue(NullWritable.get()))
    }

    val eagg = getAggregatedValue [
      AggregationStorage[NullWritable,AtomicBitSetArray]] (EPREV_ENUM)
    if (eagg != null) {
      aggregations.remove(EPREV_ENUM)
      config.set(EPREV_ENUM, eagg.getValue(NullWritable.get()))
    }

    previousAggregationsBc.destroy()
    previousAggregationsBc = sc.broadcast(aggregations)

    configBc.destroy()
    mutableConfigBc = sc.broadcast(config)
  }

  /**
   * Creates an RDD of execution engines
   */
  def getExecutionEngines[E <: Subgraph](
      superstepRDD: RDD[Unit],
      superstep: Int,
      configBc: Broadcast[SparkConfiguration[E]],
      aggAccums: Map[String,LongAccumulator],
      previousAggregationsBc: Broadcast[_]): RDD[SparkEngine[E]] = {

    val execEngines = superstepRDD.mapPartitionsWithIndex { (idx, cacheIter) =>

      val prevAggs = previousAggregationsBc.value.asInstanceOf[Map[String,_]]
      (prevAggs.get(VPREV_ENUM), prevAggs.get(EPREV_ENUM)) match {
        case (Some(vagg), Some(eagg)) =>

          val vtag = vagg.
            asInstanceOf[AggregationStorage[NullWritable,AtomicBitSetArray]].
            getValue(NullWritable.get())

          val etag = eagg.
            asInstanceOf[AggregationStorage[NullWritable,AtomicBitSetArray]].
            getValue(NullWritable.get())

          configBc.value.initializeWithTag(false)

        case (None, None) =>
          configBc.value.initializeWithTag(false)

        case tags =>
          throw new RuntimeException(s"Not allowed, missing tag: ${tags}")
      }

      val execEngine = new SparkFromScratchEngine [E] (
        partitionId = idx,
        step = superstep,
        accums = aggAccums,
        previousAggregationsBc = previousAggregationsBc,
        configurationId = configBc.value.getId
      )

      execEngine.init()
      execEngine.compute ()
      execEngine.finalize()

      configBc.value.uninitialize()

      Iterator[SparkEngine[E]](execEngine)
    }

    execEngines
  }

  def getProcessComputeFunc(_egAccums: Array[LongAccumulator],
      _awAccums: Array[LongAccumulator]): ProcessComputeFunc[S] = {
    new ProcessComputeFunc[S] with Logging {
      val numComputations = _egAccums.length

      val egAccums = _egAccums

      val awAccums = _awAccums

      var enabledVertices: AtomicBitSetArray = _

      var enabledEdges: AtomicBitSetArray = _

      var workStealingSys: WorkStealingSystem[S] = _

      var totalNumWords: Int = _

      var computations: Array[Computation[S]] = _

      def apply(iter: SubgraphEnumerator[S], c: Computation[S]): Long = {
        if (c.getDepth() == 0) {
          val config = c.getConfig()
          val execEngine = c.getExecutionEngine().asInstanceOf[SparkEngine[S]]

          egAccums(c.getDepth) = execEngine.
            accums(s"${VALID_SUBGRAPHS}_${c.getDepth}")
          awAccums(c.getDepth) = execEngine.
            accums(s"${CANONICAL_SUBGRAPHS}_${c.getDepth}")

          var currComp = c.nextComputation()
          while (currComp != null) {
            val depth = currComp.getDepth()
            currComp.setExecutionEngine(execEngine)
            currComp.init(config)
            currComp.initAggregations(config)
            egAccums(depth) = execEngine.accums(
              s"${VALID_SUBGRAPHS}_${depth}")
            awAccums(depth) = execEngine.accums(
              s"${CANONICAL_SUBGRAPHS}_${depth}")
            currComp = currComp.nextComputation
          }

          totalNumWords = config.getNumWords()

          enabledVertices = new AtomicBitSetArray(config.getNumVertices())
          c.getAggregationStorage [NullWritable,AtomicBitSetArray] (VPREV_ENUM).
            aggregate(NullWritable.get(), enabledVertices)

          enabledEdges = new AtomicBitSetArray(config.getNumEdges())
          c.getAggregationStorage [NullWritable,AtomicBitSetArray] (EPREV_ENUM).
            aggregate(NullWritable.get(), enabledEdges)

          //if (c.getStep == 0) {
          //  enabledEdges.enableAll()
          //}

          // setup work-stealing system
          val gtagExecutorActor = execEngine.
            asInstanceOf[SparkFromScratchEngine[S]].slaveActorRef

          val callback = (consumer: SubgraphEnumerator[S], ret: Long) => {
            if (ret > 0) {
              val subgraph = consumer.getSubgraph()
              val prefixSize = consumer.getPrefix().size()
              subgraph.applyTagTo(c,
                enabledVertices, enabledEdges, prefixSize - 1)
            }
          }

          workStealingSys = new WorkStealingSystem[S](
            processCompute, gtagExecutorActor, new ConcurrentLinkedQueue(),
            callback = callback)

          val ret = processCompute(iter, c)
          workStealingSys.workStealingCompute(c)
          ret

        } else {
          processCompute(iter, c)
        }
      }

      private def hasNextComputation(iter: SubgraphEnumerator[S],
          c: Computation[S], nextComp: Computation[S]): Long = {
        var currentSubgraph: S = null.asInstanceOf[S]
        var validChildren = 0L
        var _validChildren = 0L
        var addWords = 0L
        var subgraphsGenerated = 0L

        while (iter.hasNext) {
          currentSubgraph = iter.next
          addWords += 1

          if (c.filter(currentSubgraph)) {

            subgraphsGenerated += 1
            currentSubgraph.nextExtensionLevel
            _validChildren = nextComp.compute(currentSubgraph)
            currentSubgraph.previousExtensionLevel

            if (_validChildren > 0) {
              currentSubgraph.applyTagFrom(c,
                enabledVertices, enabledEdges,
                currentSubgraph.getNumWords() - 1)
              validChildren += _validChildren
            }
          }
        }

        awAccums(c.getDepth).add(addWords)
        egAccums(c.getDepth).add(subgraphsGenerated)

        validChildren
      }

      private def lastComputation(iter: SubgraphEnumerator[S],
          c: Computation[S]): Long = {
        var currentSubgraph: S = null.asInstanceOf[S]
        var addWords = 0L
        var subgraphsGenerated = 0L

        while (iter.hasNext) {
          currentSubgraph = iter.next
          addWords += 1
          if (c.filter(currentSubgraph)) {
            subgraphsGenerated += 1
            c.process(currentSubgraph)
            currentSubgraph.applyTagFrom(c,
              enabledVertices, enabledEdges, currentSubgraph.getNumWords() - 1)
          }
        }

        awAccums(c.getDepth).add(addWords)
        egAccums(c.getDepth).add(subgraphsGenerated)

        subgraphsGenerated
      }

      private def untaggedLastComputation(iter: SubgraphEnumerator[S],
          c: Computation[S]): Long = {
        var currentSubgraph: S = null.asInstanceOf[S]
        var addWords = 0L
        var subgraphsGenerated = 0L

        while (iter.hasNext) {
          currentSubgraph = iter.next
          addWords += 1
          if (c.filter(currentSubgraph)) {
            subgraphsGenerated += 1
            c.process(currentSubgraph)
          }
        }

        awAccums(c.getDepth).add(addWords)
        egAccums(c.getDepth).add(subgraphsGenerated)

        subgraphsGenerated
      }

      private def processCompute(iter: SubgraphEnumerator[S],
          c: Computation[S]): Long = {
        val nextComp = c.nextComputation()

        val ret = if (nextComp != null) {
          hasNextComputation(iter, c, nextComp)
        } else {
          lastComputation(iter, c)
        }

        ret
      }
    }
  }

  def aggregationRegister(cc: ComputationContainer[S], depth: Int)
    : (Computation[S]) => Unit = {

    val aggStorageClass =
      classOf[AggregationStorage[NullWritable,AtomicBitSetArray]]
    val keyClass = classOf[NullWritable]
    val valueClass = classOf[AtomicBitSetArray]
    val persistent = false
    val reductionFunc = new ReductionFunctionContainer(
      (s1: AtomicBitSetArray, s2: AtomicBitSetArray) => {
        if (s1 != null) {
          if (s2 != null) {
            s1.union(s2)
          } else {
            s1
          }
        } else {
          if (s2 != null) {
            s2
          } else {
            null
          }
        }
      }
    )

    val endAggregationFunc =
      null.asInstanceOf[EndAggregationFunction[NullWritable,AtomicBitSetArray]]

    // get the old init aggregations function in order to compose it
    val oldInitAggregation = cc.initAggregationsOpt match {
      case Some(initAggregations) => initAggregations
      case None => (c: Computation[S]) => {}
    }

    // construct an incremental init aggregations function
    (c: Computation[S]) => {
      oldInitAggregation (c) // init aggregations so far
      c.getConfig().registerAggregation [NullWritable,AtomicBitSetArray] (
        VPREV_ENUM,
        aggStorageClass, keyClass, valueClass, persistent,
        reductionFunc, endAggregationFunc, false)
      c.getConfig().registerAggregation [NullWritable,AtomicBitSetArray] (
        EPREV_ENUM,
        aggStorageClass, keyClass, valueClass, persistent,
        reductionFunc, endAggregationFunc, false)
    }
  }
}

object SparkGraphRedMasterEngine {
  val VPREV_ENUM = "vtag"
  val EPREV_ENUM = "etag"
}
