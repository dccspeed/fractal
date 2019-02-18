package br.ufmg.cs.systems.fractal.computation

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.function.IntConsumer
import java.util.{Iterator => JavaIterator}

import akka.actor._
import br.ufmg.cs.systems.fractal.conf.SparkConfiguration
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.{Logging, ProcessComputeFunc}
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
class SparkFromScratchMasterEngine[E <: Subgraph](
    _config: SparkConfiguration[E],
    _parentOpt: Option[SparkMasterEngine[E]]) extends SparkMasterEngine [E] {

  import SparkFromScratchMasterEngine._

  def config: SparkConfiguration[E] = _config
  
  def parentOpt: Option[SparkMasterEngine[E]] = _parentOpt
  
  var gtagMasterActorRef: ActorRef = _
  
  def this(_sc: SparkContext, config: SparkConfiguration[E]) {
    this (config, None)
    sc = _sc
    init()
  }
  
  def this(_sc: SparkContext, config: SparkConfiguration[E],
      parent: SparkMasterEngine[E]) {
    this (config, Option(parent))
    sc = _sc
    init()
  }

  override def init(): Unit = {
    val start = System.currentTimeMillis

    super.init()
    
    // gtag computations must have incremental aggregations because we compute
    // from scratch all the steps, then if one of those depends on any previous
    // aggregation (e.g., fsm computation) we are safe
    config.set("incremental_aggregation", true)

    // gtag actor
    gtagMasterActorRef = ActorMessageSystem.createActor(this)

    logInfo(s"Started gtag-master-actor(step=${superstep}):" +
      s" ${gtagMasterActorRef}")

    val end = System.currentTimeMillis

    logInfo(s"${this} took ${(end - start)}ms to initialize.")
  }
  
  /**
   * Master's computation takes place here, superstep by superstep
   */
  lazy val next: Boolean = {

    
    logInfo (s"${this} Computation starting from ${superstepRDD}," +
      s", StorageLevel=${superstepRDD.getStorageLevel}")

    // save original container, i.e., without parents' computations
    val originalContainer = config.computationContainer[E]
    logInfo (s"From scratch computation (${this})." +
      s" Original computation: ${originalContainer}")

    // find out how many computations are pipelined
    val numComputations = {
      var cc = originalContainer
      var curr: SparkMasterEngine[E] = this
      while (curr.parentOpt.isDefined) {
        curr = curr.parentOpt.get
        cc = curr.config.computationContainer[E].withComputationAppended(cc)
      }
      cc.setDepth(0)
    }

    // adding accumulators to each computation
    val egAccums = new Array[LongAccumulator](numComputations)
    val awAccums = new Array[LongAccumulator](numComputations)
    val exAccums = new Array[LongAccumulator](numComputations)
    var i = 0
    while (i < numComputations) {
      val egKey = s"${VALID_SUBGRAPHS}_${i}"
      val awKey = s"${CANONICAL_SUBGRAPHS}_${i}"
      val exKey = s"${NEIGHBORHOOD_LOOKUPS}_${i}"

      egAccums(i) = sc.longAccumulator(egKey)
      awAccums(i) = sc.longAccumulator(awKey)
      exAccums(i) = sc.longAccumulator(exKey)

      aggAccums.update (egKey, egAccums(i))
      aggAccums.update (awKey, awAccums(i))
      aggAccums.update (exKey, exAccums(i))

      logInfo(s"Added accumulators (${egKey},${awKey},${exKey})")

      i += 1
    }

    // debug accumulators
    aggAccums.update("kws-filter", sc.longAccumulator("kws-filter"))
    aggAccums.update("kws-filter-outerLoop", sc.longAccumulator("kws-filter-outerLoop"))
    aggAccums.update("kws-filter-innerLoop", sc.longAccumulator("kws-filter-innerLoop"))
    //

    // we will contruct the pipeline in this var
    var cc = originalContainer.withComputationLabel("last_step_begins")

    // add parents' computations
    var curr: SparkMasterEngine[E] = this
    while (curr.parentOpt.isDefined) {
      curr = curr.parentOpt.get
      cc = curr.config.computationContainer[E].withComputationAppended(cc)
    }

    // configure custom ProcessComputeFunc and aggregations
    val processComputeFunc = getProcessComputeFunc(egAccums, awAccums)
    cc = {
      def withCustomFuncs(cc: ComputationContainer[E], depth: Int)
        : ComputationContainer[E] = cc.nextComputationOpt match {
        case Some(c) =>
          val ncc = withCustomFuncs(c.asInstanceOf[ComputationContainer[E]],
            depth + 1)
          cc.shallowCopy(
            processComputeOpt = Option(processComputeFunc),
            nextComputationOpt = Option(ncc),
            processOpt = None)

        case None =>
          cc.shallowCopy(processComputeOpt = Option(processComputeFunc))
      }

      cc = withCustomFuncs(cc, 0).withComputationLabel("first_computation")
      cc.setDepth(0)
      cc
    }

    // set the modified pipelined computation
    this.config.set(SparkConfiguration.COMPUTATION_CONTAINER, cc)

    // NOTE: We need this extra initAggregations because this communication
    // strategy adds a 'previous_enumeration' aggregation
    cc.initAggregations(this.config)

    logInfo (s"From scratch computation (${this}). Final computation: ${cc}")

    logInfo (s"SparkConfiguration estimated size = " +
      s"${SizeEstimator.estimate(config)} bytes")
    logInfo (s"HadoopConfiguration estimated size = " +
      s"${SizeEstimator.estimate(config.hadoopConf)} bytes")

    val initStart = System.currentTimeMillis
    val _configBc = configBc
    superstepRDD.mapPartitions { iter =>
      _configBc.value.initialize(isMaster = false)
      iter
    }.foreachPartition(_ => {})

    val initElapsed = System.currentTimeMillis - initStart

    logInfo (s"Initialization took ${initElapsed} ms")

    val superstepStart = System.currentTimeMillis

    val enumerationStart = System.currentTimeMillis

    val _aggAccums = aggAccums

    val execEngines = getExecutionEngines (
      superstepRDD = superstepRDD,
      superstep = superstep,
      configBc = configBc,
      aggAccums = _aggAccums,
      previousAggregationsBc = previousAggregationsBc)

    execEngines.persist(MEMORY_ONLY_SER)
    execEngines.foreachPartition (_ => {})

    val enumerationElapsed = System.currentTimeMillis - enumerationStart

    logInfo(s"Enumeration step=${superstep} took ${enumerationElapsed} ms")

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
          logInfo (s"Aggregation[${name}][numMappings=${numMappings}][${agg}]\n" +
            s"${mapping.take(10).map(t => s"Aggregation[${name}][${superstep}]" +
            s" ${t._1}: ${t._2}").mkString("\n")}\n...")
        }

        previousAggregationsBc = sc.broadcast (aggregations)

      case Failure(e) =>
        logError (s"Error in collecting aggregations: ${e.getMessage}")
        throw e
    }

    execEngines.unpersist()

    logInfo (s"StorageLevel = ${storageLevel}")

    // whether the user chose to customize master computation, executed every
    // superstep
    masterComputation.compute()

    // print stats
    aggAccums.foreach { case (name, accum) =>
      logInfo (s"Accumulator[${superstep}][${name}]: ${accum.value}")
    }

    // master will send poison pills to all executor actors of this step
    gtagMasterActorRef ! Reset

    val superstepFinish = System.currentTimeMillis
    logInfo (
      s"Superstep $superstep finished in ${superstepFinish - superstepStart} ms"
    )

    // make sure we maintain the engine's original state
    this.config.set(SparkConfiguration.COMPUTATION_CONTAINER, originalContainer)

    !sc.isStopped && !isComputationHalted
  }

  /**
   * Creates an RDD of execution engines
   * TODO
   */
  def getExecutionEngines[E <: Subgraph](
      superstepRDD: RDD[Unit],
      superstep: Int,
      configBc: Broadcast[SparkConfiguration[E]],
      aggAccums: Map[String,LongAccumulator],
      previousAggregationsBc: Broadcast[_]): RDD[SparkEngine[E]] = {

    val execEngines = superstepRDD.mapPartitionsWithIndex { (idx, cacheIter) =>

      configBc.value.initialize(isMaster = false)

      val execEngine = new SparkFromScratchEngine [E] (
        partitionId = idx,
        superstep = superstep,
        accums = aggAccums,
        previousAggregationsBc = previousAggregationsBc,
        configurationId = configBc.value.getId
      )

      execEngine.init()
      execEngine.compute ()
      execEngine.finalize()

      Iterator[SparkEngine[E]](execEngine)
    }

    execEngines
  }

  def getProcessComputeFunc(_egAccums: Array[LongAccumulator],
      _awAccums: Array[LongAccumulator]): ProcessComputeFunc[E] = {
    new ProcessComputeFunc[E] with Logging {
      val numComputations = _egAccums.length

      val egAccums = _egAccums

      val awAccums = _awAccums

      var workStealingSys: WorkStealingSystem[E] = _

      var validSubgraphs: AtomicLong = _

      var lastStepConsumer: LastStepConsumer[E] = _

      def apply(iter: JavaIterator[E], c: Computation[E]): Long = {
        if (c.getDepth() == 0) {
          val config = c.getConfig()
          val execEngine = c.getExecutionEngine().
            asInstanceOf[SparkFromScratchEngine[E]]

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

          lastStepConsumer = new LastStepConsumer[E]()

          validSubgraphs = execEngine.validSubgraphs

          var start = System.currentTimeMillis
          val ret = processCompute(iter, c)
          var elapsed = System.currentTimeMillis - start

          logInfo (s"WorkStealingMode internal=${config.internalWsEnabled()}" +
            s" external=${config.externalWsEnabled()}")

          logInfo (s"InitialComputation step=${c.getStep}" +
            s" partitionId=${c.getPartitionId} took ${elapsed} ms")

          // setup work-stealing system
          start = System.currentTimeMillis
          if (config.wsEnabled()) {
            val gtagExecutorActor = execEngine.gtagActorRef
            workStealingSys = new WorkStealingSystem[E](
              processCompute, gtagExecutorActor, new ConcurrentLinkedQueue())

            workStealingSys.workStealingCompute(c)
          }
          elapsed = System.currentTimeMillis - start

          logInfo (s"WorkStealingComputation step=${c.getStep}" +
            s" partitionId=${c.getPartitionId} took ${elapsed} ms")

          ret
        } else {
          processCompute(iter, c)
        }
      }

      private def lastStepBeginsOnNextComputation(iter: JavaIterator[E],
          c: Computation[E], nextComp: Computation[E]): Long = {
        var currentSubgraph: E = null.asInstanceOf[E]
        var addWords = 0L
        var SubgraphsGenerated = 0L

        while (iter.hasNext) {
          currentSubgraph = iter.next
          addWords += 1
          if (c.filter(currentSubgraph)) {
            SubgraphsGenerated += 1
            validSubgraphs.incrementAndGet
            currentSubgraph.nextExtensionLevel
            nextComp.compute(currentSubgraph)
            currentSubgraph.previousExtensionLevel
          }
        }

        awAccums(c.getDepth).add(addWords)
        egAccums(c.getDepth).add(SubgraphsGenerated)

        0
      }

      private def hasNextComputation(iter: JavaIterator[E],
          c: Computation[E], nextComp: Computation[E]): Long = {
        var currentSubgraph: E = null.asInstanceOf[E]
        var addWords = 0L
        var SubgraphsGenerated = 0L

        while (iter.hasNext) {
          currentSubgraph = iter.next
          addWords += 1
          if (c.filter(currentSubgraph)) {
            SubgraphsGenerated += 1
            currentSubgraph.nextExtensionLevel
            nextComp.compute(currentSubgraph)
            currentSubgraph.previousExtensionLevel
          }
        }

        awAccums(c.getDepth).add(addWords)
        egAccums(c.getDepth).add(SubgraphsGenerated)

        0
      }

      private def lastComputation(iter: JavaIterator[E],
          c: Computation[E]): Long = {
        //var currentSubgraph: E = null.asInstanceOf[E]
        //var addWords = 0L
        //var SubgraphsGenerated = 0L

        //while (iter.hasNext) {
        //  currentSubgraph = iter.next
        //  addWords += 1
        //  if (c.filter(currentSubgraph)) {
        //    SubgraphsGenerated += 1
        //    if (c.shouldExpand(currentSubgraph)) {
        //      c.getExecutionEngine().processExpansion(currentSubgraph)
        //    }
        //    c.process(currentSubgraph)
        //  }
        //}
        //awAccums(c.getDepth).add(addWords)
        //egAccums(c.getDepth).add(SubgraphsGenerated)

        val embIter = iter.asInstanceOf[SubgraphEnumerator[E]]
        lastStepConsumer.set(embIter.getSubgraph(), c)
        embIter.getWordIds().forEach(lastStepConsumer)
        awAccums(c.getDepth).add(lastStepConsumer.addWords)
        egAccums(c.getDepth).add(lastStepConsumer.SubgraphsGenerated)

        0
      }

      private def processCompute(iter: JavaIterator[E],
          c: Computation[E]): Long = {
        val nextComp = c.nextComputation()

        if (nextComp != null) {
          if (nextComp.computationLabel() == "last_step_begins") {
            lastStepBeginsOnNextComputation(iter, c, nextComp)
          } else {
            hasNextComputation(iter, c, nextComp)
          }
        } else {
          lastComputation(iter, c)
        }
      }
    }
  }
}

class LastStepConsumer[E <: Subgraph] extends IntConsumer {
  var subgraph: E = _
  var computation: Computation[E] = _
  var addWords: Long = _
  var SubgraphsGenerated: Long = _

  def set(subgraph: E, computation: Computation[E]): LastStepConsumer[E] = {
    this.subgraph = subgraph
    this.computation = computation
    this.addWords = 0L
    this.SubgraphsGenerated = 0L
    this
  }

  override def accept(w: Int): Unit = {
    addWords += 1
    subgraph.addWord(w)
    if (computation.filter(subgraph)) {
      SubgraphsGenerated += 1
      computation.process(subgraph)
    }
    subgraph.removeLastWord()
  }
}

object SparkFromScratchMasterEngine {
  val NEIGHBORHOOD_LOOKUPS = "neighborhood_lookups"

  val NEIGHBORHOOD_LOOKUPS_ARR = {
    val arr = new Array[String](16)
    var i = 0
    while (i < arr.length) {
      arr(i) = s"${NEIGHBORHOOD_LOOKUPS}_${i}"
      i += 1
    }
    arr
  }

  def NEIGHBORHOOD_LOOKUPS(depth: Int): String = {
    NEIGHBORHOOD_LOOKUPS_ARR(depth)
  }

  val CANONICAL_SUBGRAPHS = "canonical_subgraphs"

  val VALID_SUBGRAPHS = "valid_subgraphs"

  val AGG_CANONICAL_FILTER = "canonical_filter"
}
