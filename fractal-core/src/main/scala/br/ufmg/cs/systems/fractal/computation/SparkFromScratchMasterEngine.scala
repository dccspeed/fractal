package br.ufmg.cs.systems.fractal.computation

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.function.IntConsumer

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
class SparkFromScratchMasterEngine[S <: Subgraph](
    _config: SparkConfiguration[S],
    _parentOpt: Option[SparkMasterEngine[S]]) extends SparkMasterEngine [S] {

  import SparkFromScratchMasterEngine._

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
    // aggregation (e.g., fsm computation) we are safe
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

    logInfo (s"${this} Computation starting from ${stepRDD}," +
      s", StorageLevel=${stepRDD.getStorageLevel}")

    // save original container, i.e., without parents' computations
    val originalContainer = config.computationContainer[S]
    logInfo (s"From scratch computation (${this})." +
      s" Original computation: ${originalContainer}")

    // find out how many computations are pipelined
    val numComputations = {
      var cc = originalContainer
      var curr: SparkMasterEngine[S] = this
      while (curr.parentOpt.isDefined) {
        curr = curr.parentOpt.get
        cc = curr.config.computationContainer[S].withComputationAppended(cc)
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

    // we will contruct the pipeline in this var
    var cc = originalContainer.withComputationLabel("last_step_begins")

    // add parents' computations
    var curr: SparkMasterEngine[S] = this
    while (curr.parentOpt.isDefined) {
      curr = curr.parentOpt.get
      cc = curr.config.computationContainer[S].withComputationAppended(cc)
    }

    // configure custom ProcessComputeFunc and aggregations
    val processComputeFunc = getProcessComputeFunc(egAccums, awAccums)
    cc = {
      def withCustomFuncs(cc: ComputationContainer[S], depth: Int)
        : ComputationContainer[S] = cc.nextComputationOpt match {
        case Some(c) =>
          val ncc = withCustomFuncs(c.asInstanceOf[ComputationContainer[S]],
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
    stepRDD.mapPartitions { iter =>
      _configBc.value.initializeWithTag(isMaster = false)
      iter
    }.foreachPartition(_ => {})

    val initElapsed = System.currentTimeMillis - initStart

    logInfo (s"Initialization took ${initElapsed} ms")

    val superstepStart = System.currentTimeMillis

    val enumerationStart = System.currentTimeMillis

    val _aggAccums = aggAccums
    validSubgraphsAccum = sc.longAccumulator

    val execEngines = getExecutionEngines (
      superstepRDD = stepRDD,
      superstep = step,
      configBc = configBc,
      aggAccums = _aggAccums,
      validSubgraphsAccum,
      previousAggregationsBc = previousAggregationsBc)

    execEngines.persist(MEMORY_ONLY_SER)
    execEngines.foreachPartition (_ => {})

    val enumerationElapsed = System.currentTimeMillis - enumerationStart

    logInfo(s"Enumeration step=${step} took ${enumerationElapsed} ms")

    val globalAggStart = System.currentTimeMillis()

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
            s"${mapping.take(10).map(t => s"Aggregation[${name}][${step}]" +
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

    val globalAggElapsed = System.currentTimeMillis() - globalAggStart


    val superstepFinish = System.currentTimeMillis
    logInfo (
      s"Superstep $step finished in ${superstepFinish - superstepStart} ms"
    )

    // make sure we maintain the engine's original state
    this.config.set(SparkConfiguration.COMPUTATION_CONTAINER, originalContainer)

    /**
     * Print statistics of this step
     */
    aggAccums.toArray.sortBy(_._1).foreach { case (name, accum) =>
      logInfo (s"FractalStep[${step}][${name}]: ${accum.value}")
    }
    if (validSubgraphsAccum.value == 0) {
      validSubgraphsAccum.add(aggAccums(s"${VALID_SUBGRAPHS}_${numComputations - 1}").value)
    }
    logInfo(s"FractalStep[${step}][valid_subgraphs]: ${validSubgraphsAccum.value}")
    logInfo(s"FractalStep[${step}][initialization_time]: ${initElapsed}")
    logInfo(s"FractalStep[${step}][enumeration_time]: ${enumerationElapsed}")
    logInfo(s"FractalStep[${step}][global_aggregation_time]: ${globalAggElapsed}")
    logInfo(s"FractalStep[${step}][total_time]: ${initElapsed + enumerationElapsed + globalAggElapsed}")

    // master will send poison pills to all executor actors of this step
    masterActorRef ! Reset

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
      validSubgraphsAccum: LongAccumulator,
      previousAggregationsBc: Broadcast[_]): RDD[SparkEngine[E]] = {

    val execEngines = superstepRDD.mapPartitionsWithIndex { (idx, cacheIter) =>

      configBc.value.initializeWithTag(isMaster = false)

      val execEngine = new SparkFromScratchEngine [E] (
        partitionId = idx,
        step = superstep,
        accums = aggAccums,
        validSubgraphsAccum = validSubgraphsAccum,
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
      _awAccums: Array[LongAccumulator]): ProcessComputeFunc[S] = {
    new ProcessComputeFunc[S] with Logging {
      val numComputations = _egAccums.length

      val egAccums = _egAccums

      val awAccums = _awAccums

      var workStealingSys: WorkStealingSystem[S] = _

      var lastStepConsumer: LastStepConsumer[S] = _

      def apply(iter: SubgraphEnumerator[S], c: Computation[S]): Long = {
        if (c.getDepth() == 0) {
          val config = c.getConfig()
          val execEngine = c.getExecutionEngine().
            asInstanceOf[SparkFromScratchEngine[S]]

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

          lastStepConsumer = new LastStepConsumer[S]()

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
            val gtagExecutorActor = execEngine.slaveActorRef
            workStealingSys = new WorkStealingSystem[S](
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

      private def hasNextComputation(iter: SubgraphEnumerator[S],
          c: Computation[S], nextComp: Computation[S]): Long = {
        var currentSubgraph: S = null.asInstanceOf[S]
        var addWords = 0L
        var subgraphsGenerated = 0L
        var ret = 0L

        while (iter.hasNext) {
          val nextEnum = iter.extend()
          currentSubgraph = iter.getSubgraph()
          addWords += 1
          if (c.filter(currentSubgraph)) {
            subgraphsGenerated += 1
            currentSubgraph.nextExtensionLevel()
            ret += nextComp.compute(currentSubgraph)
            currentSubgraph.previousExtensionLevel()
          }
        }

        //while (senum.hasNext) {
        //  currentSubgraph = senum.next
        //  addWords += 1
        //  if (c.filter(currentSubgraph)) {
        //    subgraphsGenerated += 1
        //    currentSubgraph.nextExtensionLevel
        //    nextComp.compute(currentSubgraph)
        //    currentSubgraph.previousExtensionLevel
        //  }
        //}

        awAccums(c.getDepth).add(addWords)
        egAccums(c.getDepth).add(subgraphsGenerated)

        ret
      }

      private def lastComputation(iter: SubgraphEnumerator[S],
          c: Computation[S]): Long = {

        var addWords = 0L
        var subgraphsGenerated = 0L

        //try {
          val wordIds = iter.getWordIds()
          if (wordIds != null) {
            lastStepConsumer.set(iter.getSubgraph(), c)
            wordIds.forEach(lastStepConsumer)
            addWords += lastStepConsumer.addWords
            subgraphsGenerated += lastStepConsumer.subgraphsGenerated
          } else {
            val subgraph = iter.next()
            addWords += 1
            if (c.filter(subgraph)) {
              subgraphsGenerated += 1
              c.process(subgraph)
            }
          }
        //} catch {
        //  case e: Exception =>
        //    throw new RuntimeException(s"${e} ${iter} ${c}")
        //    //var currentSubgraph: S = null.asInstanceOf[S]
        //    //var addWords = 0L
        //    //var subgraphsGenerated = 0L

        //    //while (iter.hasNext) {
        //    //  currentSubgraph = iter.next
        //    //  addWords += 1
        //    //  if (c.filter(currentSubgraph)) {
        //    //    subgraphsGenerated += 1
        //    //    c.process(currentSubgraph)
        //    //  }
        //    //}
        //    //awAccums(c.getDepth).add(addWords)
        //    //egAccums(c.getDepth).add(subgraphsGenerated)
        //}

        awAccums(c.getDepth).add(addWords)
        egAccums(c.getDepth).add(subgraphsGenerated)

        subgraphsGenerated
      }

      private def processCompute(iter: SubgraphEnumerator[S],
        c: Computation[S]): Long = {
        val nextComp = c.nextComputation()

        if (nextComp != null) {
          hasNextComputation(iter, c, nextComp)
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
  var subgraphsGenerated: Long = _

  def set(subgraph: E, computation: Computation[E]): LastStepConsumer[E] = {
    this.subgraph = subgraph
    this.computation = computation
    this.addWords = 0L
    this.subgraphsGenerated = 0L
    this
  }

  override def accept(w: Int): Unit = {
    addWords += 1
    subgraph.addWord(w)
    if (computation.filter(subgraph)) {
      subgraphsGenerated += 1
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
