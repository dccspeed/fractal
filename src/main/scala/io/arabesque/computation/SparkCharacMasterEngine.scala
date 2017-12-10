package io.arabesque.computation

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.{Iterator => JavaIterator}

import akka.actor.ActorRef
import io.arabesque.ProcessComputeFunc
import io.arabesque.aggregation._
import io.arabesque.aggregation.reductions._
import io.arabesque.cache.LZ4ObjectCache
import io.arabesque.conf.SparkConfiguration
import io.arabesque.embedding._
import io.arabesque.utils.Logging
import org.apache.hadoop.io.{IntWritable, LongWritable}
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
 */
class SparkCharacMasterEngine[E <: Embedding](
    _config: SparkConfiguration[E],
    _parentOpt: Option[SparkMasterEngine[E]])
  extends SparkMasterEngine [E] {

  import SparkCharacMasterEngine._
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
    // aggregation (e.g., fsm computation) we are secured
    config.set("incremental_aggregation", true)

    // gtag actor
    gtagMasterActorRef = GtagMessagingSystem.createActor(this)

    logInfo(s"Started gtag-master-actor(step=${superstep}):" +
      s" ${gtagMasterActorRef}")

    val end = System.currentTimeMillis

    logInfo(s"${this} took ${(end - start)} ms to initialize.")
  }

  /**
   * Master's computation takes place here, superstep by superstep
   */
  lazy val next: Boolean = {

    val superstepStart = System.currentTimeMillis

    logInfo(s"${this} Computation starting from ${superstepRDD}," +
      s", StorageLevel=${superstepRDD.getStorageLevel}")

    logInfo(s"Computation step=${superstep} configId=${this.config.getId}")

    // save original container, i.e., without parents' computations
    val originalContainer = config.computationContainer[E]
    logInfo(s"From scratch computation (${this})." +
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
    val veAccums = new Array[LongAccumulator](numComputations)
    val ceAccums = new Array[LongAccumulator](numComputations)
    val eAccums = new Array[LongAccumulator](numComputations)

    var i = 0
    while (i < numComputations) {
      val veKey = s"${VALID_EMBEDDINGS}_${i}"
      val ceKey = s"${CANONICAL_EMBEDDINGS}_${i}"
      val eKey = s"${NEIGHBORHOOD_LOOKUPS}_${i}"

      veAccums(i) = sc.longAccumulator(veKey)
      ceAccums(i) = sc.longAccumulator(ceKey)
      eAccums(i) = sc.longAccumulator(eKey)

      aggAccums.update(veKey, veAccums(i))
      aggAccums.update(ceKey, ceAccums(i))
      aggAccums.update(eKey, eAccums(i))

      logInfo(s"Added accumulators (${veKey},${ceKey},${eKey})")

      i += 1
    }

    // we will contruct the pipeline in this var
    var cc = originalContainer.withComputationLabel("last_step_begins")

    // configure custom WordFilterFunc, except for computations of the last step
    var curr: SparkMasterEngine[E] = this
    while (curr.parentOpt.isDefined) {
      curr = curr.parentOpt.get
      cc = curr.config.computationContainer[E].
        withComputationAppended(cc)
    }

    // configure custom ProcessComputeFunc and aggregations
    val processComputeFunc = getProcessComputeFunc(numComputations)
    cc = {
      def withCustomFuncs(cc: ComputationContainer[E], depth: Int)
      : ComputationContainer[E] = cc.nextComputationOpt match {
        case Some(c) =>
          val ncc = withCustomFuncs(c.asInstanceOf[ComputationContainer[E]],
            depth + 1)
          val _cc = cc.shallowCopy(
            processComputeOpt = Option(processComputeFunc),
            initAggregationsOpt = Option(aggregationRegister(cc, depth)),
            nextComputationOpt = Option(ncc))
          _cc.initAggregations(this.config)
          _cc

        case None =>
          val _cc = cc.shallowCopy(processComputeOpt = Option(processComputeFunc),
            initAggregationsOpt = Option(aggregationRegister(cc, depth)))
          _cc.initAggregations(this.config)
          _cc
      }

      cc = withCustomFuncs(cc, 0).withComputationLabel("first_computation")
      cc.setDepth(0)
      cc
    }

    // set the modified pipelined computation
    this.config.set(SparkConfiguration.COMPUTATION_CONTAINER, cc)

    logInfo(s"From scratch computation (${this}). Final computation: ${cc}")

    logInfo(s"SparkConfiguration estimated size = " +
      s"${SizeEstimator.estimate(config)} bytes")
    logInfo(s"HadoopConfiguration estimated size = " +
      s"${SizeEstimator.estimate(config.hadoopConf)} bytes")

    val _aggAccums = aggAccums

    val execEngines = getExecutionEngines(
      superstepRDD = superstepRDD,
      superstep = superstep,
      configBc = configBc,
      aggAccums = _aggAccums,
      previousAggregationsBc = previousAggregationsBc)

    execEngines.persist(MEMORY_ONLY)
    execEngines.foreachPartition(_ => {})

    /** [1] We extract and aggregate the *aggregations* globally.
      */
    val aggregationsFuture = getAggregations(execEngines, numPartitions)
    // aggregations
    Await.ready(aggregationsFuture, atMost = Duration.Inf)
    aggregationsFuture.value.get match {
      case Success(previousAggregations) =>
        aggregations = mergeOrReplaceAggregations(aggregations,
          previousAggregations)

        val aggSizes = aggregations.map(t => (t._1, t._2.getNumberMappings))
        logInfo(s"Aggregations and sizes: ${aggSizes.mkString(",")}")

        aggregations.foreach { case (name, agg) =>
          val mapping = agg.getMapping
          logInfo(s"Aggregation[${name}][${agg}]\n" +
            s"${
              mapping.map(t => s"Aggregation[${name}][${superstep}]" +
                s" ${t._1}: ${t._2}").mkString("\n")
            }")
        }

        previousAggregationsBc = sc.broadcast(aggregations)

      case Failure(e) =>
        logError(s"Error in collecting aggregations: ${e.getMessage}")
        throw e
    }

    execEngines.unpersist()

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

    !sc.isStopped
  }

  /**
   * Creates an RDD of execution engines
   * TODO
   */
  def getExecutionEngines[E <: Embedding](
      superstepRDD: RDD[LZ4ObjectCache],
      superstep: Int,
      configBc: Broadcast[SparkConfiguration[E]],
      aggAccums: Map[String,LongAccumulator],
      previousAggregationsBc: Broadcast[_]): RDD[SparkEngine[E]] = {

    val execEngines = superstepRDD.mapPartitionsWithIndex { (idx, cacheIter) =>

      configBc.value.initialize()

      val execEngine = new SparkFromScratchEngine [E] (
        partitionId = idx,
        superstep = superstep,
        accums = aggAccums,
        previousAggregationsBc = previousAggregationsBc,
        configurationId = configBc.value.getId
      )

      execEngine.init()
      val numWords = configBc.value.getNumWords()

      var prevEnums: List[AggregationStorage[IntWritable,LongWritable]] = Nil
      var semaphoreOpts: List[Option[AtomicLong]] = Nil

      var currComp = execEngine.computation
      while (currComp != null) {
        val prevEnum = execEngine.getAggregatedValue [
          AggregationStorage[IntWritable,LongWritable]] (
            s"${PREV_ENUM}_${currComp.getDepth}")

        prevEnums = prevEnum :: prevEnums

        var semaphoreOpt: Option[AtomicLong] = None
        if (prevEnum != null) {
          semaphoreOpt = Option(prevEnum.resetAtomicArray(numWords))
        }

        semaphoreOpts = semaphoreOpt :: semaphoreOpts

        currComp = currComp.nextComputation()
      }

      execEngine.compute (cacheIter)
      execEngine.finalize()

      while (prevEnums != Nil) {
        val prevEnum = prevEnums.head
        val semaphoreOpt = semaphoreOpts.head

        if (prevEnum != null && semaphoreOpt.isDefined) {
          prevEnum.setAtomicArrayDirty(semaphoreOpt.get);
        }

        prevEnums = prevEnums.tail
        semaphoreOpts = semaphoreOpts.tail
      }

      Iterator[SparkEngine[E]](execEngine)
    }

    execEngines
  }

  def getProcessComputeFunc(_numComputations: Int): ProcessComputeFunc[E] = {
    new ProcessComputeFunc[E] with Logging {
      val numComputations = _numComputations

      var egAccums: Array[LongAccumulator] = _

      var awAccums: Array[LongAccumulator] = _

      var prevEnumAggs: Array[AggregationStorage[IntWritable,LongWritable]] = _

      var writableOne: LongWritable = _

      var reusableIdWritable: IntWritable = _

      var workStealingSys: WorkStealingSystem[E] = _

      var totalNumWords: Int = _

      def apply(iter: JavaIterator[E], c: Computation[E]): Long = {
        if (c.getDepth() == 0) {
          val config = c.getConfig()
          val execEngine = c.getExecutionEngine().asInstanceOf[SparkEngine[E]]

          egAccums = new Array[LongAccumulator](numComputations)
          awAccums = new Array[LongAccumulator](numComputations)
          prevEnumAggs = new Array[AggregationStorage[IntWritable,LongWritable]](numComputations)

          var depth = 0
          var currComp = c
          while (depth < numComputations) {
            if (depth > 0) {
              currComp.setExecutionEngine(c.getExecutionEngine())
              currComp.init(c.getConfig())
              currComp.initAggregations(c.getConfig())
            }

            egAccums(depth) = execEngine.accums(
              s"${VALID_EMBEDDINGS}_${depth}")
            awAccums(depth) = execEngine.accums(
              s"${CANONICAL_EMBEDDINGS}_${depth}")

            prevEnumAggs(depth) = currComp.
              getAggregationStorage [IntWritable,LongWritable] (
                s"${PREV_ENUM}_${depth}")

            depth += 1
            currComp = currComp.nextComputation
          }

          reusableIdWritable = new IntWritable()
          writableOne = new LongWritable(1)

          totalNumWords = c.getConfig().getNumWords()

          // work-stealing
          val gtagExecutorActor = c.getExecutionEngine().
            asInstanceOf[SparkFromScratchEngine[E]].gtagActorRef

          workStealingSys = new WorkStealingSystem[E](
            processCompute, gtagExecutorActor, new ConcurrentLinkedQueue())

          val ret = processCompute(iter, c)
          workStealingSys.workStealingCompute(c)
          ret
        } else {
          processCompute(iter, c)
        }
      }

      private def hasNextComputation(iter: JavaIterator[E],
          c: Computation[E], nextComp: Computation[E]): Long = {
        var currentEmbedding: E = null.asInstanceOf[E]
        var addWords = 0L
        var embeddingsGenerated = 0L

        while (iter.hasNext) {
          currentEmbedding = iter.next
          addWords += 1

          if (c.filter(currentEmbedding)) {
            embeddingsGenerated += 1
            currentEmbedding.nextExtensionLevel
            nextComp.compute(currentEmbedding)
            currentEmbedding.previousExtensionLevel
          }
        }

        awAccums(c.getDepth).add(addWords)
        egAccums(c.getDepth).add(embeddingsGenerated)

        0
      }

      private def lastComputation(iter: JavaIterator[E],
          c: Computation[E]): Long = {
        var currentEmbedding: E = null.asInstanceOf[E]
        var addWords = 0L
        var embeddingsGenerated = 0L

        while (iter.hasNext) {
          currentEmbedding = iter.next
          addWords += 1
          if (c.filter(currentEmbedding)) {
            embeddingsGenerated += 1
            if (c.shouldExpand(currentEmbedding)) {

              // log word counts for next enumeration
              val words = currentEmbedding.getWords()
              val numWords = currentEmbedding.getNumWords()
              var i = 0
              while (i < numWords) {
                reusableIdWritable.set(words.getUnchecked(i))
                prevEnumAggs(i).
                  aggregateWithReusables(reusableIdWritable, writableOne)
                i += 1
              }
              c.getExecutionEngine().processExpansion(currentEmbedding)
            }
            c.process(currentEmbedding)
          }
        }

        awAccums(c.getDepth).add(addWords)
        egAccums(c.getDepth).add(embeddingsGenerated)

        0
      }

      private def processCompute(iter: JavaIterator[E],
          c: Computation[E]): Long = {
        val nextComp = c.nextComputation()

        if (nextComp != null) {
          hasNextComputation(iter, c, nextComp)
        } else {
          lastComputation(iter, c)
        }
      }
    }
  }

  def aggregationRegister(cc: ComputationContainer[E], depth: Int)
    : (Computation[E]) => Unit = {

    val name = PREV_ENUM
    val aggStorageClass =
      classOf[AggregationStorage[IntWritable,LongWritable]]
    val keyClass = classOf[IntWritable]
    val valueClass = classOf[LongWritable]
    val persistent = false
    val reductionFunc = new LongSumReduction()
    val endAggregationFunc =
      null.asInstanceOf[EndAggregationFunction[IntWritable,LongWritable]]

    // get the old init aggregations function in order to compose it
    val oldInitAggregation = cc.initAggregationsOpt match {
      case Some(initAggregations) => initAggregations
      case None => (c: Computation[E]) => {}
    }

    // construct an incremental init aggregations function
    (c: Computation[E]) => {
      oldInitAggregation (c) // init aggregations so far
      c.getConfig().registerAggregation [IntWritable,LongWritable] (
        s"${name}_${depth}",
        aggStorageClass, keyClass, valueClass, persistent,
        reductionFunc, endAggregationFunc, false)
    }
  }
}

object SparkCharacMasterEngine {
  val PREV_ENUM = "previous_enumeration"
}
