package io.arabesque.computation

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicLong, AtomicLongArray}
import java.util.{Iterator => JavaIterator}

import akka.actor._
import io.arabesque.aggregation._
import io.arabesque.aggregation.reductions._
import io.arabesque.cache.LZ4ObjectCache
import io.arabesque.conf.SparkConfiguration
import io.arabesque.embedding._
import io.arabesque.utils.Logging
import io.arabesque.{ProcessComputeFunc, WordFilterFunc}
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
 * Underlying engine that runs the Arabesque master.
 * It interacts directly with the RDD interface in Spark by handling the
 * SparkContext.
 */
class SparkGtagMasterEngine[E <: Embedding](
    _config: SparkConfiguration[E],
    _parentOpt: Option[SparkMasterEngine[E]]) extends SparkMasterEngine [E] {

  import SparkFromScratchMasterEngine._
  import SparkGtagMasterEngine._
  import SparkMasterEngine._

  def config: SparkConfiguration[E] = _config
  
  config.initialize()

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

    logInfo(s"${this} took ${(end - start)}ms to initialize.")
  }
  
  /**
   * Master's computation takes place here, superstep by superstep
   */
  lazy val next: Boolean = {

    val superstepStart = System.currentTimeMillis
    
    logInfo (s"${this} Computation starting from ${superstepRDD}," +
      s", StorageLevel=${superstepRDD.getStorageLevel}")

    logInfo(s"Computation step=${superstep} configId=${this.config.getId}")

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
    val gfAccums = new Array[LongAccumulator](numComputations)
    val cfAccums = new Array[LongAccumulator](numComputations)
    val gfcfAccums = new Array[LongAccumulator](numComputations)
    val exAccums = new Array[LongAccumulator](numComputations)
    var i = 0
    while (i < numComputations) {
      val egKey = s"${AGG_EMBEDDINGS_GENERATED}_${i}"
      val awKey = s"${CANONICAL_EMBEDDINGS}_${i}"
      val gfKey = s"${AGG_GTAG_FILTER}_${i}"
      val cfKey = s"${AGG_CANONICAL_FILTER}_${i}"
      val gfcfKey = s"${AGG_GTAG_FILTER}_${AGG_CANONICAL_FILTER}_${i}"
      val exKey = s"${NEIGHBORHOOD_LOOKUPS}_${i}"
      
      egAccums(i) = sc.longAccumulator(egKey)
      awAccums(i) = sc.longAccumulator(awKey)
      gfAccums(i) = sc.longAccumulator(gfKey)
      cfAccums(i) = sc.longAccumulator(cfKey)
      gfcfAccums(i) = sc.longAccumulator(gfcfKey)
      exAccums(i) = sc.longAccumulator(exKey)

      aggAccums.update (egKey, egAccums(i))
      aggAccums.update (awKey, awAccums(i))
      aggAccums.update (gfKey, gfAccums(i))
      aggAccums.update (cfKey, cfAccums(i))
      aggAccums.update (gfcfKey, gfcfAccums(i))
      aggAccums.update (exKey, exAccums(i))

      logInfo(s"Added accumulators (${egKey},${awKey},${gfKey},${cfKey}" +
        s",${gfcfKey},${exKey})")

      i += 1
    }
    
    // we will contruct the pipeline in this var
    var cc = originalContainer.withComputationLabel("last_step_begins")

    if (this.parentOpt.isDefined) {
      val lastWordFilterFunc = getLastWordFilterFunc(
        cfAccums, gfAccums, gfcfAccums)
      cc = cc.shallowCopy(wordFilterOpt = Option(lastWordFilterFunc))
    }

    // configure custom WordFilterFunc, except for computations of the last step
    val wordFilterFunc = getWordFilterFunc(cfAccums, gfAccums, gfcfAccums)
    var curr: SparkMasterEngine[E] = this
    while (curr.parentOpt.isDefined) {
      curr = curr.parentOpt.get
      cc = curr.config.computationContainer[E].
        shallowCopy (wordFilterOpt = Option(wordFilterFunc)).
        withComputationAppended(cc)
    }

    // configure custom ProcessComputeFunc and aggregations
    val processComputeFunc = getProcessComputeFunc(egAccums, awAccums)
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

    logInfo (s"From scratch computation (${this}). Final computation: ${cc}")

    logInfo (s"SparkConfiguration estimated size = " +
      s"${SizeEstimator.estimate(config)} bytes")
    logInfo (s"HadoopConfiguration estimated size = " +
      s"${SizeEstimator.estimate(config.hadoopConf)} bytes")

    val _aggAccums = aggAccums

    val execEngines = getExecutionEngines (
      superstepRDD = superstepRDD,
      superstep = superstep,
      configBc = configBc,
      aggAccums = _aggAccums,
      previousAggregationsBc = previousAggregationsBc)

    execEngines.persist(MEMORY_ONLY_SER)
    execEngines.foreachPartition (_ => {})

    /** [1] We extract and aggregate the *aggregations* globally.
     */
    val aggregationsFuture = getAggregations (execEngines, numPartitions)
    // aggregations
    Await.ready (aggregationsFuture, atMost = Duration.Inf)
    aggregationsFuture.value.get match {
      case Success(previousAggregations) =>
        aggregations = mergeOrReplaceAggregations (aggregations,
          previousAggregations)
        
        val aggSizes = aggregations.map(t => (t._1,t._2.getNumberMappings))
        logInfo (s"Aggregations and sizes: ${aggSizes.mkString(",")}")

        aggregations.foreach { case (name, agg) =>
          val mapping = agg.getMapping
          logDebug (s"Aggregation[${name}]\n" +
            s"${mapping.map(t => s"Aggregation[${name}][${superstep}]" +
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
   */
  def getExecutionEngines[E <: Embedding](
      superstepRDD: RDD[LZ4ObjectCache],
      superstep: Int,
      configBc: Broadcast[SparkConfiguration[E]],
      aggAccums: Map[String,LongAccumulator],
      previousAggregationsBc: Broadcast[_]): RDD[SparkEngine[E]] = {

    val execEngines = superstepRDD.mapPartitionsWithIndex { (idx, cacheIter) =>

      // configBc.value.initialize()
      
      configBc.value.initializeWithTag()

      //previousAggregationsBc.value.asInstanceOf[Map[String,_]].get(PREV_ENUM) match {
      //  case Some(agg) =>
      //    configBc.value.initializeWithTag(
      //      agg.asInstanceOf[AggregationStorage[NullWritable,AtomicBitSetArray]].
      //        getValue(NullWritable.get()))
      //  case None =>
      //    configBc.value.initialize()
      //}

      val execEngine = new SparkFromScratchEngine [E] (
        partitionId = idx,
        superstep = superstep,
        accums = aggAccums,
        previousAggregationsBc = previousAggregationsBc,
        configurationId = configBc.value.getId
      )

      execEngine.init()
      val prevEnum = execEngine.getAggregatedValue [
        AggregationStorage[IntWritable,LongWritable]] (PREV_ENUM)
      val numWords = execEngine.computation.getInitialNumWords()

      var semaphoreOpt: Option[AtomicLong] = None
      if (prevEnum != null) {
        semaphoreOpt = Option(prevEnum.resetAtomicArray(numWords))
      }

      execEngine.compute (cacheIter)
      execEngine.finalize()

      if (prevEnum != null && semaphoreOpt.isDefined) {
        prevEnum.setAtomicArrayDirty(semaphoreOpt.get)
      }
      
      configBc.value.uninitialize()

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

      var wordCounts: AtomicLongArray = _

      var prevEnumAgg: AggregationStorage[IntWritable,LongWritable] = _

      var writableOne: LongWritable = _

      var reusableIdWritable: IntWritable = _

      var workStealingSys: WorkStealingSystem[E] = _

      var totalNumWords: Int = _

      def getWordCounts(c: Computation[E]): AtomicLongArray = {
        val prevAgg = c.
          readAggregation[IntWritable,LongWritable](PREV_ENUM)
        if (prevAgg != null) {
          val numWords = c.getConfig().getNumWords()
          prevAgg.asAtomicArray(numWords)
        } else {
          null
        }
      }

      def apply(iter: JavaIterator[E], c: Computation[E]): Long = {
        if (c.getDepth() == 0) {

          var currComp = c.nextComputation()
          while (currComp != null) {
            currComp.setExecutionEngine(c.getExecutionEngine())
            currComp.init(c.getConfig())
            currComp.initAggregations(c.getConfig())
            currComp = currComp.nextComputation
          }

          prevEnumAgg = c.
            getAggregationStorage [IntWritable,LongWritable] (PREV_ENUM)

          wordCounts = getWordCounts(c)

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

      private def lastStepBeginsOnNextComputation(iter: JavaIterator[E],
          c: Computation[E], nextComp: Computation[E]): Long = {
        var currentEmbedding: E = null.asInstanceOf[E]
        var backtrack = 0L
        var addWords = 0L
        var embeddingsGenerated = 0L

        while (iter.hasNext) {
          backtrack = 0
          currentEmbedding = iter.next
          addWords += 1

          if (c.filter(currentEmbedding)) {

            val words = currentEmbedding.getWords
            val numWords = words.size()
            var i = 0
            while (i < numWords) {
              val currCount = wordCounts.decrementAndGet(
                words.getUnchecked(i))

              if (currCount == 0) {
                //logInfo (s"WordInvalidation step=${c.getStep}" +
                //  s" word=${words.getUnchecked(i)}")
                //EmbeddingIterator.invalidateWords(wordCounts,
                //  words.getUnchecked(i), currentEmbedding.getNumWords - 1)
                backtrack = backtrack max (
                  numWords - (i + 1))
              } else if (currCount < 0) {
                throw new RuntimeException(
                  s"Counter should never be less than zero:" +
                  s" belowzero step=${c.getStep}" +
                  s" partitionId=${c.getPartitionId}" +
                  s" embedding=${currentEmbedding}" +
                  s" word=${words.getUnchecked(i)}" +
                  s" currComp=${currCount} computation=${c}")
              }

              i += 1
            }

            embeddingsGenerated += 1
            currentEmbedding.nextExtensionLevel
            backtrack = backtrack max nextComp.compute(currentEmbedding)
            currentEmbedding.previousExtensionLevel
            
            if (backtrack > 0) {
              if (iter.isInstanceOf[EmbeddingIterator[E]]) {
                val _iter = iter.asInstanceOf[EmbeddingIterator[E]]
                _iter.invalidate()
              }
              awAccums(c.getDepth).add(addWords)
              egAccums(c.getDepth).add(embeddingsGenerated)
              return backtrack - 1
            }
          }
        }
        
        awAccums(c.getDepth).add(addWords)
        egAccums(c.getDepth).add(embeddingsGenerated)

        backtrack
      }

      private def hasNextComputation(iter: JavaIterator[E],
          c: Computation[E], nextComp: Computation[E]): Long = {
        var currentEmbedding: E = null.asInstanceOf[E]
        var backtrack = 0L
        var addWords = 0L
        var embeddingsGenerated = 0L

        while (iter.hasNext) {
          backtrack = 0
          currentEmbedding = iter.next
          addWords += 1

          if (c.filter(currentEmbedding)) {

            embeddingsGenerated += 1
            currentEmbedding.nextExtensionLevel
            backtrack = backtrack max nextComp.compute(currentEmbedding)
            currentEmbedding.previousExtensionLevel

            if (backtrack > 0) {
              if (iter.isInstanceOf[EmbeddingIterator[E]]) {
                val _iter = iter.asInstanceOf[EmbeddingIterator[E]]
                _iter.invalidate()
              }
              awAccums(c.getDepth).add(addWords)
              egAccums(c.getDepth).add(embeddingsGenerated)
              return backtrack - 1
            }
          }
        }
        
        awAccums(c.getDepth).add(addWords)
        egAccums(c.getDepth).add(embeddingsGenerated)

        backtrack
      }

      private def lastComputationNoAgg(iter: JavaIterator[E],
          c: Computation[E]): Long = {
        var currentEmbedding: E = null.asInstanceOf[E]
        var backtrack = 0L
        var addWords = 0L
        var embeddingsGenerated = 0L

        while (iter.hasNext) {
          currentEmbedding = iter.next
          addWords += 1
          if (c.filter(currentEmbedding)) {
            embeddingsGenerated += 1
            if (c.shouldExpand(currentEmbedding)) {
              c.getExecutionEngine().processExpansion(currentEmbedding)
            }
            c.process(currentEmbedding)
          }
        }
        
        awAccums(c.getDepth).add(addWords)
        egAccums(c.getDepth).add(embeddingsGenerated)

        backtrack
      }

      private def lastComputation(iter: JavaIterator[E],
          c: Computation[E]): Long = {
        var currentEmbedding: E = null.asInstanceOf[E]
        var backtrack = 0L
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
                prevEnumAgg.
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

        backtrack
      }

      private def processCompute(iter: JavaIterator[E],
          c: Computation[E]): Long = {
        val nextComp = c.nextComputation()

        if (nextComp != null) {
          nextComp.setExecutionEngine(c.getExecutionEngine())
          nextComp.init(c.getConfig())
          nextComp.initAggregations(c.getConfig())

          if (wordCounts != null &&
              nextComp.computationLabel() == "last_step_begins") {
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

  def getWordFilterFunc(
      _cfAccums: Array[LongAccumulator],
      _gfAccums: Array[LongAccumulator],
      _gfcfAccums: Array[LongAccumulator]): WordFilterFunc[E] = {
    new WordFilterFunc[E] {
      val cfAccums = _cfAccums

      val gfAccums = _gfAccums

      val gfcfAccums = _gfcfAccums

      var wordCountsOpt: Option[AtomicLongArray] = None

      def getWordCounts(c: Computation[E])
        : AtomicLongArray = wordCountsOpt match {
        case Some(wordCounts) =>
          wordCounts

        case None =>
          val wordCounts = c.
            readAggregation[IntWritable,LongWritable](PREV_ENUM).
            asAtomicArray(c.getConfig().getNumWords())
          wordCountsOpt = Option(wordCounts)
          wordCounts
      }

      def apply(e: E, w: Int, c: Computation[E]): Boolean = {
        getWordCounts(c).get(w) > 0
      }
    }
  }

  def getLastWordFilterFunc(
      _cfAccums: Array[LongAccumulator],
      _gfAccums: Array[LongAccumulator],
      _gfcfAccums: Array[LongAccumulator]): WordFilterFunc[E] = {
    new WordFilterFunc[E] {
      val cfAccums = _cfAccums

      val gfAccums = _gfAccums

      val gfcfAccums = _gfcfAccums

      var wordCountsOpt: Option[AtomicLongArray] = None

      def getWordCounts(c: Computation[E])
        : AtomicLongArray = wordCountsOpt match {
        case Some(wordCounts) =>
          wordCounts

        case None =>
          val wordCounts = c.
            readAggregation[IntWritable,LongWritable](PREV_ENUM).
            asAtomicArray(c.getConfig().getNumWords())
          wordCountsOpt = Option(wordCounts)
          wordCounts
      }

      def apply(e: E, w: Int, c: Computation[E]): Boolean = {
        getWordCounts(c).get(w) != -1
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
      c.getConfig().registerAggregation [IntWritable,LongWritable] (name,
        aggStorageClass, keyClass, valueClass, persistent,
        reductionFunc, endAggregationFunc, false)
    }
  }
}

object SparkGtagMasterEngine {
  val PREV_ENUM = "previous_enumeration"
  val AGG_GTAG_FILTER = "gtag_filter"
}
