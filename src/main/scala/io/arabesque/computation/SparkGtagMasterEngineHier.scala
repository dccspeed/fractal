package io.arabesque.computation

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicLong, AtomicLongArray}
import java.util.{Iterator => JavaIterator}

import io.arabesque.aggregation._
import io.arabesque.aggregation.reductions._
import io.arabesque.cache.LZ4ObjectCache
import io.arabesque.conf.SparkConfiguration
import io.arabesque.embedding._
import io.arabesque.{ProcessComputeFunc, WordFilterFunc}
import org.apache.hadoop.io.{IntWritable, LongWritable}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.Map

/**
 */
class SparkGtagMasterEngineHier[E <: Embedding](
    _config: SparkConfiguration[E],
    _parentOpt: Option[SparkMasterEngine[E]])
  extends SparkGtagMasterEngine [E] (_config, _parentOpt) {

  import SparkGtagMasterEngine._

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

  /**
   * Creates an RDD of execution engines
   */
  override def getExecutionEngines[E <: Embedding](
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

  override def getProcessComputeFunc(_egAccums: Array[LongAccumulator],
      _awAccums: Array[LongAccumulator]): ProcessComputeFunc[E] = {
    new ProcessComputeFunc[E] {
      val numComputations = _egAccums.length

      val egAccums = _egAccums

      val awAccums = _awAccums

      var wordCounts: Array[AtomicLongArray] = new Array(numComputations)

      var prevEnumAggs: Array[AggregationStorage[IntWritable,LongWritable]] =
        new Array(numComputations)

      var writableOne: LongWritable = _

      var reusableIdWritable: IntWritable = _

      var workStealingSys: WorkStealingSystem[E] = _

      var totalNumWords: Int = _

      def getWordCounts(c: Computation[E]): AtomicLongArray = {
        val depth = c.getDepth
        if (wordCounts(depth) == null) {
          val prevAgg = c.
            readAggregation[IntWritable,LongWritable](s"${PREV_ENUM}_${depth}")
          if (prevAgg != null) {
            val numWords = c.getConfig().getNumWords()
            wordCounts(depth) = prevAgg.asAtomicArray(numWords)
          }
        }
        wordCounts(depth)
      }

      def apply(iter: JavaIterator[E], c: Computation[E]): Long = {
        if (c.getDepth() == 0) {

          var depth = 0
          var currComp = c
          while (depth < numComputations) {
            prevEnumAggs(depth) = currComp.
              getAggregationStorage [IntWritable,LongWritable] (
                s"${PREV_ENUM}_${depth}")

            wordCounts(depth) = getWordCounts(currComp)
            depth += 1
            currComp = currComp.nextComputation

            if (currComp != null) {
              currComp.setExecutionEngine(c.getExecutionEngine())
              currComp.init(c.getConfig())
              currComp.initAggregations(c.getConfig())
            }
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
            val numWords = currentEmbedding.getNumWords
            var i = 0
            while (i < numWords) {
              val currCount = wordCounts(i).decrementAndGet(
                words.getUnchecked(i))

              if (currCount == 0) {
                //EmbeddingIterator.invalidateWords(wordCounts,
                //  words.getUnchecked(i), currentEmbedding.getNumWords - 1)
                backtrack = backtrack max (
                  currentEmbedding.getNumWords - (i + 1))
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

        backtrack
      }

      private def processCompute(iter: JavaIterator[E],
          c: Computation[E]): Long = {
        val nextComp = c.nextComputation()

        if (nextComp != null) {

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

  override def getWordFilterFunc(
      _cfAccums: Array[LongAccumulator],
      _gfAccums: Array[LongAccumulator],
      _gfcfAccums: Array[LongAccumulator]): WordFilterFunc[E] = {
    new WordFilterFunc[E] {
      val cfAccums = _cfAccums

      val gfAccums = _gfAccums

      val gfcfAccums = _gfcfAccums

      val wordCounts = new Array[AtomicLongArray](_cfAccums.length)

      def getWordCounts(c: Computation[E]): AtomicLongArray = {
        val depth = c.getDepth
        if (wordCounts(depth) == null) {
          wordCounts(depth) = c.
            readAggregation[IntWritable,LongWritable](
              s"${PREV_ENUM}_${depth}").
            asAtomicArray(c.getConfig().getNumWords())
        }
        wordCounts(depth)
      }

      def apply(e: E, w: Int, c: Computation[E]): Boolean = {
        getWordCounts(c).get(w) > 0
      }
    }
  }

  override def getLastWordFilterFunc(
      _cfAccums: Array[LongAccumulator],
      _gfAccums: Array[LongAccumulator],
      _gfcfAccums: Array[LongAccumulator]): WordFilterFunc[E] = {
    new WordFilterFunc[E] {
      val cfAccums = _cfAccums

      val gfAccums = _gfAccums

      val gfcfAccums = _gfcfAccums

      val wordCounts = new Array[AtomicLongArray](_cfAccums.length)

      def getWordCounts(c: Computation[E], depth: Int): AtomicLongArray = {
        if (wordCounts(depth) == null) {
          wordCounts(depth) = c.
            readAggregation[IntWritable,LongWritable](
              s"${PREV_ENUM}_${depth}").
            asAtomicArray(c.getConfig().getNumWords())
        }
        wordCounts(depth)
      }

      def apply(e: E, w: Int, c: Computation[E]): Boolean = {
        var depth = 0
        while (depth < wordCounts.length - 1) {
          if (getWordCounts(c, depth).get(w) == -1) {
            return false
          }
          depth += 1
        }
        true
      }
    }
  }

  override def aggregationRegister(cc: ComputationContainer[E], depth: Int)
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
