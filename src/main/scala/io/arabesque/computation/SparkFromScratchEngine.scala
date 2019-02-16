package io.arabesque.computation

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic._

import akka.actor._
import io.arabesque.cache.LZ4ObjectCache
import io.arabesque.conf.Configuration
import io.arabesque.embedding._
import io.arabesque.utils.Logging
import org.apache.hadoop.io.SequenceFile.{Writer => SeqWriter}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.{LongAccumulator, SizeEstimator}

import scala.collection.mutable.Map

/**
 */
case class SparkFromScratchEngine[E <: Embedding](
    partitionId: Int,
    superstep: Int,
    accums: Map[String,LongAccumulator],
    previousAggregationsBc: Broadcast[_],
    configurationId: Int) extends SparkEngine[E] {

  @transient var currentCache: LZ4ObjectCache = _

  @transient var gtagActorRef: ActorRef = _

  var validEmbeddings: AtomicLong = _

  override def init() = {
    val start = System.currentTimeMillis

    super.init()

    // initial embedding cache
    currentCache = null

    // accumulators
    numEmbeddingsProcessed = 0
    numEmbeddingsGenerated = 0
    numEmbeddingsOutput = 0

    // gtag actor
    gtagActorRef = GtagMessagingSystem.createActor(this)
    
    // register computation
    SparkFromScratchEngine.registerComputation(computation)

    logInfo(s"Started slave-actor(step=${superstep}," +
      s" partitionId=${partitionId}): ${gtagActorRef}")

    validEmbeddings = new AtomicLong(0)

    val end = System.currentTimeMillis

    logInfo(s"SparkFromScratchEngine(step=${superstep},partitionId=${partitionId}" +
      s" took ${(end - start)} ms to initialize.")
  }

  /**
   * Releases resources allocated for this instance
   */
  override def finalize() = {
    super.finalize()
    currentCache = null
    gtagActorRef = null
    // make sure we close writers
    if (outputStreamOpt.isDefined) outputStreamOpt.get.close
    if (embeddingWriterOpt.isDefined) embeddingWriterOpt.get.close
  }

  /**
   * It does the computation of this module, i.e., expand/compute
   *
   * @param inboundCaches
   */
  def compute(inboundCaches: Iterator[LZ4ObjectCache]): Unit = {
    val start = System.currentTimeMillis
    expansionCompute (inboundCaches)
    aggregationStorages.foreach {
      case (name, agg) =>
        aggregateAndSplitFinalAggregation(name, agg)
    }
    flushStatsAccumulators
    val elapsed = System.currentTimeMillis - start
    logInfo(s"SparkFromScratchEngine(step=${superstep},partitionId=${partitionId}" +
      s" took ${elapsed} ms to compute.")
  }

  /**
   * Iterates over embedding caches and call expansion/compute procedures on
   * them. It also bootstraps the cycle by requesting empty embedding from
   * configuration and expanding them.
   *
   * @param inboundCaches iterator of embedding caches
   */
  private def expansionCompute(
      inboundCaches: Iterator[LZ4ObjectCache]): Unit = {
    if (inboundCaches.isEmpty) { // bootstrap

      val initialEmbedd: E = configuration.createEmbedding()
      computation.compute (initialEmbedd)

    } else {
      var hasNext = true
      var embedding: E = null.asInstanceOf[E]
      while (hasNext) {
        embedding = getNextInboundEmbedding (inboundCaches)
        if (embedding == null) {
          hasNext = false
        } else {
          computation.compute (embedding)
          numEmbeddingsProcessed += 1
        }
      }
    }
  }

  /**
   * Reads next embedding from previous caches
   *
   * @param remainingCaches
   * @return some embedding or none
   */
  @scala.annotation.tailrec
  private def getNextInboundEmbedding(
      remainingCaches: Iterator[LZ4ObjectCache]): E = {
    if (currentCache == null) {
      if (remainingCaches.hasNext) {
        currentCache = remainingCaches.next
        currentCache.prepareForIteration
        getNextInboundEmbedding (remainingCaches)
      } else null.asInstanceOf[E]
    } else {
      if (currentCache.hasNext) {
        val embedding = currentCache.next.asInstanceOf[E]
        if (computation.aggregationFilter(embedding.getPattern))
          embedding
        else
          getNextInboundEmbedding (remainingCaches)
      } else {
        currentCache = null
        getNextInboundEmbedding (remainingCaches)
      }
    }
  }

  /**
   * Called whenever an embedding survives the expand/filter process and must be
   * carried on to the next superstep
   *
   * @param embedding embedding that must be processed
   */
  def addOutboundEmbedding(embedding: E) = processExpansion (embedding)

  /**
   * Adds an expansion (embedding) to the outbound odags.
   *
   * @param expansion embedding to be added to the stash of outbound odags
   */
  override def processExpansion(expansion: E) = {
    numEmbeddingsGenerated += 1
  }

  def flush: Iterator[(Int,LZ4ObjectCache)] = Iterator.empty
}

object SparkFromScratchEngine extends Logging {
  private val nextIdxs
    : ConcurrentHashMap[Int, AtomicInteger] = new ConcurrentHashMap()
  
  private val activeComputationsIdx
    : ConcurrentHashMap[Int, ConcurrentHashMap[Int,Int]] = new ConcurrentHashMap()

  private val activeComputations
    : ConcurrentHashMap[Int, Array[Computation[_]]] = new ConcurrentHashMap()

  def localComputations [E <: Embedding] (step: Int): Array[Computation[E]] = {
    activeComputations.get(step).asInstanceOf[Array[Computation[E]]]
  }
  
  def localComputation [E <: Embedding] (
      step: Int, partitionId: Int): Computation[E] = {
    val stepIdxs = activeComputationsIdx.getOrDefault(step, null)
    if (stepIdxs == null) return null

    val computationIdx = stepIdxs.getOrDefault(partitionId, -1)
    if (computationIdx == -1) return null
    
    activeComputations.get(step)(computationIdx).asInstanceOf[Computation[E]]
  }

  def createComputationsMap(step: Int, numComputations: Int): Unit = {
    activeComputations.synchronized {
      if (!activeComputations.containsKey(step)) {
        logInfo (s"Registering computation map step=${step}" +
          s" numComputations=${numComputations}")

        val newArr = new Array[Computation[_]](numComputations)
        activeComputations.put(step, newArr)
        nextIdxs.put(step, new AtomicInteger(0))
        activeComputationsIdx.put(step, new ConcurrentHashMap(numComputations))

      } else {
        val _numComputations = activeComputations.get(step).length

        if (_numComputations != numComputations) {
          throw new RuntimeException(
            s"NumberOfComputations current: ${_numComputations}" +
            s", expected: ${numComputations}")
        }
      }
    }
  }

  def registerComputation(computation: Computation[_]): Unit = {
    val step = computation.getStep
    val partitionId = computation.getPartitionId
    val computations = activeComputations.get(step)
    val computationIdx = nextIdxs.get(step).getAndIncrement()
    activeComputationsIdx.get(step).put(partitionId, computationIdx)
    computations(computationIdx) = computation

    logInfo (s"Registered computation step=${step} partitionId=${partitionId}" +
      s" computations=${computations.filter(_ != null).size}" +
      s" computationsIdx=${activeComputationsIdx.get(step)}" +
      s" nextIdxs=${nextIdxs.get(step)}")
  }
  
  def unregisterComputation(computation: Computation[_]): Unit = {
    val step = computation.getStep
    val partitionId = computation.getPartitionId
    if (nextIdxs.get(step).decrementAndGet() == 0) {
      logInfo (s"Unregistering last computation step=${step} partitionId=${partitionId}")
      activeComputations.remove(step)
      nextIdxs.remove(step)
      activeComputationsIdx.remove(step)
    } else {
      logInfo (s"Unregistering computation step=${step} partitionId=${partitionId}")
      val computations = activeComputations.get(step)
      val computationIdx = activeComputationsIdx.get(step).get(partitionId)
      computations(computationIdx) = null
    }
  }
}
