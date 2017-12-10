package io.arabesque.computation

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import akka.actor._
import io.arabesque.cache.LZ4ObjectCache
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

    // register computation
    SparkFromScratchEngine.registerComputation(computation)

    // initial embedding cache
    currentCache = null

    // accumulators
    numEmbeddingsProcessed = 0
    numEmbeddingsGenerated = 0
    numEmbeddingsOutput = 0

    // gtag actor
    gtagActorRef = GtagMessagingSystem.createActor(this)

    logInfo(s"Started gtag-actor(step=${superstep}," +
      s" partitionId=${partitionId}): ${gtagActorRef}")

    validEmbeddings = new AtomicLong(0)

    val end = System.currentTimeMillis

    logInfo(s"SparkGtagEngine(step=${superstep},partitionId=${partitionId}" +
      s" took ${(end - start)} ms to initialize.")
  }

  /**
   * Releases resources allocated for this instance
   */
  override def finalize() = {
    super.finalize()
    currentCache = null
    gtagActorRef = null
    // remove from active computations
    // SparkFromScratchEngine.unregisterComputation(computation)
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
    expansionCompute (inboundCaches)
    aggregationStorages.foreach {
      case (name, agg) =>
        aggregateAndSplitFinalAggregation(name, agg)
    }
    flushStatsAccumulators
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
  lazy val activeComputations
    : ConcurrentHashMap[(Int,Int),Computation[_]] = new ConcurrentHashMap()

  def registerComputation(computation: Computation[_]): Computation[_] = {
    activeComputations.put(
      (computation.getStep, computation.getPartitionId), computation)
  }

  def unregisterComputation(computation: Computation[_]): Computation[_] = {
    activeComputations.remove((computation.getStep, computation.getPartitionId))
  }
}
