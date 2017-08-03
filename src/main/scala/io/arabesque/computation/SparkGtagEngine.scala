package io.arabesque.computation

import akka.actor._
import com.typesafe.config.ConfigFactory

import io.arabesque.aggregation.{AggregationStorage, AggregationStorageFactory}
import io.arabesque.cache.LZ4ObjectCache
import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding._
import io.arabesque.utils.collection.IntArrayList
import io.arabesque.utils.{Logging, SerializableConfiguration}

import java.io._
import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, Writable, SequenceFile}
import org.apache.hadoop.io.SequenceFile.{Writer => SeqWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConversions._
import scala.collection.mutable.{Map, Set}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Random

/**
 */
case class SparkGtagEngine[E <: Embedding](
    partitionId: Int,
    superstep: Int,
    accums: Map[String,Accumulator[_]],
    previousAggregationsBc: Broadcast[_],
    configuration: SparkConfiguration[E]) extends SparkEngine[E] {
  
  var currentCache: LZ4ObjectCache = _

  var gtagActorRef: ActorRef = _

  override def init() = {
    val start = System.currentTimeMillis

    super.init()

    // register computation
    SparkGtagEngine.registerComputation(computation)
    
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

    val end = System.currentTimeMillis

    logInfo(s"${this} took ${(end - start)}ms to initialize.")
  }

  /**
   * Releases resources allocated for this instance
   */
  override def finalize() = {
    super.finalize()
    // remove from active computations
    SparkGtagEngine.unregisterComputation(computation)
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
      while (hasNext) getNextInboundEmbedding (inboundCaches) match {
        case None =>
          hasNext = false

        case Some(embedding) =>
          computation.compute (embedding)
          numEmbeddingsProcessed += 1
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
      remainingCaches: Iterator[LZ4ObjectCache]): Option[E] = {
    if (currentCache == null) {
      if (remainingCaches.hasNext) {
        currentCache = remainingCaches.next
        currentCache.prepareForIteration
        getNextInboundEmbedding (remainingCaches)
      } else None
    } else {
      if (currentCache.hasNext) {
        val embedding = currentCache.next.asInstanceOf[E]
        if (computation.aggregationFilter(embedding.getPattern))
          Some(embedding)
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

object SparkGtagEngine extends Logging {
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
