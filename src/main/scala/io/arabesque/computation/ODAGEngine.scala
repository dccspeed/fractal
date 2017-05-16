package io.arabesque.computation

import io.arabesque.aggregation.{AggregationStorage, AggregationStorageFactory}
import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding._
import io.arabesque.odag.domain.DomainEntry
import io.arabesque.odag._
import io.arabesque.odag.BasicODAGStash.EfficientReader
import io.arabesque.pattern.Pattern
import io.arabesque.utils.SerializableConfiguration

import java.io._
import java.util.concurrent.{ExecutorService, Executors}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, SequenceFile, Writable}
import org.apache.hadoop.io.SequenceFile.{Writer => SeqWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, Map}
import scala.reflect.ClassTag

trait ODAGEngine[
    E <: Embedding,           
    O <: BasicODAG,
    S <: BasicODAGStash[O,S],
    C <: ODAGEngine[E,O,S,C]
  ] extends SparkEngine[E] {

  // update aggregations before flush
  def withNewAggregations(aggregationsBc: Broadcast[_]): C

  /**
   * Stashes: it is dependent of odag and stash implementation
   */
  var currentEmbeddingStashOpt: Option[S] = None

  var nextEmbeddingStash: S = _

  @transient var odagStashReader: EfficientReader = _

  /* */
  
  /**
   * Reader parameters
   */
  lazy val numBlocks: Int =
    configuration.getInteger ("numBlocks",
      getNumberPartitions() * getNumberPartitions())

  lazy val maxBlockSize: Int =
    configuration.getInteger ("maxBlockSize", 10000) // TODO: magic number ??

  lazy val numPartitionsPerWorker = configuration.numPartitionsPerWorker

  /* */
 
  /**
   * Releases resources allocated for this instance
   */
  override def finalize() = {
    super.finalize()
    // make sure we close writers
    if (outputStreamOpt.isDefined) outputStreamOpt.get.close
    if (embeddingWriterOpt.isDefined) embeddingWriterOpt.get.close
  }

  /**
   * It does the computation of this module, i.e., expand/compute
   *
   * @param inboundStashes iterator of BasicODAG stashes
   */
  def compute(inboundStashes: Iterator[S]) = {
    logInfo (s"Computing partition(${partitionId}) of superstep ${superstep}")
    if (computed)
      throw new RuntimeException ("computation must be atomic")
    if (configuration.getEmbeddingClass() == null)
      configuration.setEmbeddingClass (computation.getEmbeddingClass())
    expansionCompute (inboundStashes)
    flushStatsAccumulators
    computed = true
  }

  /**
   * Iterates over BasicODAG stashes and call expansion/compute procedures on
   * them. It also bootstraps the cycle by requesting empty embedding from
   * configuration and expanding them.
   *
   * @param inboundStashes iterator of BasicODAG stashes
   */
  private def expansionCompute(inboundStashes: Iterator[S]): Unit = {
    if (superstep == 0) { // bootstrap

      val initialEmbedd: E = configuration.createEmbedding()
      computation.compute (initialEmbedd)

    } else {
      var hasNext = true
      while (hasNext) getNextInboundEmbedding (inboundStashes) match {
        case None =>
          hasNext = false

        case Some(embedding) =>
          computation.compute (embedding)
          numEmbeddingsProcessed += 1
      }
    }
  }

  /**
   * Reads next embedding from previous ODAGs
   *
   * @param remainingStashes iterator containing SinglePatternODAG
   * stashes which hold compressed embeddings
   * @return some embedding or none
   */
  def getNextInboundEmbedding(
      remainingStashes: Iterator[S]): Option[E] = {
    if (!currentEmbeddingStashOpt.isDefined) {
      if (remainingStashes.hasNext) {

        val currentEmbeddingStash = remainingStashes.next

        currentEmbeddingStash.finalizeConstruction (
          ODAGEngine.pool(numPartitionsPerWorker),
          numPartitionsPerWorker)
        
        // odag stashes have an efficient reader for compressed embeddings
        odagStashReader = new EfficientReader (currentEmbeddingStash,
          configuration,
          computation,
          getNumberPartitions(),
          numBlocks,
          maxBlockSize)

        currentEmbeddingStashOpt = Some(currentEmbeddingStash)

      } else return None
    }

    // new embedding was found
    if (odagStashReader.hasNext) {
      Some(odagStashReader.next.asInstanceOf[E])
    // no more embeddings to be read from current stash, try to get another
    // stash by recursive call
    } else {
      currentEmbeddingStashOpt = None
      getNextInboundEmbedding(remainingStashes)
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
    nextEmbeddingStash.addEmbedding (expansion)
    numEmbeddingsGenerated += 1
  }
}

object ODAGEngine {
  import Configuration._
  import SparkConfiguration._

  def apply [
      E <: Embedding,
      O <: BasicODAG,
      S <: BasicODAGStash[O,S],
      C <: ODAGEngine[E,O,S,C]
    ] (
      partitionId: Int,
      superstep: Int,
      accums: Map[String,Accumulator[_]],
      previousAggregationsBc: Broadcast[_],
      configuration: SparkConfiguration[E]): C = 
    configuration.getString(CONF_COMM_STRATEGY,
      CONF_COMM_STRATEGY_DEFAULT) match {
      case COMM_ODAG_SP =>
        new ODAGEngineSP [E] (partitionId, superstep,
          accums, previousAggregationsBc, configuration).asInstanceOf[C]
      case COMM_ODAG_MP =>
        new ODAGEngineMP [E] (partitionId, superstep,
          accums, previousAggregationsBc, configuration).asInstanceOf[C]
  }

  // pool related vals
  private var poolOpt: Option[ExecutorService] = None
  
  def pool(poolSize: Int) = poolOpt match {
    case Some(pool) => pool
    case None =>
      val pool = Executors.newFixedThreadPool (poolSize)
      poolOpt = Some(pool)
      pool
  }

  def shutdownPool: Unit = poolOpt match {
    case Some(pool) => pool.shutdown()
    case None =>
  }

  override def finalize: Unit = shutdownPool
}
