package br.ufmg.cs.systems.fractal.computation

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic._

import akka.actor._
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.Map

/**
 */
case class SparkFromScratchEngine[E <: Subgraph](
                                                  partitionId: Int,
                                                  step: Int,
                                                  accums: Map[String,LongAccumulator],
                                                  previousAggregationsBc: Broadcast[_],
                                                  configurationId: Int) extends SparkEngine[E] {

  @transient var slaveActorRef: ActorRef = _

  override def init() = {
    val start = System.currentTimeMillis

    super.init()

    // accumulators
    numSubgraphsOutput = 0

    // actor
    slaveActorRef = ActorMessageSystem.createActor(this)

    // register computation
    SparkFromScratchEngine.registerComputation(computation)

    logInfo(s"Started slave-actor(step=${step}," +
      s" partitionId=${partitionId}): ${slaveActorRef}")

    val end = System.currentTimeMillis

    logInfo(s"SparkFromScratchEngine(step=${step},partitionId=${partitionId}" +
      s" took ${(end - start)} ms to initialize.")
  }

  /**
   * Releases resources allocated for this instance
   */
  override def finalize() = {
    super.finalize()
    slaveActorRef = null
    // make sure we close writers
    if (outputStreamOpt.isDefined) outputStreamOpt.get.close
    if (subgraphWriterOpt.isDefined) subgraphWriterOpt.get.close
  }

  /**
   * It does the computation of this module, i.e., expand/compute
   *
   * @param inboundCaches
   */
  def compute(): Unit = {
    val start = System.currentTimeMillis
    val subgraph: E = configuration.createSubgraph()
    val ret = computation.compute (subgraph)
    aggregationStorages.foreach {
      case (name, agg) =>
        aggregateAndSplitFinalAggregation(name, agg)
    }
    flushStatsAccumulators
    val elapsed = System.currentTimeMillis - start
    logInfo(s"SparkFromScratchEngine(step=${step},partitionId=${partitionId}" +
      s" took ${elapsed} ms to compute.")
  }

}

object SparkFromScratchEngine extends Logging {
  private val nextIdxs
    : ConcurrentHashMap[Int, AtomicInteger] = new ConcurrentHashMap()
  
  private val activeComputationsIdx
    : ConcurrentHashMap[Int, ConcurrentHashMap[Int,Int]] = new ConcurrentHashMap()

  private val activeComputations
    : ConcurrentHashMap[Int, Array[Computation[_]]] = new ConcurrentHashMap()

  def localComputations [E <: Subgraph] (step: Int): Array[Computation[E]] = {
    activeComputations.get(step).asInstanceOf[Array[Computation[E]]]
  }
  
  def localComputation [E <: Subgraph] (
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
