package br.ufmg.cs.systems.fractal.computation

import java.io.Serializable
import java.util.concurrent.{Executors, ThreadFactory}

import akka.actor._
import br.ufmg.cs.systems.fractal
import br.ufmg.cs.systems.fractal.Primitive
import br.ufmg.cs.systems.fractal.aggregation._
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.{FractalThreadStats, Logging, ReflectionSerializationUtils}
import one.profiler.{AsyncProfiler, Events}
import org.apache.spark.TaskContext
import org.apache.spark.util.CollectionAccumulator

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

class SparkFromScratchEngine[S <: Subgraph]
(
   val partitionId: Int,
   val step: Int,
   val computation: Computation[S],
   val configuration: SparkConfiguration,
   val threadStatusAccum: CollectionAccumulator[FractalThreadStats])
   extends SparkEngine[S] {

   private var previous: SparkFromScratchEngine[_ <: Subgraph] = _
   private var next: SparkFromScratchEngine[_ <: Subgraph] = _

   @transient var slaveActorRef: ActorRef = _

   private var subgraphAggregation: SubgraphAggregation[S] = _

   private var executionContext: ExecutionContextExecutorService = _

   override def slaveActor(): ActorRef = slaveActorRef

   private var computationTimeStart: Long = _
   private var computationTimeEnd: Long = _
   private var initTimeStart: Long = _
   private var initTimeEnd: Long = _

   private var computationWorkStealingTimeStart: Long = _
   private var computationWorkStealingTimeEnd: Long = _

   private def ensureExecutionContext(): Unit = {
      if (executionContext != null) return
      // executor service
      val engine = this
      val threadFactory = new ThreadFactory {
         override def newThread(runnable: Runnable): Thread = {
            new FractalWorkerThread(runnable, engine)
         }
      }

      executionContext = ExecutionContext.fromExecutorService(
         Executors.newSingleThreadExecutor(threadFactory)
      )
   }

   override def init(): Unit = {
      initTimeStart = System.nanoTime()
      super.init()

      // actor
      if (previous == null && configuration.externalWsEnabled()) {
         slaveActorRef = ActorMessageSystem.createActor(this)
         logInfo(s"StartedSlaveActor step=${step} stageId=${stageId}" +
            s" id=${partitionId} ${slaveActorRef}")
      }

      if (previous == null && configuration.wsEnabled()) {
         LocalComputationStore.createComputationsMap(this)
         LocalComputationStore.registerComputation(computation)
      }

      initTimeEnd = System.nanoTime()

      if (previous != null) previous.init()
   }

   /**
    * Releases resources allocated for this instance
    */
   override def finalizeEngine(): Unit = {
      if (previous != null) previous.finalizeEngine()

      val initElapsedTime = (initTimeEnd - initTimeStart) * 1e-9
      val computeElapsedTime = (
         computationTimeEnd - computationTimeStart) * 1e-9
      val computeWorkStealingElapsedTime = (
         computationWorkStealingTimeEnd - computationWorkStealingTimeStart) *
         1e-9
      logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
         s" id=${partitionId}" +
         s" init_time ${initElapsedTime}")
      logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
         s" id=${partitionId}" +
         s" init_timeline ${initTimeStart} ${initTimeEnd}")
      logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
         s" id=${partitionId}" +
         s" compute_time ${computeElapsedTime}")
      logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
         s" id=${partitionId}" +
         s" compute_timeline ${computationTimeStart} ${computationTimeEnd}")
      logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
         s" id=${partitionId}" +
         s" compute_workstealing_time ${computeWorkStealingElapsedTime}")
      logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
         s" id=${partitionId}" +
         s" compute_workstealing_timeline ${computationWorkStealingTimeStart} " +
         s"${computationWorkStealingTimeEnd}")
      logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
         s" id=${partitionId}" +
         s" subgraph_throughput" +
         s" ${computation.lastComputation().getNumValidExtensions / computeElapsedTime}")


      var comp = computation
      while (comp != null) {
         val d = comp.getDepth
         logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
            s" id=${partitionId} depth=${d}" +
            s" extensions_valid ${comp.getNumValidExtensions}")
         logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
            s" id=${partitionId} depth=${d}" +
            s" primitives=${primitives.take(d + 1).mkString("-")}" +
            s" extensions_canonical ${comp.getNumCanonicalExtensions}")
         logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
            s" id=${partitionId} depth=${d}" +
            s" internal_work_steals ${comp.getInternalWorkSteals}")
         logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
            s" id=${partitionId} depth=${d}" +
            s" external_work_steals ${comp.getExternalWorkSteals}")
         comp = comp.nextComputation()
      }

      if (Configuration.INSTRUMENTATION_ENABLED) {
         var comp = computation
         while (comp != null) {
            val d = comp.getDepth
            logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
               s" id=${partitionId} depth=${d}" +
               s" extensions ${comp.getNumExtensions}")
            logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
               s" id=${partitionId} depth=${d}" +
               s" extensions_unique ${comp.getNumUniqueExtensions}")
            comp = comp.nextComputation()
         }
      }

      // clear-up resources
      if (executionContext != null) executionContext.shutdown()

      if (previous == null && configuration.externalWsEnabled()) {
         slaveActorRef ! Terminate
         slaveActorRef = null
      }

      if (threadStatusAccum != null) {
         val threadStatus = new FractalThreadStats(computation)
         threadStatusAccum.add(threadStatus)
      }

      if (previous == null && configuration.wsEnabled()) {
         LocalComputationStore.unregisterComputation(this)
      }
   }

   override def getSubgraphAggregation() = subgraphAggregation

   private def compute(): Unit = {
      computationTimeStart = System.nanoTime()
      if (previous != null) {
         previous.compute()
      } else {
         val subgraphEnumerator = computation.getSubgraphEnumerator
         subgraphEnumerator.computeFirstLevelExtensions_EXTENSION_PRIMITIVE()
         computation.processCompute(subgraphEnumerator)
         workStealingCompute()
      }
      computationTimeEnd = System.nanoTime()
   }

   override def initialWorkCompute(): Unit = {
      val subgraphEnumerator = computation.getSubgraphEnumerator
      subgraphEnumerator.computeExtensions_EXTENSION_PRIMITIVE()
      computation.processCompute(subgraphEnumerator)
   }

   private def workStealingCompute(): Unit = {
      logInfo(s"WorkStealingStart step=${step} stageId=${stageId}" +
         s" id=${partitionId}")
      computationWorkStealingTimeStart = System.nanoTime()
      val workStealingSystem = new WorkStealingSystem[S](computation)
      workStealingSystem.workStealingCompute_WORK_STEALING(computation)
      computationWorkStealingTimeEnd = System.nanoTime()
   }

   /**
    * This call starts the step computation of this engine and aggregates the
    * valid subgraphs into a single long number
    *
    * @param longSubgraphAggregation
    * @return a single long
    */
   override def computeAggregationLong
   (longSubgraphAggregation: LongSubgraphAggregation[S])
   : Long = {
      subgraphAggregation = longSubgraphAggregation
      longSubgraphAggregation.init(configuration)
      init()
      compute()
      finalizeEngine()
      longSubgraphAggregation.value()
   }

   /**
    * This call starts this engine computation and aggregates the valid
    * subgraphs by key/value, where value is a long.
    *
    * @param objLongSubgraphAggregation
    * @tparam K key type parameter
    * @return iterator of (K,Long) to be consumed downstream
    */
   override def computeAggregationObjLong[K <: Serializable]
   (objLongSubgraphAggregation: ObjLongSubgraphAggregation[S, K])
   : Iterator[(K, Long)] = {
      // initialization
      subgraphAggregation = objLongSubgraphAggregation
      objLongSubgraphAggregation.init(configuration)
      init()
      ensureExecutionContext()

      // future acting as a key/value *producer* (async)
      val computeFuture = Future(compute())(executionContext)

      // iterator acting as a key/value *consumer*
      val objLongIterator = new ObjLongIteratorConsumer[S,K](
         objLongSubgraphAggregation, () => {finalizeEngine()})

      // finish consumer after producer finished producing (async)
      computeFuture.onComplete { _ =>
         objLongIterator.finishIterator
      }(executionContext)

      objLongIterator
   }

   /**
    * This call starts this engine computation and aggregates the valid
    * subgraphs by key/value, where both keys and values are objects
    *
    * @param objObjSubgraphAggregation
    * @tparam K key type parameter
    * @tparam V value type parameter
    * @return an iterator of (K,V) to be consumed downstream
    */
   override def computeAggregationObjObj[K <: Serializable, V <: Serializable]
   (objObjSubgraphAggregation: ObjObjSubgraphAggregation[S, K, V])
   : Iterator[(K, V)] = {
      // initialization
      subgraphAggregation = objObjSubgraphAggregation
      objObjSubgraphAggregation.init(configuration)
      init()
      ensureExecutionContext()

      // future acting as a key/value *producer* (async)
      val computeFuture = Future(compute())(executionContext)

      // iterator acting as a key/value *consumer*
      val objObjIterator = new ObjObjIteratorConsumer[S,K,V](
         objObjSubgraphAggregation, () => {finalizeEngine()})

      // finish consumer after producer finished producing (async)
      computeFuture.onComplete { _ =>
         objObjIterator.finishIterator
      }(executionContext)

      objObjIterator
   }

   /**
    * This call starts this engine computation and aggregates the valid
    * subgraphs by key/value, where value is an int.
    *
    * @param intIntSubgraphAggregation
    * @return iterator of (Int,Int) to be consumed downstream
    */
   override def computeAggregationIntInt
   (intIntSubgraphAggregation: IntIntSubgraphAggregation[S])
   : Iterator[(Int, Int)] = {
      // initialization
      subgraphAggregation = intIntSubgraphAggregation
      intIntSubgraphAggregation.init(configuration)
      init()
      ensureExecutionContext()

      // future acting as a key/value *producer* (async)
      val computeFuture = Future(compute())(executionContext)

      // iterator acting as a key/value *consumer*
      val intIntIterator = new IntIntIteratorConsumer[S](
         intIntSubgraphAggregation, () => {finalizeEngine()})

      // finish consumer after producer finished producing (async)
      computeFuture.onComplete { _ =>
         intIntIterator.finishIterator
      }(executionContext)

      intIntIterator
   }

   /**
    * This call starts this engine computation and aggregates the valid
    * subgraphs by key/value, where value is a long.
    *
    * @param longLongSubgraphAggregation
    * @return iterator of (Long,Long) to be consumed downstream
    */
   override def computeAggregationLongLong
   (longLongSubgraphAggregation: LongLongSubgraphAggregation[S])
   : Iterator[(Long, Long)] = {
      // initialization
      subgraphAggregation = longLongSubgraphAggregation
      longLongSubgraphAggregation.init(configuration)
      init()
      ensureExecutionContext()

      // future acting as a key/value *producer* (async)
      val computeFuture = Future(compute())(executionContext)

      // iterator acting as a key/value *consumer*
      val longLongIterator = new LongLongIteratorConsumer[S](
         longLongSubgraphAggregation, () => {finalizeEngine()})

      // finish consumer after producer finished producing (async)
      computeFuture.onComplete { _ =>
         longLongIterator.finishIterator
      }(executionContext)

      longLongIterator
   }

   /**
    * This call starts this engine computation and aggregates the valid
    * subgraphs by key/value, where value is a long.
    *
    * @param longObjSubgraphAggregation
    * @return iterator of (Long,Long) to be consumed downstream
    */
   override def computeAggregationLongObj
   [V <: Serializable]
   (longObjSubgraphAggregation: LongObjSubgraphAggregation[S, V])
   : Iterator[(Long, V)] = {
      // initialization
      subgraphAggregation = longObjSubgraphAggregation
      longObjSubgraphAggregation.init(configuration)
      init()
      ensureExecutionContext()

      // future acting as a key/value *producer* (async)
      val computeFuture = Future(compute())(executionContext)

      // iterator acting as a key/value *consumer*
      val longObjIterator = new LongObjteratorConsumer[S,V](
         longObjSubgraphAggregation, () => {finalizeEngine()})

      // finish consumer after producer finished producing (async)
      computeFuture.onComplete { _ =>
         longObjIterator.finishIterator
      }(executionContext)

      longObjIterator
   }

   override def getComputationTimeStart: Long = computationTimeStart

   override def getComputationTimeEnd: Long = computationTimeEnd

   override def getInitTimeStart: Long = initTimeStart

   override def getInitTimeEnd: Long = initTimeEnd

   override def getComputationWorkStealingTimeStart: Long =
      computationWorkStealingTimeStart

   override def getComputationWorkStealingTimeEnd: Long =
      computationWorkStealingTimeEnd

   def setPreviousEngine
   (previousEngine: SparkFromScratchEngine[_ <: Subgraph]): Unit = {
      this.previous = previousEngine
   }

   def setNextEngine
   (nextEngine: SparkFromScratchEngine[_ <: Subgraph]): Unit = {
      this.next = nextEngine
   }

   override def getNextEngine: ExecutionEngine[_ <: Subgraph] = {
      next
   }

   override def getPreviousEngine: ExecutionEngine[_ <: Subgraph] = {
      previous
   }

   override def toString: String = {
      s"SparkEngine(${step},${previous},${next})"
   }
}

class FractalWorkerThread(r: Runnable,
                          engine: SparkFromScratchEngine[_ <: Subgraph])
   extends Thread(r, s"FractalWorkerThread(${Thread.currentThread().getName})") {

   def getEngine(): SparkFromScratchEngine[_ <: Subgraph] = engine
}

object SparkFromScratchEngine {
   def getStageId: Int = {
      val t = Thread.currentThread()
      if (t.isInstanceOf[FractalWorkerThread]) {
         t.asInstanceOf[FractalWorkerThread].getEngine().stageId
      } else {
         TaskContext.get().stageId()
      }
   }
}
