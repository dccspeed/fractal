package br.ufmg.cs.systems.fractal.computation

import java.io.Serializable
import java.util.concurrent.atomic._
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Executors, ThreadFactory}

import akka.actor._
import br.ufmg.cs.systems.fractal.aggregation._
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList
import com.koloboke.collect.map._
import com.koloboke.collect.map.hash.{HashIntIntMaps, HashIntObjMaps}
import org.apache.spark.TaskContext

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, ExecutionContextExecutorService, Future}

class SparkFromScratchEngine[S <: Subgraph]
(
   val partitionId: Int,
   val step: Int,
   val computation: Computation[S],
   val configuration: SparkConfiguration) extends SparkEngine[S] {

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

   val computationCopy: Computation[S] = {
      val comp = SparkConfiguration.clone(computation)
      if (configuration.getSubgraphClass() == null) {
         configuration.setSubgraphClass(comp.getSubgraphClass())
      }
      comp.init(this, configuration)
      comp
   }

   private def ensureExecutionContext(): Unit = {
      if (executionContext != null) return
      // executor service
      val threadName = Thread.currentThread().getName
      val threadFactory = new ThreadFactory {
         override def newThread(runnable: Runnable): Thread = {
            new Thread(runnable, s"FractalWorkerThread(${threadName})")
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
      if (configuration.externalWsEnabled()) {
         slaveActorRef = ActorMessageSystem.createActor(this)
         logInfo(s"StartedSlaveActor step=${step} stageId=${stageId}" +
            s" id=${partitionId} ${slaveActorRef}")
      }

      if (configuration.wsEnabled()) {
         LocalComputationStore.createComputationsMap(this)
         LocalComputationStore.registerComputation(computation)
      }

      initTimeEnd = System.nanoTime()
   }

   /**
    * Releases resources allocated for this instance
    */
   override def finalizeEngine(): Unit = {
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
            logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
               s" id=${partitionId} depth=${d}" +
               s" extensions_valid ${comp.getNumValidExtensions}")
            logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
               s" id=${partitionId} depth=${d}" +
               s" extensions_canonical ${comp.getNumCanonicalExtensions}")
            comp = comp.nextComputation()
         }
      }

      // clear-up resources
      if (executionContext != null) executionContext.shutdown()

      if (configuration.externalWsEnabled()) {
         slaveActorRef ! Terminate
         slaveActorRef = null
      }

      if (configuration.wsEnabled()) {
         LocalComputationStore.unregisterComputation(this)
      }
   }

   override def getSubgraphAggregation() = subgraphAggregation

   private def compute(): Unit = {
      computationTimeStart = System.nanoTime()

      val subgraphEnumerator = computation.getSubgraphEnumerator
      subgraphEnumerator.computeFirstLevelExtensions()
      computation.processCompute(subgraphEnumerator)

      logInfo(s"WorkStealingStart step=${step} stageId=${stageId}" +
         s" id=${partitionId}")
      computationWorkStealingTimeStart = System.nanoTime()
      val workStealingSystem = new WorkStealingSystem[S](computation)
      workStealingSystem.workStealingCompute(computation)
      computationWorkStealingTimeEnd = System.nanoTime()

      computationTimeEnd = System.nanoTime()
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
      val objLongIterator = new ObjLongIteratorConsumer[S,K](objLongSubgraphAggregation)

      // finish consumer after producer finished producing (async)
      computeFuture.onComplete { _ =>
         objLongIterator.finishIterator
         finalizeEngine()
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
      val objObjIterator = new ObjObjIteratorConsumer[S,K,V](objObjSubgraphAggregation)

      // finish consumer after producer finished producing (async)
      computeFuture.onComplete { _ =>
         objObjIterator.finishIterator
         finalizeEngine()
      }(executionContext)

      objObjIterator
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
      val longLongIterator = new LongLongIteratorConsumer[S](longLongSubgraphAggregation)

      // finish consumer after producer finished producing (async)
      computeFuture.onComplete { _ =>
         longLongIterator.finishIterator
         finalizeEngine()
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
      val longObjIterator = new LongObjteratorConsumer[S,V](longObjSubgraphAggregation)

      // finish consumer after producer finished producing (async)
      computeFuture.onComplete { _ =>
         longObjIterator.finishIterator
         finalizeEngine()
      }(executionContext)

      longObjIterator
   }
}

