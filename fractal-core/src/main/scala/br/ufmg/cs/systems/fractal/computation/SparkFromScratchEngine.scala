package br.ufmg.cs.systems.fractal.computation

import java.io.Serializable
import java.util.concurrent.atomic._
import java.util.concurrent.{ConcurrentHashMap, Executors, ThreadFactory}

import akka.actor._
import br.ufmg.cs.systems.fractal.aggregation._
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList
import br.ufmg.cs.systems.fractal.util.{EventTimer, Logging, OperationCounter, ReflectionUtils}
import com.koloboke.collect.map.hash.{HashIntIntMaps, HashIntObjMaps}
import com.koloboke.collect.map.{IntIntMap, IntObjMap, LongLongCursor, LongLongMap, LongObjCursor, ObjLongCursor, ObjObjCursor}
import org.apache.spark.TaskContext

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

case class SparkFromScratchEngine[S <: Subgraph]
(
   partitionId: Int,
   step: Int,
   computation: Computation[S],
   configuration: SparkConfiguration) extends SparkEngine[S] {

   @transient var slaveActorRef: ActorRef = _

   private var subgraphAggregation: SubgraphAggregation[S] = _

   private var executorContext: ExecutionContext = _

   override def slaveActor(): ActorRef = slaveActorRef

   private var computationTimeStart: Long = _
   private var computationTimeEnd: Long = _

   val computationCopy: Computation[S] = {
      val comp = SparkConfiguration.clone(computation)
      if (configuration.getSubgraphClass() == null) {
         configuration.setSubgraphClass(comp.getSubgraphClass())
      }
      comp.init(this, configuration)
      comp
   }

   override def init(): Unit = synchronized {
      val start = System.currentTimeMillis
      super.init()

      // executor service
      val threadName = Thread.currentThread().getName
      val threadFactory = new ThreadFactory {
         override def newThread(runnable: Runnable): Thread = {
            new Thread(runnable, s"FractalWorkerThread(${threadName})")
         }
      }
      executorContext = ExecutionContext.fromExecutor(
         Executors.newSingleThreadExecutor(threadFactory)
      )

      // actor
      if (configuration.externalWsEnabled()) {
         slaveActorRef = ActorMessageSystem.createActor(this)
      }

      SparkFromScratchEngine.createComputationsMap(this)

      // register computation
      SparkFromScratchEngine.registerComputation(computation)

      logInfo(s"Started slave-actor(step=${step}," +
         s" partitionId=${partitionId}): ${slaveActorRef}")

      val end = System.currentTimeMillis

      logInfo(
         s"SparkFromScratchEngine(step=${step},partitionId=${partitionId}" +
            s",stageId=${stageId},taskAttempt=${
               TaskContext.get().taskAttemptId()
            }" +
            s" took ${(end - start)} ms to initialize.")
   }

   /**
    * Releases resources allocated for this instance
    */
   override def finalizeEngine(): Unit = synchronized {
      super.finalizeEngine()
      if (slaveActorRef != null) {
         slaveActorRef ! Terminate
         slaveActorRef = null
      } else {
         SparkFromScratchEngine.unregisterComputation(this)
      }

      if (Configuration.OPCOUNTER_ENABLED) {
         computationTimeEnd = System.nanoTime()
         val elapsed = (computationTimeEnd - computationTimeStart) * 1e-9
         logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
            s" id=${partitionId}" +
            s" total_compute_time ${elapsed}")
         logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
            s" id=${partitionId}" +
            s" timeline ${computationTimeStart} ${computationTimeEnd}")
         var comp = computation
         while (comp != null) {
            val d = comp.getDepth
            logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
               s" id=${partitionId} depth=${d}" +
               s" valid_subgraphs ${comp.getValidSubgraphs}")
            logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
               s" id=${partitionId} depth=${d}" +
               s" canonical_subgraphs ${comp.getCanonicalSubgraphs}")
            logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
               s" id=${partitionId} depth=${d}" +
               s" extension_candidates ${comp.getExpansionCandidates}")
            logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
               s" id=${partitionId} depth=${d}" +
               s" total_compute_extensions_time" +
               s" ${comp.getTotalComputeExtensionsTime}")
            logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
               s" id=${partitionId} depth=${d}" +
               s" min_compute_extensions_time" +
               s" ${comp.getComputeExtensionsMin}")
            logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
               s" id=${partitionId} depth=${d}" +
               s" max_compute_extensions_time" +
               s" ${comp.getComputeExtensionsMax}")
            logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
               s" id=${partitionId} depth=${d}" +
               s" num_samples_compute_extensions" +
               s" ${comp.getComputeExtensionsNumSamples}")
            logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
               s" id=${partitionId} depth=${d}" +
               s" mean_compute_extensions" +
               s" ${comp.getComputeExtensionsRunningMean}")
            logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
               s" id=${partitionId} depth=${d}" +
               s" m2_compute_extensions" +
               s" ${comp.getComputeExtensionsRunningM2}")
            logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
               s" id=${partitionId} depth=${d}" +
               s" variance_compute_extensions" +
               s" ${
                  comp.getComputeExtensionsRunningM2 / (comp
                     .getComputeExtensionsNumSamples - 1)
               }")
            logInfo(s"ThreadStats step=${step} stageId=${stageId}" +
               s" id=${partitionId} depth=${d}" +
               s" std_compute_extensions" +
               s" ${
                  Math.sqrt(comp.getComputeExtensionsRunningM2 / (comp
                     .getComputeExtensionsNumSamples - 1))
               }")
            comp = comp.nextComputation()
         }
      }
   }

   override def getSubgraphAggregation() = subgraphAggregation

   private def run(): Unit = {
      if (EventTimer.ENABLED) {
         EventTimer.workerInstance(partitionId).finishAndStart(
            EventTimer.INITIALIZATION,
            EventTimer.ENUMERATION_FILTERING)
      }

      if (OperationCounter.ENABLED) {
         OperationCounter.clear(partitionId)
      }

      if (Configuration.OPCOUNTER_ENABLED) {
         computationTimeStart = System.nanoTime()
      }

      //computation.compute()
      val subgraphEnumerator = computation.getSubgraphEnumerator
      subgraphEnumerator.computeFirstLevelExtensions()
      computation.processCompute(subgraphEnumerator)

      if (EventTimer.ENABLED) {
         EventTimer.workerInstance(partitionId)
            .finish(EventTimer.ENUMERATION_FILTERING)
      }
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
      longSubgraphAggregation.init(configuration)
      val start = System.currentTimeMillis

      // make sure this is set before init, because callbacks may use this
      // reference
      subgraphAggregation = longSubgraphAggregation

      // init this engine, do enumeration work and finalize this engine
      init()
      run()
      finalizeEngine()

      val elapsed = System.currentTimeMillis - start
      logInfo(s"SparkFromScratchEngineLongAggregation(step=${step}" +
         s",stageId=${stageId}" +
         s",partitionId=${partitionId}" +
         s",taskAttempt=${TaskContext.get().taskAttemptId()})" +
         s" took ${elapsed} ms to compute.")

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
      objLongSubgraphAggregation.init(configuration)
      val start = System.currentTimeMillis

      /**
       * make sure this is set before the init, because subgraph callbacks
       * may access this field during initialization
       */
      subgraphAggregation = objLongSubgraphAggregation

      /**
       * init this engine
       */
      init()

      /**
       * start enumeration work asynchronously using a future
       */
      val computeFuture = Future(run())(executorContext)

      /**
       * Build a special iterator to work together with the subgraph
       * aggregation above. This iterator consumes batches of itens. A batch
       * becomes available when notified by the subgraph aggregation. The
       * subgraph aggregator then waits until this iterator notifies that the
       * last batch made available is consumed. Therefore, subgraph
       * aggregation and this iterator work in turns:
       * subgraphAggregation works -> iterator works -> subgraphAggregation
       * works -> iterator works -> ...
       */
      val keyValueIterator = new Iterator[(K, Long)] {

         private val computationFinished = new AtomicBoolean(false)
         private val finished = new AtomicBoolean(false)
         private var cursor: ObjLongCursor[K] = _
         private var hasNextCalled: Boolean = false
         private var lastHasNext: Boolean = _

         /**
          * used to indicate that the subgraph aggregation process is done
          * with the batches, this iterator may then finish
          */
         def finishIterator = computationFinished.set(true)

         def iteratorFinished() = finished.get

         private def ensureCursor(): Unit = {
            if (cursor == null) {
               //objLongSubgraphAggregation.waitForNextKeyValueMap()
               objLongSubgraphAggregation.waitWorkProduced()
               val nextKeyValueMap = objLongSubgraphAggregation
                  .consumeKeyValueMap()
               cursor = nextKeyValueMap.cursor()
            }
         }

         override def hasNext: Boolean = {
            if (hasNextCalled) return lastHasNext

            while (!computationFinished.get()) {
               ensureCursor()
               if (cursor.moveNext()) {
                  hasNextCalled = true
                  lastHasNext = true
                  return true
               } else {
                  //objLongSubgraphAggregation.notifyNextKeyValueMapConsumed()
                  objLongSubgraphAggregation.notifyWorkConsumed()
                  cursor = null
               }
            }

            ensureCursor()

            lastHasNext = cursor.moveNext()
            finished.set(!lastHasNext)

            hasNextCalled = true
            lastHasNext
         }

         override def next(): (K, Long) = {
            hasNextCalled = false
            (cursor.key(), cursor.value())
         }
      }

      /**
       * when the enumeration work finishes, we are certain that all batches
       * were already generated, we may finish the iterator and finalize this
       * engine
       */
      var callbackCalled = false
      computeFuture.onComplete { _ =>
         var shouldCallCallback = true
         synchronized {
            if (!callbackCalled) callbackCalled = true
            else shouldCallCallback = false
         }

         if (shouldCallCallback) {
            keyValueIterator.finishIterator
            //objLongSubgraphAggregation.notifyNextKeyValueMapAvailable()
            synchronized {
               while (!keyValueIterator.iteratorFinished()) {
                  //objLongSubgraphAggregation.notifyNextKeyValueMapAvailable()
                  objLongSubgraphAggregation.notifyWorkProduced()
                  wait(100)
               }
            }

            val elapsed = System.currentTimeMillis - start
            logInfo(s"SparkFromScratchEngineObjLongAggregation(step=${step}" +
               s",stageId=${stageId}" +
               s",partitionId=${partitionId}) took ${elapsed} ms to compute.")

            finalizeEngine()
         }
      }(executorContext)

      // note that this makes sense because the enumeration work is
      // dispatched asynchronously
      keyValueIterator
   }

   /**
    * This call starts this engine computation and aggregates the valid
    * subgraphs by key/value, where both keys and values are objects
    *
    * @param objObjSubgraphAggregation
    *
    * @tparam K key type parameter
    * @tparam V value type parameter
    * @return an iterator of (K,V) to be consumed downstream
    */
   override def computeAggregationObjObj[K <: Serializable, V <: Serializable]
   (objObjSubgraphAggregation: ObjObjSubgraphAggregation[S,K,V])
   : Iterator[(K, V)] = {
      objObjSubgraphAggregation.init(configuration)
      val start = System.currentTimeMillis

      // make sure this is set before the init call, because the subgraph
      // callbacks may use this reference
      subgraphAggregation = objObjSubgraphAggregation

      // init this engine
      init()

      // starts the enumeration work asynchronously
      val computeFuture = Future(run())(executorContext)

      /**
       * Build a special iterator to work together with the subgraph
       * aggregation above. This iterator consumes batches of itens. A batch
       * becomes available when notified by the subgraph aggregation. The
       * subgraph aggregator then waits until this iterator notifies that the
       * last batch made available is consumed. Therefore, subgraph
       * aggregation and this iterator work in turns:
       * subgraphAggregation works -> iterator works -> subgraphAggregation
       * works -> iterator works -> ...
       */
      val keyValueIterator = new Iterator[(K, V)] {

         private val computationFinished = new AtomicBoolean(false)
         private val finished = new AtomicBoolean(false)
         private var cursor: ObjObjCursor[K, V] = _
         private var hasNextCalled: Boolean = false
         private var lastHasNext: Boolean = _

         def finishIterator = computationFinished.set(true)

         def iteratorFinished() = finished.get

         private def ensureCursor(): Unit = {
            if (cursor == null) {
               objObjSubgraphAggregation.waitWorkProduced()
               val nextKeyValueMap = objObjSubgraphAggregation
                  .consumeKeyValueMap()
               cursor = nextKeyValueMap.cursor()
            }
         }

         override def hasNext: Boolean = {
            if (hasNextCalled) return lastHasNext

            while (!computationFinished.get()) {
               ensureCursor()
               if (cursor.moveNext()) {
                  hasNextCalled = true
                  lastHasNext = true
                  return true
               } else {
                  objObjSubgraphAggregation.notifyWorkConsumed()
                  cursor = null
               }
            }

            ensureCursor()

            lastHasNext = cursor.moveNext()
            finished.set(!lastHasNext)

            hasNextCalled = true
            lastHasNext
         }

         override def next(): (K, V) = {
            hasNextCalled = false
            (cursor.key(), cursor.value())
         }
      }

      /**
       * when the enumeration work finishes, we are certain that all batches
       * were already generated, we may finish the iterator and finalize this
       * engine
       */
      var callbackCalled = false
      computeFuture.onComplete { _ =>
         var shouldCallCallback = true
         synchronized {
            if (!callbackCalled) callbackCalled = true
            else shouldCallCallback = false
         }

         if (shouldCallCallback) {
            keyValueIterator.finishIterator
            synchronized {
               while (!keyValueIterator.iteratorFinished()) {
                  objObjSubgraphAggregation.notifyWorkProduced()
                  wait(100)
               }
            }

            val elapsed = System.currentTimeMillis - start
            logInfo(s"SparkFromScratchEngineObjObjAggregation(step=${step}" +
               s",stageId=${stageId}" +
               s",partitionId=${partitionId}) took ${elapsed} ms to compute.")

            finalizeEngine()
         }
      }(executorContext)

      // note that this makes sense because the enumeration work is
      // dispatched asynchronously
      keyValueIterator
   }

   /**
    * This call starts this engine computation and aggregates the valid
    * subgraphs by key/value, where value is a long.
    * @param longLongSubgraphAggregation
    *
    * @return iterator of (Long,Long) to be consumed downstream
    */
   override def computeAggregationLongLong
   (longLongSubgraphAggregation: LongLongSubgraphAggregation[S])
   : Iterator[(Long, Long)] = {
      longLongSubgraphAggregation.init(configuration)
      val start = System.currentTimeMillis

      /**
       * make sure this is set before the init, because subgraph callbacks
       * may access this field during initialization
       */
      subgraphAggregation = longLongSubgraphAggregation

      /**
       * init this engine
       */
      init()

      /**
       * start enumeration work asynchronously using a future
       */
      val computeFuture = Future(run())(executorContext)

      /**
       * Build a special iterator to work together with the subgraph
       * aggregation above. This iterator consumes batches of itens. A batch
       * becomes available when notified by the subgraph aggregation. The
       * subgraph aggregator then waits until this iterator notifies that the
       * last batch made available is consumed. Therefore, subgraph
       * aggregation and this iterator work in turns:
       * subgraphAggregation works -> iterator works -> subgraphAggregation
       * works -> iterator works -> ...
       */
      val keyValueIterator = new Iterator[(Long, Long)] {

         private val computationFinished = new AtomicBoolean(false)
         private val finished = new AtomicBoolean(false)
         private var cursor: LongLongCursor = _
         private var hasNextCalled: Boolean = false
         private var lastHasNext: Boolean = _

         /**
          * used to indicate that the subgraph aggregation process is done
          * with the batches, this iterator may then finish
          */
         def finishIterator = computationFinished.set(true)

         def iteratorFinished() = finished.get

         private def ensureCursor(): Unit = {
            if (cursor == null) {
               longLongSubgraphAggregation.waitWorkProduced()
               val nextKeyValueMap = longLongSubgraphAggregation
                  .getKeyValueMap()
               cursor = nextKeyValueMap.cursor()
            }
         }

         override def hasNext: Boolean = {
            if (hasNextCalled) return lastHasNext

            while (!computationFinished.get()) {
               ensureCursor()
               if (cursor.moveNext()) {
                  hasNextCalled = true
                  lastHasNext = true
                  return true
               } else {
                  longLongSubgraphAggregation.notifyWorkConsumed()
                  cursor = null
               }
            }

            ensureCursor()

            lastHasNext = cursor.moveNext()
            finished.set(!lastHasNext)

            hasNextCalled = true
            lastHasNext
         }

         override def next(): (Long, Long) = {
            hasNextCalled = false
            (cursor.key(), cursor.value())
         }
      }

      /**
       * when the enumeration work finishes, we are certain that all batches
       * were already generated, we may finish the iterator and finalize this
       * engine
       */
      var callbackCalled = false
      computeFuture.onComplete { _ =>
         var shouldCallCallback = true
         synchronized {
            if (!callbackCalled) callbackCalled = true
            else shouldCallCallback = false
         }

         if (shouldCallCallback) {
            keyValueIterator.finishIterator
            synchronized {
               while (!keyValueIterator.iteratorFinished()) {
                  longLongSubgraphAggregation.notifyWorkProduced()
                  wait(100)
               }
            }

            val elapsed = System.currentTimeMillis - start
            logInfo(s"SparkFromScratchEngineLongLongAggregation(step=${step}" +
               s",stageId=${stageId}" +
               s",partitionId=${partitionId}) took ${elapsed} ms to compute.")

            finalizeEngine()
         }
      }(executorContext)

      // note that this makes sense because the enumeration work is
      // dispatched asynchronously
      keyValueIterator
   }

   /**
    * This call starts this engine computation and aggregates the valid
    * subgraphs by key/value, where value is a long.
    * @param longObjSubgraphAggregation
    *
    * @return iterator of (Long,Long) to be consumed downstream
    */
   override def computeAggregationLongObj
   [V <: Serializable]
   (longObjSubgraphAggregation: LongObjSubgraphAggregation[S,V])
   : Iterator[(Long, V)] = {
      longObjSubgraphAggregation.init(configuration)
      val start = System.currentTimeMillis

      /**
       * make sure this is set before the init, because subgraph callbacks
       * may access this field during initialization
       */
      subgraphAggregation = longObjSubgraphAggregation

      /**
       * init this engine
       */
      init()

      /**
       * start enumeration work asynchronously using a future
       */
      val computeFuture = Future(run())(executorContext)

      /**
       * Build a special iterator to work together with the subgraph
       * aggregation above. This iterator consumes batches of itens. A batch
       * becomes available when notified by the subgraph aggregation. The
       * subgraph aggregator then waits until this iterator notifies that the
       * last batch made available is consumed. Therefore, subgraph
       * aggregation and this iterator work in turns:
       * subgraphAggregation works -> iterator works -> subgraphAggregation
       * works -> iterator works -> ...
       */
      val keyValueIterator = new Iterator[(Long, V)] {

         private val computationFinished = new AtomicBoolean(false)
         private val finished = new AtomicBoolean(false)
         private var cursor: LongObjCursor[V] = _
         private var hasNextCalled: Boolean = false
         private var lastHasNext: Boolean = _

         /**
          * used to indicate that the subgraph aggregation process is done
          * with the batches, this iterator may then finish
          */
         def finishIterator = computationFinished.set(true)

         def iteratorFinished() = finished.get

         private def ensureCursor(): Unit = {
            if (cursor == null) {
               longObjSubgraphAggregation.waitWorkProduced()
               val nextKeyValueMap = longObjSubgraphAggregation.getKeyValueMap()
               cursor = nextKeyValueMap.cursor()
            }
         }

         override def hasNext: Boolean = {
            if (hasNextCalled) return lastHasNext

            while (!computationFinished.get()) {
               ensureCursor()
               if (cursor.moveNext()) {
                  hasNextCalled = true
                  lastHasNext = true
                  return true
               } else {
                  longObjSubgraphAggregation.notifyWorkConsumed()
                  cursor = null
               }
            }

            ensureCursor()

            lastHasNext = cursor.moveNext()
            finished.set(!lastHasNext)

            hasNextCalled = true
            lastHasNext
         }

         override def next(): (Long, V) = {
            hasNextCalled = false
            (cursor.key(), cursor.value())
         }
      }

      /**
       * when the enumeration work finishes, we are certain that all batches
       * were already generated, we may finish the iterator and finalize this
       * engine
       */
      var callbackCalled = false
      computeFuture.onComplete { _ =>
         var shouldCallCallback = true
         synchronized {
            if (!callbackCalled) callbackCalled = true
            else shouldCallCallback = false
         }

         if (shouldCallCallback) {
            keyValueIterator.finishIterator
            synchronized {
               while (!keyValueIterator.iteratorFinished()) {
                  longObjSubgraphAggregation.notifyWorkProduced()
                  wait(100)
               }
            }

            val elapsed = System.currentTimeMillis - start
            logInfo(s"SparkFromScratchEngineLongLongAggregation(step=${step}" +
               s",stageId=${stageId}" +
               s",partitionId=${partitionId}) took ${elapsed} ms to compute.")

            finalizeEngine()
         }
      }(executorContext)

      // note that this makes sense because the enumeration work is
      // dispatched asynchronously
      keyValueIterator
   }

}

object SparkFromScratchEngine extends Logging {
   private val lastStageId: AtomicInteger = new AtomicInteger()

   private val activeComputations
   : IntObjMap[ObjArrayList[Computation[_]]] = HashIntObjMaps.newMutableMap()

   private def maybeUpdateLastStageId(engine: SparkEngine[_]): Unit = {
      val existing = lastStageId.get()
      if (existing < engine.stageId) {
         lastStageId.compareAndSet(existing, engine.stageId)
      }
   }

   private def cleanOldComputations(): Unit = {
      val existing = lastStageId.get()

      activeComputations.synchronized {
         val cur = activeComputations.cursor()
         while (cur.moveNext()) {
            if (cur.key() < existing - 1) {
               logInfo(s"Cleaning stage ${cur.key()}:" +
                  s" computations=${cur.value().size()}" +
                  s" activeStages=${activeComputations.size()}")
               cur.remove()
            }
         }
      }
   }


   def localComputations[S <: Subgraph](stageId: Int)
   : ObjArrayList[Computation[S]] = {
      activeComputations.get(stageId).asInstanceOf[ObjArrayList[Computation[S]]]
   }

   def createComputationsMap(engine: SparkEngine[_]): Unit = {
      val stageId = engine.stageId
      maybeUpdateLastStageId(engine)
      val computations = activeComputations.get(stageId)
      if (computations == null) {
         activeComputations.synchronized {
            val computations = activeComputations.get(stageId)
            if (computations == null) {
               activeComputations.put(
                  stageId, new ObjArrayList[Computation[_]]())
            }
         }
      }
   }

   def registerComputation(computation: Computation[_]): Unit = {
      val stageId = computation.getExecutionEngine.getStageId
      val computations = activeComputations.get(stageId)

      computations.synchronized {
         computations.add(computation)
      }
   }

   def unregisterComputation(engine: SparkEngine[_]): Unit = {
      if (engine.getPartitionId() == 0) {
         cleanOldComputations
      }
   }
}

object SparkFromScratchEngine2 extends Logging {
   private val nextIdxs
   : ConcurrentHashMap[Int, AtomicInteger] = new ConcurrentHashMap()

   private val activeComputationsIdx
   : ConcurrentHashMap[Int, ConcurrentHashMap[Int, Int]] = new
         ConcurrentHashMap()

   private val activeComputations
   : ConcurrentHashMap[Int, ObjArrayList[Computation[_]]] = new
         ConcurrentHashMap()

   private val stepToStage: IntIntMap = HashIntIntMaps.newMutableMap()

   def localComputations[S <: Subgraph](step: Int)
   : ObjArrayList[Computation[S]] = {
      activeComputations.get(step).asInstanceOf[ObjArrayList[Computation[S]]]
   }

   private def stepStageStart(step: Int, engineStageId: Int)
   : Unit = {
      var existingStageId = stepToStage.getOrDefault(step, -1)

      while (existingStageId != engineStageId) {
         if (existingStageId == -1) { // empty, just set
            stepToStage.synchronized {
               existingStageId = stepToStage.getOrDefault(step, -1)
               if (existingStageId == -1) {
                  existingStageId = engineStageId
                  stepToStage.put(step, existingStageId)
               }
            }
         } else {
            logDebug(s"StepStageConflict step=${step} " +
               s"expectedStageId=${engineStageId} " +
               s"foundStageId=${existingStageId}")
            Thread.sleep(100)
            existingStageId = stepToStage.getOrDefault(step, -1)
         }
      }

      logDebug(s"StepStageStart step=${step} " +
         s"expectedStageId=${engineStageId}")
   }

   private def stepStageFinish(step: Int, engineStageId: Int)
   : Unit = {
      val existingStageId = stepToStage.getOrDefault(step, -1)

      if (existingStageId != engineStageId) {
         throw new RuntimeException("Unexpected stage ID for step")
      }

      stepToStage.synchronized {
         stepToStage.remove(step)
      }

      logDebug(s"StepStageFinish step=${step} stageId=${engineStageId}")
   }

   def createComputationsMap(engine: SparkEngine[_]): Unit = {
      val step = engine.step
      activeComputations.synchronized {
         if (!activeComputations.containsKey(step)) {
            logInfo(s"Registering computation map step=${step}")

            //val newArr = new Array[Computation[_]](numComputations)
            val newArr = new ObjArrayList[Computation[_]]()
            activeComputations.put(step, newArr)
            nextIdxs.put(step, new AtomicInteger(0))
            activeComputationsIdx.put(step, new ConcurrentHashMap())

            stepStageStart(step, engine.stageId)
         }
         //else {
         //   val _numComputations = activeComputations.get(step).size()

         //   if (_numComputations != numComputations) {
         //      throw new RuntimeException(
         //         s"NumberOfComputations current: ${_numComputations}" +
         //            s", expected: ${numComputations}")
         //   }
         //}
      }
   }

   def registerComputation(computation: Computation[_]): Unit = {
      val step = computation.getStep
      val partitionId = computation.getPartitionId
      val computations = activeComputations.get(step)
      val computationIdx = nextIdxs.get(step).getAndIncrement()
      val minNumComputations = computationIdx + 1
      activeComputationsIdx.get(step).put(partitionId, computationIdx)

      computations.synchronized {
         while (computations.size() < minNumComputations) computations.add(null)
      }

      computations.set(computationIdx, computation)

      logInfo(s"Registered computation step=${step} " +
         s"partitionId=${partitionId}" +
         //s" computations=${computations.filter(_ != null).size}" +
         s" computationsIdx=${activeComputationsIdx.get(step)}" +
         s" computations=${computations}" +
         s" nextIdxs=${nextIdxs.get(step)}")
   }

   def unregisterComputation(engine: SparkEngine[_]): Unit = {
      val step = engine.step
      val partitionId = engine.getStep()
      if (nextIdxs.get(step).decrementAndGet() == 0) {
         logInfo(s"Unregistering last computation step=${step} " +
            s"partitionId=${partitionId}")
         activeComputations.remove(step)
         nextIdxs.remove(step)
         activeComputationsIdx.remove(step)
         stepStageFinish(step, engine.stageId)
      } else {
         logInfo(s"Unregistering computation step=${step} " +
            s"partitionId=${partitionId}")
         val computations = activeComputations.get(step)
         val computationStep = activeComputationsIdx.get(step)
         if (computationStep != null) {
            val computationIdx = computationStep.get(partitionId)
            computations.set(computationIdx, null)
         }
      }
   }
}
