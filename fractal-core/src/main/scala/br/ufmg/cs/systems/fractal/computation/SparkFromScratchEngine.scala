package br.ufmg.cs.systems.fractal.computation

import java.io.Serializable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic._

import akka.actor._
import br.ufmg.cs.systems.fractal.aggregation._
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.Logging
import com.koloboke.collect.map.{ObjLongCursor, ObjObjCursor}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 */
case class SparkFromScratchEngine[S <: Subgraph](
                                                   partitionId: Int,
                                                   step: Int,
                                                   accums: Map[String,LongAccumulator],
                                                   validSubgraphsAccum: LongAccumulator,
                                                   previousAggregationsBc: Broadcast[_],
                                                   configurationId: Int) extends SparkEngine[S] {

   @transient var slaveActorRef: ActorRef = _

   private var subgraphAggregation: SubgraphAggregation[S] = _

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
      slaveActorRef ! Terminate
      slaveActorRef = null
      // make sure we close writers
      if (outputStreamOpt.isDefined) outputStreamOpt.get.close
      if (subgraphWriterOpt.isDefined) subgraphWriterOpt.get.close
   }

   def compute(): Unit = {
      val start = System.currentTimeMillis
      val subgraph: S = configuration.createSubgraph()
      computation.getSubgraphEnumerator.set(computation, subgraph, null)
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

   override def getSubgraphAggregation() = subgraphAggregation

   private def run(): Unit = {
      val subgraph: S = configuration.createSubgraph()
      computation.getSubgraphEnumerator.set(computation, subgraph, null)
      computation.compute(subgraph)
   }

   /**
    * This call starts the step computation of this engine and aggregates the
    * valid subgraphs into a single long number
    * @param _defaultValue default aggregation value
    * @param _value mapping function to obtain a long from a subgraph
    * @param _reduce reducing function to combine values
    * @return a single long
    */
   override def computeAggregationLong
   (_defaultValue: Long, _value: S => Long, _reduce: (Long,Long) => Long)
   : Long = {
      val start = System.currentTimeMillis

      // build the subgraph aggregation for longs
      val longSubgraphAggregation = new LongSubgraphAggregation[S](_defaultValue) {
         override def value(subgraph: S): Long = _value(subgraph)
         override def reduce(v1: Long, v2: Long): Long = _reduce(v1, v2)
      }

      // make sure this is set before init, because callbacks may use this
      // reference
      subgraphAggregation = longSubgraphAggregation

      // init this engine, do enumeration work and finalize this engine
      init()
      run()
      finalize()

      val elapsed = System.currentTimeMillis - start
      logInfo(s"SparkFromScratchEngineLongAggregation(step=${step}" +
         s",partitionId=${partitionId} took ${elapsed} ms to compute.")

      longSubgraphAggregation.value()
   }

   /**
    * This call starts this engine computation and aggregates the valid
    * subgraphs by key/value, where value is a long.
    * @param _key mapping function to obtain the key from a subgraph
    * @param _defaultValue default aggregation value
    * @param _value mapping function to obtain the value from a subgraph
    * @param _reduce reducing function to combine values
    * @tparam K key type parameter
    * @return iterator of (K,Long) to be consumed downstream
    */
   override def computeAggregationObjLong[K <: Serializable]
   (_key: S => K, _defaultValue: Long, _value: S => Long,
    _reduce: (Long,Long) => Long)
   : java.util.Iterator[(K,Long)] = {
      val start = System.currentTimeMillis

      /**
       * build subgraph aggregation with the user provided functions
       */
      val objLongSubgraphAggregation = new ObjLongSubgraphAggregation[S,K](_defaultValue) {
         override def key(subgraph: S): K = _key(subgraph)
         override def value(subgraph: S): Long = _value(subgraph)
         override def reduce(v1: Long, v2: Long): Long = _reduce(v1, v2)
      }

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
      val computeFuture = Future(run())

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
      val keyValueIterator = new java.util.Iterator[(K,Long)] {

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
               objLongSubgraphAggregation.waitForNextKeyValueMap()
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
                  objLongSubgraphAggregation.notifyNextKeyValueMapConsumed()
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
      computeFuture.onComplete { _ =>
         flushStatsAccumulators
         keyValueIterator.finishIterator
         synchronized {
            while (!keyValueIterator.iteratorFinished()) {
               objLongSubgraphAggregation.notifyNextKeyValueMapAvailable()
               wait(100)
            }
         }

         val elapsed = System.currentTimeMillis - start
         logInfo(s"SparkFromScratchEngine(step=${step}" +
            s",partitionId=${partitionId} took ${elapsed} ms to compute.")

         finalize()
      }

      // note that this makes sense because the enumeration work is
      // dispatched asynchronously
      keyValueIterator
   }

   /**
    * This call starts this engine computation and aggregates the valid
    * subgraphs by key/value, where both keys and values are objects
    * @param _key mapping function to obtain the key from a subgraph
    * @param _value mapping function to obtain the value from a subgraph
    * @param _aggregate function that combines the second parameter value into
    *                   the first parameter value
    * @tparam K key type parameter
    * @tparam V value type parameter
    * @return an iterator of (K,V) to be consumed downstream
    */
   override def computeAggregationObjObj[K <: Serializable, V <: Serializable]
   (_key: S => K, _value: S => V, _aggregate: (V,V) => Unit)
   : java.util.Iterator[(K,V)] = {
      val start = System.currentTimeMillis

      // build a subgraph aggregation where keys and values are objects,
      // using the provided user functions
      val objObjSubgraphAggregation = new ObjObjSubgraphAggregation[S,K,V] {
         override def key(subgraph: S): K = _key(subgraph)
         override def value(subgraph: S): V = _value(subgraph)
         override def aggregate(existingValue: V, otherValue: V): Unit =
            _aggregate(existingValue, otherValue)
      }

      // make sure this is set before the init call, because the subgraph
      // callbacks may use this reference
      subgraphAggregation = objObjSubgraphAggregation

      // init this engine
      init()

      // starts the enumeration work asynchronously
      val computeFuture = Future(run())

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
      val keyValueIterator = new java.util.Iterator[(K,V)] {

         private val computationFinished = new AtomicBoolean(false)
         private val finished = new AtomicBoolean(false)
         private var cursor: ObjObjCursor[K,V] = _
         private var hasNextCalled: Boolean = false
         private var lastHasNext: Boolean = _

         def finishIterator = computationFinished.set(true)

         def iteratorFinished() = finished.get

         private def ensureCursor(): Unit = {
            if (cursor == null) {
               objObjSubgraphAggregation.waitForNextKeyValueMap()
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
                  objObjSubgraphAggregation.notifyNextKeyValueMapConsumed()
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
      computeFuture.onComplete { _ =>
         flushStatsAccumulators
         keyValueIterator.finishIterator
         synchronized {
            while (!keyValueIterator.iteratorFinished()) {
               objObjSubgraphAggregation.notifyNextKeyValueMapAvailable()
               wait(100)
            }
         }

         val elapsed = System.currentTimeMillis - start
         logInfo(s"SparkFromScratchEngine(step=${step}" +
            s",partitionId=${partitionId} took ${elapsed} ms to compute.")

         finalize()
      }

      // note that this makes sense because the enumeration work is
      // dispatched asynchronously
      keyValueIterator
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
         val computationStep = activeComputationsIdx.get(step)
         if (computationStep != null) {
            val computationIdx = computationStep.get(partitionId)
            computations(computationIdx) = null
         }
      }
   }
}
