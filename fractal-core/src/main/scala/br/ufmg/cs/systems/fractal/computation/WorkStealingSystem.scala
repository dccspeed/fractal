package br.ufmg.cs.systems.fractal.computation

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor._
import br.ufmg.cs.systems.fractal.subgraph.Subgraph
import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.collection.{IntArrayList, ObjArrayList}
import br.ufmg.cs.systems.fractal.util.pool.HashIntSetPool

class WorkStealingSystem [S <: Subgraph]
(processCompute: (SubgraphEnumerator[S],Computation[S]) => Long,
 slaveActor: ActorRef,
 remoteWorkQueue: ConcurrentLinkedQueue[StealWorkResponse]) extends Logging {

   // indicates whether this thread is allowed to keep sending new remote
   // requests or work -- this is flagged out uppon receiving an empty
   // response that considered every worker
   private var newExternalRequestsAllowed = true

   // number of requests sent without responses processed (available locally
   // or not)
   private var requestsOnTheFly = 0L

   // upper bound on the number of on the fly requests without responses
   private val maxRequestsOnTheFly = 1 // TODO: include as config

   // timeout used for waiting for external work stealing responses
   private val workQueueTimeoutMs = 500

   // just to measure how far from the upper bound on onTheFly requests we are
   private var requestsOnTheFlyMax = Long.MinValue

   // used for visiting local JVM computations in a breath-first traversal
   private val unvisitedComputations: ObjArrayList[Computation[S]] =
      new ObjArrayList[Computation[S]]()

   private def remoteWorkQueueIsEmpty: Boolean = {
      requestsOnTheFly <= 0 || remoteWorkQueue.isEmpty
   }

   private def consumeExternalWork(c: Computation[S]): Long = {
      val numPartitions = c.getNumberPartitions
      var externalSteals = 0L
      var response = remoteWorkQueue.poll()

      while (response != null) {
         requestsOnTheFly -= 1
         val workUnit = response.workUnit
         if (workUnit != null) {
            val consumer = deserializeSubgraphBatch(workUnit, c)
            val computation = consumer.getComputation()
            val ret = processCompute(consumer, computation)
            externalSteals += 1

         } else if (response.numPeers == numPartitions) {
            newExternalRequestsAllowed = false
         }

         response = remoteWorkQueue.poll()
      }

      externalSteals
   }

   def workStealingCompute(c: Computation[S]): Unit = {
      val internalWsEnabled = c.getConfig().internalWsEnabled()
      val externalWsEnabled = c.getConfig().externalWsEnabled()
      val numPartitions = c.getNumberPartitions
      var internalSteals = 0L
      var externalSteals = 0L
      var wsIterations = 0L

      if (externalWsEnabled) {
         slaveActor ! WorkQueue(remoteWorkQueue)
      }

      // case 1: internal work stealing only
      if (internalWsEnabled && !externalWsEnabled) {
         internalSteals += workStealingComputeLocal(c)
         wsIterations += 1
      }

      // case 2: internal and external work stealing
      else if (internalWsEnabled && externalWsEnabled) {

         // step 1: internal and external work stealing allowed
         var wsIterationsStep1 = 0L
         while (newExternalRequestsAllowed) {
            // internal work stealing while still possible
            internalSteals += workStealingComputeLocal(c)

            // maybe send external work stealing request
            if (requestsOnTheFly < maxRequestsOnTheFly) {
               slaveActor ! StealWork
               requestsOnTheFly += 1
               requestsOnTheFlyMax = requestsOnTheFlyMax.max(requestsOnTheFly)
            }

            // check response and external work stealing
            externalSteals += consumeExternalWork(c)
            if (requestsOnTheFly > 0) {
               // wait for response (with timeout)
               remoteWorkQueue.synchronized {
                  remoteWorkQueue.wait(workQueueTimeoutMs)
               }
            }

            wsIterationsStep1 += 1
         }

         // step 2: ensure all sent requests got responses
         var wsIterationsStep2 = 0L
         externalSteals += consumeExternalWork(c)
         while (requestsOnTheFly > 0) {
            remoteWorkQueue.synchronized {
               remoteWorkQueue.wait(workQueueTimeoutMs)
            }
            externalSteals += consumeExternalWork(c)
            wsIterationsStep2 += 1
         }

         // step 3: drain any rejected response that may exist in this thread
         // obs. too many rejected work is a sign of network delay/failure
         var wsIterationsStep3 = 0L
         slaveActor ! DrainRejectedWork
         requestsOnTheFly += 1
         while (requestsOnTheFly > 0) {
            externalSteals += consumeExternalWork(c)
            wsIterationsStep3 += 1
         }
         while (!remoteWorkQueue.isEmpty) {
            externalSteals += consumeExternalWork(c)
            wsIterationsStep3 += 1
         }

         // step 4: only internal work stealing allowed (final)
         internalSteals += workStealingComputeLocal(c)

         wsIterations += wsIterationsStep1 + wsIterationsStep2 + wsIterationsStep3

         logInfo(s"FinishingExecutor" +
            s" step=${c.getStep}" +
            s" partitionId=${c.getPartitionId}" +
            s" internalSteals=${internalSteals}" +
            s" externalSteals=${externalSteals}" +
            s" requestsOnTheFly=${requestsOnTheFly}" +
            s" requestsOnTheFlyMax=${requestsOnTheFlyMax}" +
            s" wsIterations=${wsIterations}" +
            s" wsIterationsStep1=${wsIterationsStep1}" +
            s" wsIterationsStep2=${wsIterationsStep2}" +
            s" wsIterationsStep3=${wsIterationsStep3}" +
            s" maxRequestsOnTheFly=${maxRequestsOnTheFly}" +
            s" numPartitionsTotal=${numPartitions}")
      }

      // case 3: only external work stealing allowed
      else if (!internalWsEnabled && externalWsEnabled) {
         throw new UnsupportedOperationException
      }

      // case 4: neither internal nor external work stealing allowed
      else {
         // do nothing
      }

   }

   private def workStealingComputeLocal(c: Computation[S]): Long = {
      var internalSteals = 0L
      var lastInternalSteals = 0L
      var continue = remoteWorkQueueIsEmpty

      while (continue) {
         val computations = SparkFromScratchEngine.localComputations[S](
            c.getExecutionEngine.getStageId)
         lastInternalSteals = workStealingComputeLocalIter(c, computations)
         internalSteals += lastInternalSteals
         continue = lastInternalSteals > 0 && remoteWorkQueueIsEmpty
      }

      internalSteals
   }

   private def workStealingComputeLocalIter
   (c: Computation[S], computations: ObjArrayList[Computation[S]]): Long = {
      var thisComp = c
      var thisSubgraphEnumerator = thisComp.getSubgraphEnumerator
      var continue = remoteWorkQueueIsEmpty
      val numComputations = computations.size()
      var internalSteals = 0L
      unvisitedComputations.clear()

      // first level: root computations (fill visited array)
      var i = 0
      while (continue && i < numComputations) {
         val thatComp = computations.getu(i)
         if (thatComp != null) {
            val thatSubgraphEnumerator = thatComp.getSubgraphEnumerator
            if (thatSubgraphEnumerator.forkEnumerator(thisComp)) {
               processCompute(thisSubgraphEnumerator, thisComp)
               internalSteals += 1
            }

            val nextCompNext = thatComp.nextComputation()
            if (nextCompNext != null) {
               unvisitedComputations.add(thatComp.nextComputation())
            }

         }

         i += 1
         continue = continue && remoteWorkQueueIsEmpty
      }

      // remaining levels
      i = 0
      var numUnvisitedComputations = unvisitedComputations.size()
      continue = continue && i < numUnvisitedComputations &&
         remoteWorkQueueIsEmpty
      while (continue) {
         thisComp = thisComp.nextComputation()
         thisSubgraphEnumerator = thisComp.getSubgraphEnumerator
         while (continue && i < numUnvisitedComputations) {
            val thatComp = unvisitedComputations.getu(i)
            val thatSubgraphEnumerator = thatComp.getSubgraphEnumerator
            if (thatSubgraphEnumerator.forkEnumerator(thisComp)) {
               processCompute(thisSubgraphEnumerator, thisComp)
               internalSteals += 1
            }

            val nextCompNext = thatComp.nextComputation()
            if (nextCompNext != null) {
               unvisitedComputations.add(nextCompNext)
            }

            i += 1
            continue = continue && remoteWorkQueueIsEmpty
         }

         numUnvisitedComputations = unvisitedComputations.size()

         continue = continue && i < numUnvisitedComputations &&
            remoteWorkQueueIsEmpty
      }

      internalSteals
   }

   private def deserializeSubgraphBatch
   (workUnit: IntArrayList, c: Computation[S]): SubgraphEnumerator[S] = {

      var i = 0
      val depth = workUnit.getu(i)
      i += 1

      // find computation given depth
      var currComp = c
      while (currComp.getDepth < depth) currComp = currComp.nextComputation()

      // fill subgraph according to prefix
      val subgraphEnum = currComp.getSubgraphEnumerator
      val subgraph = subgraphEnum.getSubgraph
      subgraph.reset()

      val prefixSize = workUnit.getu(i)
      i += 1

      // add prefix into subgraph
      var j = 0
      while (j < prefixSize) {
         subgraph.addWord(workUnit.getu(i))
         j += 1
         i += 1
      }

      // set subgraph enumerator and rebuild its state
      subgraphEnum.newExtensions(workUnit.view(i, workUnit.size()))
      subgraphEnum.rebuildState()

      subgraphEnum
   }
}
