package br.ufmg.cs.systems.fractal.computation

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Arrays, Comparator, Properties}

import akka.actor._
import akka.routing._
import br.ufmg.cs.systems.fractal.FractalContext
import br.ufmg.cs.systems.fractal.conf.Configuration
import br.ufmg.cs.systems.fractal.subgraph.Subgraph
import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.collection.{IntArrayList, ObjArrayList}
import com.koloboke.collect.map.LongObjMap
import com.koloboke.collect.map.hash.HashLongObjMaps
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable.Map
import scala.concurrent.duration._

sealed trait SeqNum {
   def seqNum: Long
}

/**
 * Message sent to confirm a reponse sent (idempotent)
 */
case class ResponseAck(seqNum: Long, rejected: Boolean) extends SeqNum

/**
 * Retry message used to re-send a message periodically
 */
case class Retry(msg: SeqNum, dest: ActorRef, delayMs: Int)

/**
 * Message broadcasted by worker threads to indicate that the termination
 * barrier has been reached
 */
case class TerminationBarrier(seqNum: Long, partitionId: Int) extends SeqNum

/**
 * Message used to confirm that the termination barrier sent has been received
 */
case class TerminationBarrierAck(seqNum: Long) extends SeqNum

/**
 * Message sent by slaves to the master for registering
 */
case class HelloMaster(partitionId: Int, slaveRef: ActorRef)

/**
 * Message sent by slaves to the master indicating the end of a step
 */
case class ByeMaster(partitionId: Int, slaveRef: ActorRef)

/**
 * Message sent by the master to a slave to inform the references of all other
 * slaves
 */
case class HelloSlave(slaveRefs: Array[ActorRef])

/**
 * Message sent to the local gtag actor to start a remote work-stealing event
 */
case object StealWork

/**
 * Message sent to indicate that the worker thread must drain any rejected
 * work (sent to requester but not confirmed) to the local work queue to be
 * consumed locally.
 */
case object DrainRejectedWork

/**
 * Message sent by a slave to other slaves to indicate a ready state for
 * termination
 */
case class ReadyToFinish(partitionId: Int, knowsEveryoneIsReadyToFinish: Boolean)

/**
 * This message is sent externally to the slave, providing a shared queue for
 * consuming work. In this case, the response is the work to be consumed.
 */
case class WorkQueue(q: ConcurrentLinkedQueue[StealWorkResponse])

/**
 * Message passed between the actors participating in a work-stealing event.
 * This message is forwarded until some work could be stealed.
 */
case class StealWorkRequest(thief: ActorRef, nextPivot: Int, lastPivot: Int,
                            seqNum: Long) extends SeqNum

/**
 * Response message to the sender of a 'StealWork'. If available, the work will
 * be serialized as an array of bytes.
 */
case class StealWorkResponse(workUnit: IntArrayList, numPeers: Int,
                             reqSeqNum: Long, seqNum: Long) extends SeqNum

/**
 * Self-sent periodically by slave actors to report stats to the master
 */
case object ReportStats

/**
 * Stats message
 */
case class Stats(partitionId: Int, validSubgraphs: Long, maxMemory: Double,
                 totalMemory: Double, freeMemory: Double, usedMemory: Double)

/**
 * Termination message
 */
case object Terminate

/**
 * Message used to terminate early any fractal job associated with a context
 */
case class TerminateFractalContext(fc: FractalContext, mastersRouter: Router)

/**
 * Stop message
 */
case object Stop

/**
 * Step actor
 */
abstract class MSActor(masterPath: String) extends Actor with Logging {
   /**
    * Akka reference of the master. Master here is used as a rendevouz point to
    * spread actor references between slaves.
    */
   def masterRef: ActorRef

   logInfo(s"Actor ${self} started")
}

object MasterActor {
   class Args[S <: Subgraph](_masterPath: String,
                             _engine: SparkMasterEngine[S]) {
      private var masterPath: String = _masterPath
      private var engine: SparkMasterEngine[S] = _engine

      def getMasterPath: String = masterPath
      def getEngine: SparkMasterEngine[S] = engine

      def clear(): Unit = {
         masterPath = null
         engine = null
      }
   }
}

/**
 * Master actor
 */
class MasterActor(args: MasterActor.Args[_])
   extends MSActor(args.getMasterPath) {

   private val masterPath: String = args.getMasterPath

   private val engine: SparkMasterEngine[_] = args.getEngine

   override def masterRef: ActorRef = self

   private val numSlaves = engine.numPartitions

   /**
    * These attributed are reset after each step
    */
   private var slaves: Array[ActorRef] = _
   private var validSubgraphs: Array[Long] = _
   private var slavesRouter: Router = _
   private var registeredSlaves: Int = _
   private var totalValidSubgraphs: Long = _
   private var maxValidSubgraphs: Long = _
   private var askedForTermination: Boolean = false

   // clear args
   args.clear()

   // initial reset
   reset()

   private def reset(): Unit = {
      logInfo(s"ResetingMaster: ${self}")
      if (slavesRouter != null) {
         logInfo(s"SendingSlavePoisonPills ${slavesRouter}")
         slavesRouter.route(PoisonPill, self)
      }

      slavesRouter = new Router(new BroadcastRoutingLogic())
      slaves = new Array[ActorRef](numSlaves)
      validSubgraphs = new Array[Long](numSlaves)
      registeredSlaves = 0
      totalValidSubgraphs = 0
      maxValidSubgraphs = engine.config.getLong(
         "max_valid_subgraphs", Long.MaxValue)
   }

   def receive = {
      case HelloMaster(partitionId, slaveRef) if askedForTermination =>
         slaveRef ! Terminate
         logInfo(s"Already asked for termination. Terminating ${slaveRef}")

      case HelloMaster(partitionId, slaveRef) =>
         if (slaves(partitionId) == null) {
            registeredSlaves += 1
            slavesRouter = slavesRouter.addRoutee(ActorRefRoutee(slaveRef))
         }

         logInfo(s"${self} knows ${registeredSlaves} slaves.")

         slaves(partitionId) = slaveRef

         if (registeredSlaves == numSlaves) {
            var publishSlaves = true

            if (publishSlaves) {
               val helloMsg = HelloSlave(slaves)
               slavesRouter.route(helloMsg, self)
               logInfo(s"Publishing ${numSlaves} slaves.")
            }
         }

      case ByeMaster(partitionId, slaveRef) =>
         if (slaves(partitionId) != null) {
            registeredSlaves -= 1
         }

         logInfo(s"ByeMaster: ${self} knows ${registeredSlaves} slaves.")

         slaves(partitionId) = null

         if (registeredSlaves == 0) {
            reset()
         }

      case Stats(partitionId, _validSubgraphs, maxMemory, totalMemory,
      freeMemory, usedMemory) =>

         logInfo(s"MemoryStats step=${engine.step} threadId=${partitionId}" +
            s" maxMemory=${maxMemory} totalMemory=${totalMemory}" +
            s" freeMemory=${freeMemory} usedMemory=${usedMemory}" +
            s" validSubgraphs=${_validSubgraphs}")

         // increment in the number of valid subgraphs
         val diff = _validSubgraphs - validSubgraphs(partitionId)

         // update valid subgraphs for slave
         validSubgraphs(partitionId) = Math.max(
            validSubgraphs(partitionId), _validSubgraphs)

         // account for increment
         if (diff > 0) {
            totalValidSubgraphs += diff
         }

         logInfo(s"ValidSubgraphsUpdate step=${engine.step}" +
            s" [${validSubgraphs.mkString(",")}]" +
            s" total=${totalValidSubgraphs}" +
            (
               if (maxValidSubgraphs < Long.MaxValue)
                  s"/${maxValidSubgraphs} total(%)=${
                     (totalValidSubgraphs / maxValidSubgraphs.toDouble) * 100
                  }/100"
               else
                  ""
               )
         )

         // if we reach *maxValidSubgraphs* then stop execution, we are done
         if (totalValidSubgraphs >= maxValidSubgraphs) {
            logWarn(
               s"Reached the limit of ${maxValidSubgraphs} valid subgraphs." +
                  s" Terminating slaves=${slaves.mkString(",")}")

            slavesRouter.route(Terminate, self)
         }

      case Terminate =>
         askedForTermination = true
         slavesRouter.route(Terminate, self)
         import context.dispatcher
         context.system.scheduler.scheduleOnce(2 seconds, self, Terminate)

      case other =>
         logWarn(s"${self} ignoring message: ${other}.")
   }
}

object SlaveActor {
   class Args[S <: Subgraph](_masterPath: String, _engine: SparkEngine[S]) {
      private var masterPath: String = _masterPath
      private var engine: SparkEngine[S] = _engine

      def getMasterPath: String = masterPath
      def getEngine: SparkEngine[S] = engine

      def clear(): Unit = {
         masterPath = null
         engine = null
      }
   }
}

/**
 * Slave actor
 */
class SlaveActor[S <: Subgraph](args: SlaveActor.Args[S])
   extends MSActor(args.getMasterPath) {

   private val engine: SparkEngine[S] = args.getEngine

   private val masterPath: String = args.getMasterPath

   private val computation: Computation[S] = engine.computation

   private val threadSafeComputation: Computation[S] = engine.computationCopy

   private val partitionId: Int = engine.getPartitionId()

   private val config: Configuration = computation.getConfig()

   private var _masterRef: ActorRef = _

   override def masterRef: ActorRef = _masterRef

   private var slaves: Array[ActorRef] = _

   private var pivots: Array[ActorRef] = _

   private var localPivotIdx: Int = -1

   private var outbox: ConcurrentLinkedQueue[StealWorkResponse] = _

   private var slavesRouter: Router = new Router(new BroadcastRoutingLogic())

   private var readyToFinishCount: Int = 0

   private var numLocalPeers: Int = _

   private val unackMessages: LongObjMap[(Retry, Cancellable)] =
      HashLongObjMaps.newMutableMap()

   private val rejectedResponses: ObjArrayList[StealWorkResponse] =
      new ObjArrayList[StealWorkResponse]()

   private var lastSeqNum: Long = -1

   private def nextSeqNum: Long = {
      lastSeqNum += 1
      lastSeqNum
   }

   private def newSlaveReadyToFinish: Unit = {
      readyToFinishCount += 1
   }

   private def getReadyToFinishCount: Int = {
      maybeCheckReadyToFinishSlaves()
      readyToFinishCount
   }

   private var readyToFinishSlaves: Array[Boolean] = _

   private val emptyResponseReusable = StealWorkResponse(null, 0, -1, -1)

   private val infoPeriod = computation.getConfig().getInfoPeriod()

   private val unvisitedComputations: ObjArrayList[Computation[S]] =
      new ObjArrayList[Computation[S]]()

   private val workUnit: IntArrayList = new IntArrayList()

   private var reachedTerminationBarrierSlaves: Array[Boolean] = _

   private var reachedTerminationBarrierCount: Int = 0

   private var forcedTermination: Boolean = false

   // clear args
   args.clear()

   // register with master
   sendIdentifyRequest()

   // report execution stats from time to time
   reportStatsScheduler()

   def receive: Actor.Receive = identifying

   def identifying: Actor.Receive = {
      case ActorIdentity(`masterPath`, Some(actor)) =>
         _masterRef = actor
         context.become(active(actor))
         masterRef ! HelloMaster(partitionId, self)
         reportStats()
         logInfo(s"${self} knows master: ${masterRef}")

      case Terminate | ActorIdentity(`masterPath`, None) =>
         context.become(terminating(null))
         self ! Terminate

      case Stop =>
         context.become(terminating(null))
         self ! Stop

      case WorkQueue(_outbox) =>
         outbox = _outbox

      case StealWork =>
         addToOutbox(emptyResponseReusable)

      case ReceiveTimeout =>
         sendIdentifyRequest()

      case other =>
         logWarn(s"Identifying: ${self} is not ready. Ignoring " +
            s"message ${other} from ${sender()}.")
   }

   def active(actor: ActorRef): Actor.Receive = {
      case HelloSlave(_slaves) =>
         slaves = _slaves
         if (slaves.length > 0) {
            val pivotMap: Map[Address, ActorRef] = Map.empty
            var i = 0
            numLocalPeers = 0
            while (i < slaves.length) {
               val ref = slaves(i)
               slavesRouter = slavesRouter.addRoutee(ActorRefRoutee(ref))
               val refAddr = ref.path.address
               pivotMap.update(refAddr, ref)
               if (refAddr.equals(self.path.address)) {
                  numLocalPeers += 1
               }
               i += 1
            }

            pivots = pivotMap.values.toArray
            Arrays.sort(pivots, ActorMessageSystem.actorRefComparator)

            // get local pivot index
            var j = 0
            while (localPivotIdx < 0 && j < pivots.length) {
               if (pivots(j).path.address.equals(self.path.address)) {
                  localPivotIdx = j
               }
               j += 1
            }

            readyToFinishSlaves = new Array[Boolean](slaves.length)
            reachedTerminationBarrierSlaves = new Array[Boolean](slaves.length)

            logInfo(s"${self} knows ${slaves.length} slaves" +
               s" (${pivots.length} pivots)" +
               s" (${numLocalPeers} local)" +
               s" (pivots=${pivots.mkString(",")})")
            self.synchronized {
               self.notify()
            }
         }

      case WorkQueue(_outbox) =>
         outbox = _outbox

      case ReadyToFinish(slaveId, knowsEveryoneIsReadyToFinish) =>
         if (readyToFinishSlaves != null) {
            if (!readyToFinishSlaves(slaveId)) {
               readyToFinishSlaves(slaveId) = true
               newSlaveReadyToFinish
            }
            if (!knowsEveryoneIsReadyToFinish) {
               sender() ! ReadyToFinish(partitionId,
                  readyToFinishCount == slaves.length)
            }
         }

      case DrainRejectedWork =>
         if (unackMessages.isEmpty) {
            if (rejectedResponses.isEmpty) {
               addToOutbox(emptyResponseReusable)
            } else {
               addAllToOutbox(rejectedResponses)
               rejectedResponses.clear()
            }

            // start termination phase
            context.become(terminating(actor))

            // tell every other thread that this thread has reached the
            // termination barrier
            var i = 0
            while (i < slaves.length) {
               val terminationMsg = TerminationBarrier(nextSeqNum, partitionId)
               sendMsgWithRetransmission(terminationMsg, slaves(i), 100)
               i += 1
            }

            outbox.synchronized {
               outbox.notify()
            }

         } else { // wait for all rejected work
            logInfo(s"Could not drain rejected work, there are unacked " +
               s"messages. ${unackMessages}")
            // schedule for later
            import context.dispatcher
            context.system.scheduler.scheduleOnce(
               100 millis, self, DrainRejectedWork)
         }

      case ReportStats =>
         reportStats()

      /**
       * Work stealing {
       */

      // worker actors not yet known, reply with empty response
      case StealWork if slaves == null =>
         addToOutbox(emptyResponseReusable)

      // start the request tour among worker pivots
      case StealWork =>
         val numPivots = pivots.length
         val nextPivot = (localPivotIdx + 1) % numPivots
         val lastPivot = (localPivotIdx + numPivots - 1) % numPivots

         // send this request with retransmission
         val msg = StealWorkRequest(self, nextPivot, lastPivot, nextSeqNum)
         val aRef = pivots(nextPivot)
         sendRequestWithTimeout(msg, aRef)

         logDebug(s"${StealWork}: ${self} sending ${msg} to ${aRef}")

      // worker actors not yet known, reply with empty response
      case req: StealWorkRequest if slaves == null =>
         val msg = StealWorkResponse(null, 0, req.seqNum, nextSeqNum)
         req.thief ! msg

      // the request reached the end of the tour among pivots and there is
      // rejected responses that can be used as response
      case req: StealWorkRequest
         if slaves != null &&
            req.nextPivot == req.lastPivot &&
            !rejectedResponses.isEmpty =>

         val msg = rejectedResponses.getLast().copy(reqSeqNum = req.seqNum)
         rejectedResponses.removeLast()
         sendMsgWithRetransmission(msg, req.thief)

      // the request reached the end of the tour among pivots and there is
      // not rejected responses, try to steal local work
      case req: StealWorkRequest
         if slaves != null && req.nextPivot == req.lastPivot &&
            rejectedResponses.isEmpty =>

         val workStealed = tryStealFromLocal(workUnit)
         val readyToFinish = workStealed == 0 &&
            getReadyToFinishCount == slaves.length

         if (readyToFinish) { // indicate termination
            val msg = StealWorkResponse(null, slaves.length,
               req.seqNum, nextSeqNum)
            req.thief ! msg

            logDebug(s"${req}: ${self} sending ${msg} to" +
               s" ${req.thief}")

         } else if (workStealed > 0) {
            val msg = StealWorkResponse(new IntArrayList(workUnit), slaves
               .length, req.seqNum, nextSeqNum)
            sendMsgWithRetransmission(msg, req.thief)

            logDebug(s"${req}: ${self} sending ${msg} to" +
               s" ${req.thief}")
         } else {
            val msg = StealWorkResponse(null, 0, req.seqNum, nextSeqNum)
            req.thief ! msg
         }

      // the request has not yet reached the end of the tour among pivots and
      // there is rejected response to be used
      case req: StealWorkRequest
         if slaves != null && req.nextPivot != req.lastPivot &&
            !rejectedResponses.isEmpty =>

         val msg = rejectedResponses.getLast.copy(
            reqSeqNum = req.seqNum
         )
         rejectedResponses.removeLast()

         // send response with retransmission
         sendMsgWithRetransmission(msg, req.thief)

      // the request has not yet reached the end of the tour among pivots and
      // there is no rejected response to be used, try stealing local work
      case req: StealWorkRequest
         if slaves != null && req.nextPivot != req.lastPivot &&
            rejectedResponses.isEmpty =>

         val workStealed = tryStealFromLocal(workUnit)

         if (workStealed > 0) { // work stealed, send produced resp.
            val msg = StealWorkResponse(new IntArrayList(workUnit),
               slaves.length, req.seqNum, nextSeqNum)

            // send response with retransmission
            sendMsgWithRetransmission(msg, req.thief)

            logDebug(s"${req}: ${self} sending ${msg} to" +
               s"${req.thief}")

         } else { // work not stealed, pass request along

            val numPivots = pivots.length
            val nextPivot = (req.nextPivot + 1) % numPivots
            val msg = StealWorkRequest(req.thief, nextPivot,
               req.lastPivot, req.seqNum)
            val p = pivots(nextPivot)

            // send request without retransmission (best-effort)
            p ! msg

            logDebug(s"${req}: ${self} forwarded ${msg} to ${p}")
         }

      // receive, add to outbox, and acknowledge incoming response
      case resp: StealWorkResponse =>
         val retryMsg = unackMessages.remove(resp.reqSeqNum)
         if (retryMsg != null) {
            addToOutbox(resp)

            // stop request retransmission
            if (retryMsg._2 != null) retryMsg._2.cancel()

            // send response ack: not rejected
            val msg = ResponseAck(resp.seqNum, rejected = false)
            sender() ! msg
         } else {
            // send response ack: rejected
            val msg = ResponseAck(resp.seqNum, rejected = true)
            sender() ! msg
         }

      // sent response confirmed, reject or not the response
      case ack: ResponseAck =>
         val msgRetry = unackMessages.remove(ack.seqNum)
         if (msgRetry != null) {
            if (msgRetry._2 != null) msgRetry._2.cancel()
            val resp = msgRetry._1.msg.asInstanceOf[StealWorkResponse]
            if (ack.rejected && resp.workUnit != null) {
               rejectedResponses.add(resp)
            }
         }

      /**
       * } Work stealing
       */

      case Terminate =>
         context.become(terminating(actor))
         self ! Terminate

      case Stop =>
         context.become(terminating(actor))
         self ! Stop

      case Retry(msg, dest, delay) =>
         if (unackMessages.containsKey(msg.seqNum)) {
            sendMsgWithRetransmission(msg, dest, delay)
            logWarn(s"Active: Retrying message step=${engine.step}" +
               s" stageId=${engine.stageId} ${msg}")
         }

      case TerminationBarrier(seqNum, partitionId) =>
         if (!reachedTerminationBarrierSlaves(partitionId)) {
            reachedTerminationBarrierSlaves(partitionId) = true
            reachedTerminationBarrierCount += 1
         }

         val ack = TerminationBarrierAck(seqNum)
         sender() ! ack

      case ReceiveTimeout =>
      // nothing

      case other =>
         logWarn(s"Active: ${self} is not ready. Ignoring message: ${other}.")
   }

   def terminating(actor: ActorRef): Actor.Receive = {
      case TerminationBarrier(seqNum, partitionId) =>
         if (!reachedTerminationBarrierSlaves(partitionId)) {
            reachedTerminationBarrierSlaves(partitionId) = true
            reachedTerminationBarrierCount += 1
         }

         val ack = TerminationBarrierAck(seqNum)
         sender() ! ack

         // every slave reached the barrier and every slave knows that this
         // thread reached the barrier --> safe to terminate
         if (reachedTerminationBarrierCount == slaves.length
            && unackMessages.isEmpty) {
            addToOutbox(emptyResponseReusable)
         }

      case TerminationBarrierAck(seqNum) =>
         val msgRetry = unackMessages.remove(seqNum)
         if (msgRetry != null) {
            msgRetry._2.cancel()
         }

         // every slave reached the barrier and every slave knows that this
         // thread reached the barrier --> safe to terminate
         if (reachedTerminationBarrierCount == slaves.length
            && unackMessages.isEmpty) {
            addToOutbox(emptyResponseReusable)
         }

      case Retry(msg, dest, delay) =>
         if (unackMessages.containsKey(msg.seqNum)) {
            sendMsgWithRetransmission(msg, dest, delay)
            logWarn(s"Terminating: Retrying message step=${engine.step}" +
               s" stageId=${engine.stageId} ${msg}")
         }

      case ReadyToFinish(slaveId, knowsEveryoneIsReadyToFinish) =>
         if (readyToFinishSlaves != null) {
            if (!readyToFinishSlaves(slaveId)) {
               readyToFinishSlaves(slaveId) = true
               newSlaveReadyToFinish
            }

            if (!knowsEveryoneIsReadyToFinish) {
               sender() ! ReadyToFinish(partitionId,
                  readyToFinishCount == slaves.length)
            }
         }

      case req: StealWorkRequest =>
         val msg = StealWorkResponse(null, slaves.length,
            req.seqNum, nextSeqNum)
         req.thief ! msg

      // sent response confirmed, reject or not the response
      case ack: ResponseAck =>
         val msgRetry = unackMessages.remove(ack.seqNum)
         if (msgRetry != null) {
            if (msgRetry._2 != null) msgRetry._2.cancel()
            val resp = msgRetry._1.msg.asInstanceOf[StealWorkResponse]
            if (ack.rejected && resp.workUnit != null) {
               rejectedResponses.add(resp)
            }
         }

      case Terminate =>
         // stop all subgraph enumerators
         logInfo(s"Forcing termination of computation ${computation}")
         forcedTermination = true
         engine.terminate()
         masterRef ! ByeMaster(partitionId, self)
         context.stop(self)

      case Stop =>
         logInfo(s"Safe termination of computation ${computation}")
         masterRef ! ByeMaster(partitionId, self)
         context.stop(self)

      case ReceiveTimeout =>
      // nothing

      case other =>
         logWarn(s"Terminating: ${self} is not ready. Ignoring " +
            s"message: ${other}. ${readyToFinishSlaves.mkString(",")}")
   }

   private def addAllToOutbox(msgs: java.util.Collection[StealWorkResponse])
   : Unit = {
      outbox.addAll(msgs)
      outbox.synchronized {
         outbox.notify()
      }
   }

   private def addToOutbox(msg: StealWorkResponse): Unit = {
      outbox.add(msg)
      outbox.synchronized {
         outbox.notify()
      }
   }

   private def sendRequestWithTimeout(msg: StealWorkRequest,
                                      dest: ActorRef): Unit = {
      // send first time
      dest ! msg

      val reqRetry = Retry(msg, dest, 1000)

      val respTimeout = StealWorkResponse(null, 0, msg.seqNum, nextSeqNum)

      import context.dispatcher
      val cancellable = context.system.scheduler.
         scheduleOnce(1 seconds, self, respTimeout)

      // mark as not acknowledged
      unackMessages.put(msg.seqNum, (reqRetry, cancellable))
   }

   private def sendMsgWithRetransmission(msg: SeqNum, dest: ActorRef,
                                         delayMs: Int = 2000): Unit = {
      // send first time
      dest ! msg

      // get or create retry message
      val retryMsgCancellable = unackMessages.get(msg.seqNum)
      val retryMsg = {
         if (retryMsgCancellable != null) {
            retryMsgCancellable._2.cancel()
            retryMsgCancellable._1
         }
         else Retry(msg, dest, delayMs)
      }

      // schedule timeout for this message
      import context.dispatcher
      val cancellable = context.system.scheduler.scheduleOnce(
         delayMs millis, self, retryMsg)

      // mark as not acknowledged
      unackMessages.put(msg.seqNum, (retryMsg, cancellable))
   }

   private def sendIdentifyRequest(): Unit = {
      context.actorSelection(masterPath) ! Identify(masterPath)
      import context.dispatcher
      context.system.scheduler.scheduleOnce(3 seconds, self, ReceiveTimeout)
      logInfo(s"${self} sending identification to master")
   }

   private def reportStatsScheduler(): Unit = {
      import context.dispatcher
      context.system.scheduler.schedule(
         infoPeriod millis, infoPeriod millis, self, ReportStats)
   }

   private def maybeCheckReadyToFinishSlaves(): Unit = {
      if (outbox != null && slaves != null &&
         readyToFinishCount != slaves.length) {
         val msg = ReadyToFinish(partitionId, false)
         slavesRouter.route(msg, self)
      }
   }

   private def serializeSubgraphBatch[S <: Subgraph]
   (consumer: SubgraphEnumerator[S], batchSize: Int, workUnit: IntArrayList)
   : Int = {
      // int array format: depth, prefixSize, prefix, words

      workUnit.clear()

      val prefix = consumer.getPrefix
      val depth = consumer.getComputation.getDepth

      workUnit.add(depth)
      workUnit.add(prefix.size())
      workUnit.addAll(prefix)

      // add words up to a maximum of *batchSize*
      var n = 0
      var extension = consumer.nextExtension()
      do {
         workUnit.add(extension)
         extension = consumer.nextExtension()
         n += 1
      } while (n < batchSize &&
         extension > SubgraphEnumerator.INVALID_EXTENSION)

      n
   }

   private def tryStealFromLocal(workUnit: IntArrayList): Int = {
      val computations = LocalComputationStore.localComputations(
         computation.getExecutionEngine.getStageId
      ).asInstanceOf[ObjArrayList[Computation[S]]]

      if (computations == null) return -1

      var thisComp = threadSafeComputation
      var thisSubgraphEnumerator = thisComp.getSubgraphEnumerator

      val batchSize = config.getWsBatchSize

      val numComputations = computations.size()
      unvisitedComputations.clear()

      // first level: root computations (fill visited array)
      var i = 0
      while (i < numComputations) {
         val thatComp = computations.getu(i)
         if (thatComp != null) {
            val thatSubgraphEnumertor = thatComp.getSubgraphEnumerator
            if (thatSubgraphEnumertor.forkEnumerator(thisComp, false)) {
               val workStealed = serializeSubgraphBatch(
                  thisSubgraphEnumerator, batchSize, workUnit)
               if (workStealed > 0) {
                  return workStealed
               }
            }

            val nextCompNext = thatComp.nextComputation()
            if (nextCompNext != null) {
               unvisitedComputations.add(nextCompNext)
            }
         }

         i += 1
      }

      // remaining levels
      i = 0
      var numUnvisitedComputations = unvisitedComputations.size()
      var continue = i < numUnvisitedComputations
      while (continue) {
         thisComp = thisComp.nextComputation()
         thisSubgraphEnumerator = thisComp.getSubgraphEnumerator
         while (continue && i < numUnvisitedComputations) {
            val thatComp = unvisitedComputations.getu(i)
            val thatSubgraphEnumerator = thatComp.getSubgraphEnumerator
            if (thatSubgraphEnumerator.forkEnumerator(thisComp, false)) {
               val workStealed = serializeSubgraphBatch(
                  thisComp.getSubgraphEnumerator, batchSize, workUnit)
               if (workStealed > 0) {
                  return workStealed
               }
            }

            val nextCompNext = thatComp.nextComputation()
            if (nextCompNext != null) {
               unvisitedComputations.add(nextCompNext)
            }

            i += 1
         }

         numUnvisitedComputations = unvisitedComputations.size()
         continue = continue && i < numUnvisitedComputations
      }

      if (numComputations < numLocalPeers) -1
      else 0
   }

   private def reportStats(): Unit = {
      val threadId = computation.getPartitionId
      val runtime = Runtime.getRuntime()
      val maxMemory = runtime.maxMemory()
      val totalMemory = runtime.totalMemory()
      val freeMemory = runtime.freeMemory()
      val usedMemory = totalMemory - freeMemory
      val validSubgraphs = computation.lastComputation().getNumValidExtensions

      val statsMsg = Stats(threadId, validSubgraphs, maxMemory, totalMemory,
         freeMemory, usedMemory)
      masterRef ! statsMsg
   }

   override def postStop(): Unit = {
      if (!forcedTermination && !unackMessages.isEmpty) {
         throw new RuntimeException(s"Unacknowledge messages " +
            s"partitionId=${partitionId}" +
            s" stepId=${engine.getStep()}" +
            s" ${unackMessages}")
      }

      if (!forcedTermination && !rejectedResponses.isEmpty) {
         throw new RuntimeException(s"Rejected responses: ${unackMessages}")
      }

      logInfo(s"SlaveActor stopped step=${computation.getStep}" +
         s" partitionId=${partitionId} actor=${self}")
      LocalComputationStore.unregisterComputation(engine)
   }
}

object ActorMessageSystem extends Logging {
   val actorRefComparator = new Comparator[ActorRef]() {
      override def compare(a1: ActorRef, a2: ActorRef): Int = {
         a1.compareTo(a2)
      }
   }

   private var _masterAkkaSysOpt: Option[ActorSystem] = None

   private var _executorAkkaSysOpt: Option[ActorSystem] = None

   def executorAkkaSysOpt: Option[ActorSystem] = _executorAkkaSysOpt

   def masterAkkaSysOpt: Option[ActorSystem] = _masterAkkaSysOpt

   private val nextActorId: AtomicInteger = new AtomicInteger(0)

   def getNextActorId: Int = nextActorId.getAndIncrement

   private def getDefaultProperties: Properties = {
      val props = new Properties()
      props.setProperty("akka.actor.provider", "remote")
      props.setProperty("akka.remote.netty.tcp.hostname",
         java.net.InetAddress.getLocalHost().getHostAddress())
      props
   }

   private def getAkkaConfig(props: Properties): Config = {
      val customConfig = ConfigFactory.parseProperties(props)
      val defaultConfig = ConfigFactory.load()
      val combinedConfig = customConfig.withFallback(defaultConfig)
      ConfigFactory.load(combinedConfig)
   }

   private lazy val executorAkkaSys: ActorSystem = {
      val props = getDefaultProperties
      // setting "0" means to allocate the first/any OS port available
      props.setProperty("akka.remote.netty.tcp.port", "0")
      val as = ActorSystem("fractal-msgsys",
         config = Some(getAkkaConfig(props)))
      _executorAkkaSysOpt = Option(as)
      logInfo(s"Started akka-sys: ${as} - executor - waiting for messages")
      as
   }

   private lazy val masterAkkaSys: ActorSystem = {
      val props = getDefaultProperties
      props.setProperty("akka.remote.netty.tcp.port", "2552")
      val as = ActorSystem("fractal-msgsys",
         config = Some(getAkkaConfig(props)))
      _masterAkkaSysOpt = Option(as)
      logInfo(s"Started akka-sys: ${as} - master - waiting for messages")
      as
   }

   private[fractal] var mastersRouter: Router =
      new Router(new BroadcastRoutingLogic)

   def terminate(): Unit = synchronized {
      logInfo(s"Terminating masters ${mastersRouter}.")
      mastersRouter.route(Terminate, null)
   }

   def shutdown() = {
      if (executorAkkaSysOpt.isDefined) {
         executorAkkaSysOpt.get.terminate()
      }
      if (masterAkkaSysOpt.isDefined) {
         masterAkkaSysOpt.get.terminate()
      }
   }

   def createActor(engine: SparkMasterEngine[_ <: Subgraph]): ActorRef =
      synchronized {
      val fc = engine.fractoid.fractalContext
      if (!fc.acceptingNewJobs) {
         throw new InterruptedException(s"${fc} not accepting new jobs.")
      }
      val remotePath = s"akka.tcp://fractal-msgsys@" +
         s"${engine.config.getMasterHostname}:2552" +
         s"/user/master-actor-${engine.step}"
      val args = new MasterActor.Args(remotePath, engine)
      val masterRef = masterAkkaSys.actorOf(
         Props(classOf[MasterActor], args).
            withDispatcher("akka.actor.default-dispatcher"),
         s"master-actor-${engine.step}")
      mastersRouter = mastersRouter.addRoutee(ActorRefRoutee(masterRef))
      masterRef
   }

   def createActor[S <: Subgraph](engine: SparkEngine[S]): ActorRef = {
      val remotePath = s"akka.tcp://fractal-msgsys@" +
         s"${engine.configuration.getMasterHostname}:2552" +
         s"/user/master-actor-${engine.step}"
      val args = new SlaveActor.Args(remotePath, engine)
      val slaveActorRef = executorAkkaSys.actorOf(
         Props(classOf[SlaveActor[S]], args).
            withDispatcher("akka.actor.default-dispatcher"),
         s"slave-actor" +
            s"-${engine.stageId}-${engine.step}" +
            s"-${engine.partitionId}-${getNextActorId}")
      slaveActorRef
   }
}
