package br.ufmg.cs.systems.fractal.computation

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, ThreadLocalRandom}
import java.util.{Arrays, Comparator, Properties}

import akka.actor._
import akka.dispatch.MessageDispatcher
import akka.routing._
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

case class ResponseAck(seqNum: Long, rejected: Boolean) extends SeqNum

case class Retry(msg: SeqNum, dest: ActorRef)

/**
 * Message sent by the master at the end of each superstep
 */
case object Reset

/**
 * Message sent by slaves to the master for registering
 */
case class HelloMaster(partitionId: Int, slaveRef: ActorRef)

/**
 * Message sent by slaves to the master indicating the end of a step
 */
case class ByeMaster(partitionId: Int, slaveRef: ActorRef)

/*
 * Message sent by the master to a slave to inform the references of all other
 * slaves
 */
case class HelloSlave(slaveRefs: Array[ActorRef])

/**
 * Message sent to the local gtag actor to start a remote work-stealing event
 */
case object StealWork

case object DrainRejectedWork

/**
 * Message sent by a slave to other slaves to indicate a ready state for
 * termination
 */
case class ReadyToFinish(partitionId: Int)

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
//case class StealWorkResponse(workOpt: Option[Array[Byte]], numPeers: Int,
//                             seqNum: Long) extends SeqNum
case class StealWorkResponse(workUnit: IntArrayList, numPeers: Int,
                             reqSeqNum: Long, seqNum: Long) extends SeqNum

/**
 * Log message
 */
case class Log(msg: String)

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
 * Gtag actor
 */
abstract class MSActor(masterPath: String) extends Actor with Logging {
   /**
    * Akka reference of the master. Master here is used as a rendevouz point to
    * spread actor references between slaves.
    */
   def masterRef: ActorRef

   logInfo(s"Actor ${self} started")
}

/**
 * Master actor
 */
class MasterActor(masterPath: String, engine: SparkMasterEngine[_])
   extends MSActor(masterPath) {

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
      case HelloMaster(partitionId, slaveRef) =>
         if (slaves(partitionId) == null) {
            registeredSlaves += 1
            slavesRouter = slavesRouter.addRoutee(ActorRefRoutee(slaveRef))
         }

         logInfo(s"${self} knows ${registeredSlaves} slaves.")

         slaves(partitionId) = slaveRef

         if (registeredSlaves == numSlaves) {
            val helloMsg = HelloSlave(slaves)
            slavesRouter.route(helloMsg, self)
            logInfo(s"Publishing ${numSlaves} slaves.")
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

      case Log(msg) =>
         logInfo(msg)

      case Stats(partitionId, _validSubgraphs, maxMemory, totalMemory,
      freeMemory, usedMemory) =>

         logInfo(s"MemoryStats threadId=${partitionId}" +
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

         logInfo(s"ValidSubgraphsUpdate [${validSubgraphs.mkString(",")}]" +
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
            logInfo(
               s"Reached the limit of ${maxValidSubgraphs} valid subgraphs." +
                  s" Terminating slaves=${slaves.mkString(",")}")

            // send termination messages to all known slaves
            var i = 0
            while (i < slaves.length) {
               val s = slaves(i)
               if (s != null) {
                  logInfo(s"Sending termination message to ${s} (id=${i})")
                  s ! Terminate
               }
               i += 1
            }
         }

      case Reset =>
         reset()

      case Terminated(p: ActorRef) =>

      case _ =>
   }
}

/**
 * Slave actor
 */
class SlaveActor[S <: Subgraph](masterPath: String, engine: SparkEngine[S])
   extends MSActor(masterPath) {

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

   // register with master
   sendIdentifyRequest()

   // report execution stats from time to time
   reportStatsScheduler()

   def receive = identifying

   def identifying: Actor.Receive = {
      case ActorIdentity(`masterPath`, Some(actor)) =>
         _masterRef = actor
         context.become(active(actor))
         masterRef ! HelloMaster(partitionId, self)
         reportStats()
         logInfo(s"${self} knows master: ${masterRef}")

      case WorkQueue(_outbox) =>
         outbox = _outbox

      case StealWork =>
         addToOutbox(emptyResponseReusable)

      case ReceiveTimeout =>
         sendIdentifyRequest()

      case other =>
         logWarning(s"${self} is not ready. Ignoring message: ${other}.")
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

      case ReadyToFinish(slaveId) =>
         if (readyToFinishSlaves != null) {
            if (!readyToFinishSlaves(slaveId)) {
               readyToFinishSlaves(slaveId) = true
               newSlaveReadyToFinish
            }
            sender() ! ReadyToFinish(partitionId)
         }

      case DrainRejectedWork =>
         if (unackMessages.isEmpty) {
            if (rejectedResponses.isEmpty) {
               outbox.add(emptyResponseReusable)
            } else {
               outbox.addAll(rejectedResponses)
               rejectedResponses.clear()
            }
            outbox.synchronized {
               outbox.notify()
            }
         } else { // wait for all rejected work
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

      case inMsg@Log(msg) =>
         masterRef ! inMsg

      case Terminated(`actor`) =>
         sendIdentifyRequest()
         context.become(identifying)
         logInfo(s"Master ${actor} terminated")

      case Terminate =>
         // stop all subgraph enumerators
         logInfo(s"Terminating computation ${computation}")
         var currComp = computation
         while (currComp != null) {
            val senum = currComp.getSubgraphEnumerator()
            if (senum != null) senum.terminate()
            currComp = currComp.nextComputation()
         }
         masterRef ! ByeMaster(partitionId, self)

      case Retry(msg, dest) =>
         if (unackMessages.containsKey(msg.seqNum)) {
            sendMsgWithRetransmission(msg, dest)
            logWarning(s"Retrying message ${msg}")
         }

      case ReceiveTimeout =>
      // nothing

      case other =>
         logWarning(s"${self} is not ready. Ignoring message: ${other}.")
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

      val reqRetry = Retry(msg, dest)

      val respTimeout = StealWorkResponse(null, 0, msg.seqNum, nextSeqNum)

      import context.dispatcher
      val cancellable = context.system.scheduler.
         scheduleOnce(1 seconds, self, respTimeout)

      // mark as not acknowledged
      unackMessages.put(msg.seqNum, (reqRetry, cancellable))
   }

   private def sendMsgWithRetransmission(msg: SeqNum, dest: ActorRef): Unit = {
      // send first time
      dest ! msg

      // get or create retry message
      val retryMsgCancellable = unackMessages.get(msg.seqNum)
      val retryMsg = {
         if (retryMsgCancellable != null) {
            retryMsgCancellable._2.cancel()
            retryMsgCancellable._1
         }
         else Retry(msg, dest)
      }

      // schedule timeout for this message
      import context.dispatcher
      val cancellable = context.system.scheduler.scheduleOnce(
         2 second, self, retryMsg)

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
         val msg = ReadyToFinish(partitionId)
         var i = 0
         while (i < slaves.length) {
            if (!readyToFinishSlaves(i)) slaves(i) ! msg
            i += 1
         }
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
            if (thatSubgraphEnumertor.forkEnumerator(thisComp)) {
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
            if (thatSubgraphEnumerator.forkEnumerator(thisComp)) {
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
      if (!unackMessages.isEmpty) {
         throw new RuntimeException(s"Unacknowledge messages " +
            s"partitionId=${partitionId}" +
            s" stepId=${engine.getStep()}" +
            s" ${unackMessages}")
      }

      if (!rejectedResponses.isEmpty) {
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

   private var _akkaSysOpt: Option[ActorSystem] = None

   private var _masterAkkaSysOpt: Option[ActorSystem] = None

   private var _executorAkkaSysOpt: Option[ActorSystem] = None

   def akkaSysOpt: Option[ActorSystem] = _akkaSysOpt

   def executorAkkaSysOpt: Option[ActorSystem] = _executorAkkaSysOpt

   def masterAkkaSysOpt: Option[ActorSystem] = _masterAkkaSysOpt

   private val nextActorId: AtomicInteger = new AtomicInteger(0)

   def getNextActorId: Int = nextActorId.getAndIncrement

   private val nextRequestId: AtomicInteger = new AtomicInteger(0)

   def getNextRequestId: Int = nextRequestId.getAndIncrement

   def akkaExecutorContext: MessageDispatcher = {
      _akkaSysOpt.map(_.dispatchers.lookup("akka.actor.default-dispatcher")).
         getOrElse(null)
   }

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

   private lazy val _executorAkkaSys: ActorSystem = {
      val props = getDefaultProperties
      // setting "0" means to allocate the first/any OS port available
      props.setProperty("akka.remote.netty.tcp.port", "0")
      val as = ActorSystem("fractal-msgsys",
         config = Some(getAkkaConfig(props)))
      _akkaSysOpt = Option(as)
      _executorAkkaSysOpt = Option(as)
      logInfo(s"Started akka-sys: ${as} - executor - waiting for messages")
      as
   }

   private lazy val _masterAkkaSys: ActorSystem = {
      val props = getDefaultProperties
      props.setProperty("akka.remote.netty.tcp.port", "2552")
      val as = ActorSystem("fractal-msgsys",
         config = Some(getAkkaConfig(props)))
      _akkaSysOpt = Option(as)
      _masterAkkaSysOpt = Option(as)
      logInfo(s"Started akka-sys: ${as} - master - waiting for messages")
      as
   }

   def akkaSys(engine: SparkMasterEngine[_]): ActorSystem = synchronized {
      _masterAkkaSys
   }

   def akkaSys(engine: SparkEngine[_]): ActorSystem = synchronized {
      _executorAkkaSys
   }

   def shutdown() = {
      if (executorAkkaSysOpt.isDefined) {
         executorAkkaSysOpt.get.terminate()
      }
      if (masterAkkaSysOpt.isDefined) {
         masterAkkaSysOpt.get.terminate()
      }
   }

   def createActor(engine: SparkMasterEngine[_]): ActorRef = {
      val remotePath = s"akka.tcp://fractal-msgsys@" +
         s"${engine.config.getMasterHostname}:2552" +
         s"/user/master-actor-${engine.step}"
      akkaSys(engine).actorOf(
         Props(classOf[MasterActor], remotePath, engine).
            withDispatcher("akka.actor.default-dispatcher"),
         s"master-actor-${engine.step}")
   }

   def createActor[E <: Subgraph](engine: SparkEngine[E]): ActorRef = {
      val remotePath = s"akka.tcp://fractal-msgsys@" +
         s"${engine.configuration.getMasterHostname}:2552" +
         s"/user/master-actor-${engine.step}"
      val slaveActorRef = akkaSys(engine).actorOf(
         Props(classOf[SlaveActor[E]], remotePath, engine).
            withDispatcher("akka.actor.default-dispatcher"),
         s"slave-actor" +
            s"-${engine.stageId}-${engine.step}" +
            s"-${engine.partitionId}-${getNextActorId}")

      slaveActorRef
   }

}
