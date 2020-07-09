package br.ufmg.cs.systems.fractal.computation

import java.io._
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentLinkedQueue, ThreadLocalRandom}
import java.util.{Arrays, Comparator, Properties}

import akka.actor._
import akka.dispatch.MessageDispatcher
import akka.routing._
import br.ufmg.cs.systems.fractal.conf.Configuration
import br.ufmg.cs.systems.fractal.subgraph.Subgraph
import br.ufmg.cs.systems.fractal.util.{EventTimer, Logging}
import com.koloboke.collect.map.LongObjMap
import com.koloboke.collect.map.hash.HashLongObjMaps
import com.koloboke.collect.set.hash.HashIntSets
import com.typesafe.config.{Config, ConfigFactory}
import io.netty.util.collection.LongObjectMap

import scala.collection.mutable.Map
import scala.concurrent.duration._

sealed trait SeqNum {
   def seqNum: Long
}

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
case class StealWorkResponse(workOpt: Option[Array[Byte]], numPeers: Int,
                             seqNum: Long) extends SeqNum

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
case class Stats(partitionId: Int, validSubgraphs: Long)

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

         if (EventTimer.ENABLED) {
            if (registeredSlaves == 1) {
               EventTimer.masterInstance(0).finish(EventTimer.INITIALIZATION)
            }
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

         if (EventTimer.ENABLED) {
            if (registeredSlaves == numSlaves - 1) {
               EventTimer.masterInstance(0).start(EventTimer.AGGREGATION)
            }
         }

         logInfo(s"ByeMaster: ${self} knows ${registeredSlaves} slaves.")

         slaves(partitionId) = null

         if (registeredSlaves == 0) {
            reset()
            engine.reportAccumulators
         }

      case Log(msg) =>
         logInfo(msg)

      case Stats(partitionId, _validSubgraphs) =>

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
                  s"/${maxValidSubgraphs} total(%)=${(totalValidSubgraphs / maxValidSubgraphs.toDouble)*100}/100"
               else
                  ""
               )
         )

         // if we reach *maxValidSubgraphs* then stop execution, we are done
         if (totalValidSubgraphs >= maxValidSubgraphs) {
            logInfo(s"Reached the limit of ${maxValidSubgraphs} valid subgraphs." +
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
class SlaveActor [S <: Subgraph](masterPath: String, engine: SparkEngine[S])
   extends MSActor(masterPath) {

   private val computation: Computation[S] = engine.computation

   private val partitionId: Int = engine.getPartitionId()

   private val config: Configuration[S] = computation.getConfig()

   private var _masterRef: ActorRef = _

   override def masterRef: ActorRef = _masterRef

   private var slaves: Array[ActorRef] = _

   private var pivots: Array[ActorRef] = _

   private var localPivotIdx: Int = -1

   private var outbox: ConcurrentLinkedQueue[StealWorkResponse] = _

   private var slavesRouter: Router = new Router(new BroadcastRoutingLogic())

   private var readyToFinishCount: Int = 0

   private val unackMessages: LongObjMap[(Retry,Cancellable)] =
      HashLongObjMaps.newMutableMap()

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

   private def emptyResponse = StealWorkResponse(None, 0, nextSeqNum)

   private def emptyResponse(seqNum: Long) = StealWorkResponse(None, 0, seqNum)

   private val infoPeriod = computation.getConfig().getInfoPeriod()

   private val externalWsEnabled = computation.getConfig().externalWsEnabled()

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

      case ReadyToFinish(slaveId) =>
         if (readyToFinishSlaves != null) {
            if (!readyToFinishSlaves(slaveId)) {
               readyToFinishSlaves(slaveId) = true
               newSlaveReadyToFinish
            }
            sender() ! ReadyToFinish(partitionId)
         }

      case StealWork =>
         sendMsgReliable(emptyResponse, self)
         //sendEmptyResponse(self)

      case inMsg: StealWorkResponse =>
         val seqNum = inMsg.seqNum
         if (unackMessages.containsKey(seqNum)) {
            outbox.add(inMsg)
            outbox.synchronized {
               outbox.notify()
            }
            unackMessages.remove(seqNum)
         }

      case ActorIdentity(`masterPath`, None) =>
         logDebug(s"Remote actor not available: $masterPath")

      case ReceiveTimeout =>
         sendIdentifyRequest()

      case inMsg @ Retry(msg, dest) =>
         if (unackMessages.containsKey(msg.seqNum)) {
            sendMsgReliable(msg, dest)
            logInfo(s"Retrying message ${msg}")
         }

      case other =>
         logDebug(s"${self} is not ready. Ignoring message: ${other}.")
   }

   def active(actor: ActorRef): Actor.Receive = {
      case HelloSlave(_slaves) =>
         slaves = _slaves
         if (slaves.length > 0) {
            val pivotMap: Map[Address,ActorRef] = Map.empty
            var i = 0
            var numLocalPeers = 0
            while (i < slaves.length) {
               val ref = slaves(i)
               slavesRouter = slavesRouter.addRoutee(ActorRefRoutee(ref))
               val refAddr = ref.path.address
               pivotMap.update (refAddr, ref)
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

            computation.getConfig().taskCheckIn(0, numLocalPeers)
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

      case ReportStats =>
         reportStats()

      case inMsg @ StealWork =>
         if (slaves != null) {
            if (externalWsEnabled) {
               val numPivots = pivots.length
               val nextPivot = (localPivotIdx + 1) % numPivots
               val lastPivot = (localPivotIdx + numPivots - 1) % numPivots

               // this message must be reliable
               val msg = StealWorkRequest(self, nextPivot, lastPivot, nextSeqNum)
               val aRef = pivots(nextPivot)
               sendMsgReliable(msg, aRef)

               //aRef ! msg

               logDebug(s"${inMsg}: ${self} sending ${msg} to ${aRef}")
            } else {
               val msg = StealWorkResponse(None, getReadyToFinishCount, nextSeqNum)
               sendMsgReliable(msg, self)
               //self ! msg
               logDebug(s"${inMsg}: ${self} sending ${msg} to ${self}")
            }
         } else {
            sendMsgReliable(emptyResponse, self)
            //sendEmptyResponse(self)
         }

      case inMsg @ StealWorkRequest(
      thief, nextPivot, lastPivot, seqNum) if nextPivot == lastPivot =>
         if (slaves != null) {
            val workOpt = ActorMessageSystem.tryStealFromLocal(computation)

            val readyToFinish = getReadyToFinishCount == slaves.length

            if (workOpt != null &&
               (workOpt.isDefined || readyToFinish)) {
               val msg = StealWorkResponse(workOpt, slaves.length, seqNum)
               thief ! msg
               logDebug(s"${inMsg}: ${self} sending ${msg} to ${thief}")
            } else {
               sendEmptyResponseWithSeqNum(thief, seqNum)
            }
         } else {
            sendEmptyResponseWithSeqNum(thief, seqNum)
         }

      case inMsg @ StealWorkRequest(thief, thisPivot, lastPivot, seqNum) =>
         if (slaves != null) {
            val workOpt = ActorMessageSystem.tryStealFromLocal(computation)
            if (workOpt != null && workOpt.isDefined) {
               val msg = StealWorkResponse(workOpt, slaves.length, seqNum)
               thief ! msg
               logDebug(s"${inMsg}: ${self} sending ${msg} to ${thief}")
            } else {
               val numPivots = pivots.length
               val nextPivot = (thisPivot + 1) % numPivots
               val msg = StealWorkRequest(thief, nextPivot, lastPivot, seqNum)
               val p = pivots(nextPivot)
               p ! msg
               logDebug(s"${inMsg}: ${self} forwarded ${msg} to ${p}")
            }
         } else {
            sendEmptyResponseWithSeqNum(thief, seqNum)
         }

      case inMsg: StealWorkResponse =>
         val seqNum = inMsg.seqNum
         if (unackMessages.containsKey(seqNum)) {
            outbox.add(inMsg)
            outbox.synchronized {
               outbox.notify()
            }
            unackMessages.remove(seqNum)
         }

      case inMsg @ Log(msg) =>
         masterRef ! inMsg

      case Terminated(`actor`) =>
         sendIdentifyRequest()
         context.become(identifying)
         logInfo(s"Master ${actor} terminated")

      case ReceiveTimeout =>
      // ignore

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

      case inMsg @ Retry(msg, dest) =>
         if (unackMessages.containsKey(msg.seqNum)) {
            sendMsgReliable(msg, dest)
            logInfo(s"Retrying message ${inMsg}")
         }
   }

   private def sendMsgReliable(msg: SeqNum, dest: ActorRef): Unit = {
      // send first time
      dest ! msg

      // get or create retry message
      val retryMsgCancellable = unackMessages.get(msg.seqNum)
      val retryMsg = {
         if (retryMsgCancellable != null) retryMsgCancellable._1
         else Retry(msg, dest)
      }

      // schedule timeout for this message
      import context.dispatcher
      val cancellable = context.system.scheduler.scheduleOnce(
         500 millis, self, retryMsg)

      // mark as not acknowledged
      unackMessages.put(msg.seqNum, (retryMsg, cancellable))
   }

   private def sendEmptyResponseWithSeqNum(ref: ActorRef, seqNum: Long)
   : Unit = {
      //if (ThreadLocalRandom.current().nextBoolean()) {
         val msg = emptyResponse(seqNum)
         ref ! msg
         logDebug(s"Sending empty response ${msg} to ${ref}.")
      //} else {
      //   logWarning(s"Loosing message ${seqNum}")
      //}
   }

   private def sendEmptyResponse(ref: ActorRef): Unit = {
      ref ! emptyResponse
      logDebug(s"Sending empty response ${emptyResponse} to ${ref}.")
   }

   private def sendIdentifyRequest(): Unit = {
      logInfo(s"${self} sending identification to master")
      context.actorSelection(masterPath) ! Identify(masterPath)
      import context.dispatcher
      context.system.scheduler.scheduleOnce(3 seconds, self, ReceiveTimeout)
   }

   private def reportStatsScheduler(): Unit = {
      import context.dispatcher
      context.system.scheduler.schedule(
         infoPeriod millis, infoPeriod millis, self, ReportStats)
   }

   private def maybeCheckReadyToFinishSlaves(): Unit = {
      if (outbox != null && slaves != null && readyToFinishCount != slaves.length) {
         val msg = ReadyToFinish(partitionId)
         var i = 0
         while (i < slaves.length) {
            if (!readyToFinishSlaves(i)) slaves(i) ! msg
            i += 1
         }
      }
   }

   private def reportStats(): Unit = {
      val engine = computation.getExecutionEngine().
         asInstanceOf[SparkFromScratchEngine[S]]

      masterRef ! Log(
         s"StatsReport{" +
            s"step=${computation.getStep}," +
            s"partitionId=${computation.getPartitionId}," +
            s"${engine.getStatsAccumulators}," +
            reportMemoryStats() + "}")

      var validSubgraphsDepth = 0
      while (engine.accums.contains(s"valid_subgraphs_${validSubgraphsDepth}")) {
         validSubgraphsDepth += 1
      }
      validSubgraphsDepth -= 1

      masterRef ! Stats(computation.getPartitionId,
         engine.accums(s"valid_subgraphs_${validSubgraphsDepth}").value)
   }

   private def reportMemoryStats(): String = {
      def scale(n: Double): Double = n / (1024 * 1024 * 1024)
      val runtime = Runtime.getRuntime()
      val maxMemory = runtime.maxMemory()
      val totalMemory = runtime.totalMemory()
      val freeMemory = runtime.freeMemory()
      val usedMemory = totalMemory - freeMemory
      val step = computation.getStep()

      s"maxMemory=${scale(maxMemory)},totalMemory=${scale(totalMemory)}," +
         s"freeMemory=${scale(freeMemory)},usedMemory=${scale(usedMemory)}"
   }

   override def postStop(): Unit = {
      //if (!unackMessages.isEmpty) {
      //   throw new RuntimeException(s"Unacknowledge messages: ${unackMessages}")
      //}
      logInfo(s"SlaveActor stopped step=${computation.getStep}" +
         s" partitionId=${partitionId} actor=${self}")
      computation.getConfig().taskCheckOut()
      SparkFromScratchEngine.unregisterComputation(engine)
   }
}

object ActorMessageSystem extends Logging {
   val actorRefComparator = new Comparator[ActorRef] () {
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
      val as = ActorSystem("fractal-msgsys", config = Some(getAkkaConfig(props)))
      _akkaSysOpt = Option(as)
      _executorAkkaSysOpt = Option(as)
      logInfo(s"Started akka-sys: ${as} - executor - waiting for messages")
      as
   }

   private lazy val _masterAkkaSys: ActorSystem = {
      val props = getDefaultProperties
      props.setProperty("akka.remote.netty.tcp.port", "2552")
      val as = ActorSystem("fractal-msgsys", config = Some(getAkkaConfig(props)))
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
         s"/user/master-actor-${engine.config.getId}-${engine.step}"
      akkaSys(engine).actorOf(
         Props(classOf[MasterActor], remotePath, engine).
            withDispatcher("akka.actor.default-dispatcher"),
         s"master-actor-${engine.config.getId}-${engine.step}")
   }

   def createActor [E <: Subgraph] (engine: SparkEngine[E]): ActorRef = {
      val remotePath = s"akka.tcp://fractal-msgsys@" +
         s"${engine.configuration.getMasterHostname}:2552" +
         s"/user/master-actor-${engine.configuration.getId}" +
         s"-${engine.step}"
      val slaveActorRef = akkaSys(engine).actorOf(
         Props(classOf[SlaveActor[E]], remotePath, engine).
            withDispatcher("akka.actor.default-dispatcher"),
         s"slave-actor" +
            s"-${engine.configuration.getId}-${engine.step}" +
            s"-${engine.partitionId}-${getNextActorId}")

      // wait for this slave to know the all other slaves before proceed
      //slaveActorRef.synchronized {
      //   while (engine.configuration.taskCounter() == 0) {
      //      slaveActorRef.wait()
      //   }
      //}

      // ensure that local computation structures are properly created
      //SparkFromScratchEngine.createComputationsMap(
      //   engine.step, engine.configuration.taskCounter())
      SparkFromScratchEngine.createComputationsMap(engine)

      slaveActorRef
   }

   /**
    */
   def tryStealFromLocal [E <: Subgraph] (
                                            c: Computation[E]): Option[Array[Byte]] = {
      val computations = SparkFromScratchEngine.localComputations[E](c.getStep())
      val numComputations = computations.size()

      val gtagBatchLow = c.getConfig().getWsBatchSizeLow()
      val gtagBatchHigh = c.getConfig().getWsBatchSizeHigh()
      val batchSize = ThreadLocalRandom.current().nextInt(
         gtagBatchHigh - gtagBatchLow + 1) + gtagBatchLow

      var missingComputation = false

      var i = ThreadLocalRandom.current().nextInt(numComputations)
      val offset = i + numComputations
      while (i < offset) {
         val currComp = computations.get(i % numComputations)
         if (currComp != null) {
            val consumer = currComp.forkEnumerator(c)
            if (consumer != null) {
               val ebytesOpt = serializeSubgraphBatch(consumer, batchSize)
               if (ebytesOpt.isDefined) {
                  return ebytesOpt
               }
            }
         } else {
            missingComputation = true
         }
         i += 1
      }

      if (!missingComputation) {
         None
      } else {
         null
      }
   }

   /**
    */
   def serializeSubgraphBatch [E <: Subgraph] (
                                                 consumer: SubgraphEnumerator[E],
                                                 batchSize: Int): Option[Array[Byte]] = {

      if (!consumer.hasNext) {
         return None
      }

      var bos: ByteArrayOutputStream = null
      var oos: ObjectOutputStream = null
      var closed = false
      try {
         bos = new ByteArrayOutputStream()
         oos = new ObjectOutputStream(bos)

         // computation label
         oos.writeObject(consumer.computationLabel())

         // prefix
         val prefix = consumer.prefix
         val prefixSize = prefix.size()
         oos.writeInt(prefixSize)
         var i = 0
         while (i < prefixSize) {
            oos.writeInt(prefix.getu(i))
            i += 1
         }

         // word ids
         oos.writeInt(batchSize)
         i = 0
         while (i < batchSize && consumer.hasNext) {
            val n = consumer.nextElem
            oos.writeInt(n)
            i += 1
         }

         oos.close
         closed = true
         Some(bos.toByteArray)
      } finally {
         if (!closed) oos.close
      }
   }

   def deserializeSubgraphBatch [E <: Subgraph] (buf: Array[Byte],
                                                 c: Computation[E]): SubgraphEnumerator[E] = {
      var bis: ByteArrayInputStream = null
      var ois: ObjectInputStream = null
      try {
         bis = new ByteArrayInputStream(buf)
         ois = new ObjectInputStream(bis)

         // label
         val label = ois.readObject().asInstanceOf[String]
         var curr = c
         while (curr != null && curr.computationLabel() != label) {
            curr = curr.nextComputation()
         }

         //assert (curr != null, s"label=${label} ${c}")

         // prefix
         val prefixSize = ois.readInt()
         val subgraph = c.getConfig().createSubgraph[E]
         var i = 0
         while (i < prefixSize) {
            subgraph.addWord(ois.readInt())
            i += 1
         }

         // word ids
         val batchSize = ois.readInt()
         val wordIds = HashIntSets.newMutableSet(batchSize)
         i = 0
         while (ois.available() > 0) {
            wordIds.add(ois.readInt())
            i += 1
         }

         val currIterator = curr.getSubgraphEnumerator()


         currIterator.set(curr, subgraph, null)
         currIterator.set(wordIds)
         currIterator.rebuildState()

         currIterator


      } finally {
         ois.close
      }
   }
}

class WorkStealingSystem [S <: Subgraph] (
                                            processCompute: (SubgraphEnumerator[S],Computation[S]) => Long,
                                            slaveActor: ActorRef,
                                            remoteWorkQueue: ConcurrentLinkedQueue[StealWorkResponse],
                                            callback: (SubgraphEnumerator[S], Long) => Unit =
                                            (e: SubgraphEnumerator[S], ret: Long) => {}) extends Logging {

   private var newExternalRequestsAllowed = true
   private var requestsOnTheFly = 0L
   private val maxRequestsOnTheFly = 10 // TODO

   def workStealingComputeExternal(c: Computation[S]): Long = {
      val numPartitions = c.getNumberPartitions
      var externalSteals = 0L
      var response = remoteWorkQueue.poll()

      while (response != null) {
         requestsOnTheFly -= 1
         response.workOpt match {
            case Some(subgraphBatchBytes) =>
               val consumer = ActorMessageSystem.
                  deserializeSubgraphBatch(subgraphBatchBytes, c)
               val computation = consumer.getComputation()

               val ret = processCompute(consumer, computation)
               callback(consumer, ret)
               externalSteals += 1

            case None if response.numPeers == numPartitions =>
               // stop sending steal requests
               newExternalRequestsAllowed = false

            case _ =>
            // continue
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

      slaveActor ! WorkQueue(remoteWorkQueue)

      // case 1: internal work stealing only
      if (internalWsEnabled && !externalWsEnabled) {
         internalSteals += workStealingComputeLocal(c)
         wsIterations += 1
      }

      // case 2: internal and external work stealing
      else if (internalWsEnabled && externalWsEnabled) {

         // step 1: internal and external work stealing allowed
         while (newExternalRequestsAllowed) {
            // internal work stealing while still possible
            internalSteals += workStealingComputeLocal(c)

            // maybe send external work stealing request
            if (requestsOnTheFly < maxRequestsOnTheFly) {
               slaveActor ! StealWork
               requestsOnTheFly += 1
            }

            // wait for response (with timeout)
            remoteWorkQueue.synchronized {
               remoteWorkQueue.wait(100)
            }

            // check response and external work stealing
            externalSteals += workStealingComputeExternal(c)

            wsIterations += 1
         }

         // step 2: ensure all sent requests got responses
         while (requestsOnTheFly > 0) {
            externalSteals += workStealingComputeExternal(c)
            remoteWorkQueue.synchronized {
               remoteWorkQueue.wait(100)
            }
            wsIterations += 1
         }

         // step 3: only internal work stealing allowed (final)
         internalSteals += workStealingComputeLocal(c)
      }

      // case 3: only external work stealing allowed
      else if (!internalWsEnabled && externalWsEnabled) {
         throw new UnsupportedOperationException
      }

      // case 4: neither internal nor external work stealing allowed
      else {
         // do nothing
      }

      logInfo(s"FinishingExecutor" +
         s" step=${c.getStep}" +
         s" partitionId=${c.getPartitionId}" +
         s" internalSteals=${internalSteals}" +
         s" externalSteals=${externalSteals}" +
         s" requestsOnTheFly=${requestsOnTheFly}" +
         s" wsIterations=${wsIterations}" +
         s" maxRequestsOnTheFly=${maxRequestsOnTheFly}" +
         s" numPartitionsTotal=${numPartitions}")
   }

   def workStealingCompute2(c: Computation[S]): Unit = {
      implicit val executionContext = ActorMessageSystem.akkaExecutorContext
      var workStealed = true
      @volatile var remoteRequestSent = false
      var requestsBalance = 0
      var remoteCancellable = null.asInstanceOf[Cancellable]
      val internalWsEnabled = c.getConfig().internalWsEnabled()
      val externalWsEnabled = c.getConfig().externalWsEnabled()
      val numPartitions = c.getNumberPartitions
      var internalSteals = 0L
      var externalSteals = 0L

      // the local communication slave must know where to produce request
      // responses, this is a shared work queue to be consumed by this and
      // produced by the local slave
      slaveActor ! WorkQueue(remoteWorkQueue)

      /**
       * This callback is scheduled right after a remote stealing request, as
       * a retrying mechanism.
       */
      def requestExpirer: Unit = {
         remoteRequestSent = false
      }

      def terminationTimeout: Unit = {
         requestsBalance = 0
         logWarning (s"Terminating slave due termination timeout.")
      }

      /**
       * Try to steal work from remote workers. Additionally, it schedules a
       * request expirerer.
       */
      def handleRemoteSteal: Unit = ActorMessageSystem.akkaSysOpt match {
         case Some(akkaSys) =>
            if (remoteCancellable != null) {
               remoteCancellable.cancel()
            }

            // send steal work request, at some point the local work queue should be
            // populated with the response to this request
            slaveActor ! StealWork
            requestsBalance += 1
            remoteRequestSent = true

            remoteCancellable = akkaSys.scheduler.scheduleOnce(500 millis)(
               requestExpirer)(executionContext)

         case None =>
         // do nothing
      }

      var wastedIterations = 0L

      if (internalWsEnabled && !externalWsEnabled) {
         internalSteals = workStealingComputeLocal(c)
      } else {
         do {
            var _internalSteals = 0L
            if (internalWsEnabled) {
               _internalSteals = workStealingComputeLocal(c)
               internalSteals += _internalSteals
            }

            if (workStealed && _internalSteals == 0 && !remoteRequestSent) {
               handleRemoteSteal
            }

            val response = remoteWorkQueue.poll()
            if (response != null) {
               requestsBalance -= 1
               response.workOpt match {
                  case Some(subgraphBatchBytes) =>
                     val consumer = ActorMessageSystem.
                        deserializeSubgraphBatch(subgraphBatchBytes, c)
                     val computation = consumer.getComputation()

                     //val start = System.currentTimeMillis
                     //val numStealedWords = consumer.getWordIds().size()
                     //gtagExecutorActor ! Log(s"WorkStealedRemote" +
                     //  s" step=${c.getStep}" +
                     //  s" stealedByPartitionId=${computation.getPartitionId}" +
                     //  s" prefix=${consumer.getPrefix()}" +
                     //  s" numStealedWords=${numStealedWords}")

                     val ret = processCompute(consumer, computation)
                     callback(consumer, ret)
                     externalSteals += 1

                     //val end = System.currentTimeMillis
                     //gtagExecutorActor ! Log(s"WorkStealedAndProcessedRemote" +
                     //  s" step=${c.getStep}" +
                     //  s" stealedByPartitionId=${computation.getPartitionId}" +
                     //  s" prefix=${consumer.getPrefix()}" +
                     //  s" numStealedWords=${numStealedWords}" +
                     //  s" elapsed=${(end - start)}ms")

                     if (remoteCancellable != null) {
                        remoteCancellable.cancel()
                     }

                     remoteRequestSent = false

                  case None =>
                     if (remoteCancellable != null) {
                        remoteCancellable.cancel()
                     }

                     remoteRequestSent = false

                     if (response.numPeers == numPartitions) {
                        val msg = Log(s"AttemptFinishingExecutor" +
                           s" step=${c.getStep}" +
                           s" partitionId=${c.getPartitionId}" +
                           s" responseNumPeers=${response.numPeers}" +
                           s" numPartitionsTotal=${numPartitions}" +
                           s" requestsBalance=${requestsBalance}")
                        slaveActor ! msg
                        workStealed = false

                        // we setup a timeout to prevent this slave hanging undefinitely
                        ActorMessageSystem.akkaSysOpt match {
                           case Some(akkaSys) =>
                              remoteCancellable = akkaSys.scheduler.scheduleOnce(
                                 15 seconds)(terminationTimeout)(executionContext)
                           case None =>
                        }
                     }
               }
            } else {
               if (_internalSteals == 0) wastedIterations += 1
            }

         } while (workStealed || !remoteWorkQueue.isEmpty || requestsBalance > 0)

         if (remoteCancellable != null) {
            remoteCancellable.cancel()
         }
      }

      slaveActor ! Log(s"FinishingExecutor" +
         s" step=${c.getStep}" +
         s" partitionId=${c.getPartitionId}" +
         s" internalSteals=${internalSteals}" +
         s" externalSteals=${externalSteals}" +
         s" requestsBalance=${requestsBalance}" +
         s" numPartitionsTotal=${numPartitions}" +
         s" wastedIterations=${wastedIterations}")

   }

   private def workStealingComputeLocal(c: Computation[S]): Long = {
      val computations = SparkFromScratchEngine.localComputations[S](c.getStep())

      var internalSteals = 0L
      var _internalSteals = 0L
      var continue = remoteWorkQueue.isEmpty
      while (continue) {
         _internalSteals = 0L
         var i = 0
         while (i < computations.size() && continue) {
            val currComp = computations.get(i)
            if (currComp != null) {
               val consumer = currComp.forkEnumerator(c)
               if (consumer != null) {
                  val label = consumer.computationLabel()

                  // reach proper computation depth w.r.t. the stealed work
                  var curr = c
                  while (curr != null && curr.computationLabel() != label) {
                     curr = curr.nextComputation()
                  }

                  //assert (curr != null, s"label=${label} ${c}")

                  //val start = System.currentTimeMillis
                  //gtagExecutorActor ! Log(s"WorkStealedLocal step=${c.getStep}" +
                  //  s" stealedByPartitionId=${curr.getPartitionId}" +
                  //  s" stealedFromPartitionId=${currComp.getPartitionId}" +
                  //  s" computationPos=${i}/${computations.length}"
                  //  )

                  val ret = processCompute(consumer, curr)
                  callback(consumer, ret)
                  _internalSteals += 1

                  //val end = System.currentTimeMillis
                  //gtagExecutorActor ! Log(s"WorkStealedAndProcessedLocal" +
                  //  s" step=${c.getStep}" +
                  //  s" stealedByPartitionId=${curr.getPartitionId}" +
                  //  s" stealedFromPartitionId=${currComp.getPartitionId}" +
                  //  s" elapsed=${(end - start)}ms")
               }
            }
            continue = continue && remoteWorkQueue.isEmpty
            i += 1
         }
         internalSteals += _internalSteals
         continue = continue && _internalSteals > 0 && remoteWorkQueue.isEmpty
      }

      internalSteals
   }
}
