package br.ufmg.cs.systems.fractal.computation

import java.io._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, ThreadLocalRandom}
import java.util.{Arrays, Comparator, Properties}

import akka.actor._
import akka.dispatch.MessageDispatcher
import akka.routing._
import br.ufmg.cs.systems.fractal.conf.Configuration
import br.ufmg.cs.systems.fractal.subgraph.Subgraph
import br.ufmg.cs.systems.fractal.util.Logging
import com.koloboke.collect.set.hash.HashIntSets
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable.Map
import scala.concurrent.duration._

/**
 * Message sent by the master at the end of each superstep
 */
case object Reset

/**
 * Message sent by slaves to the master for registering
 */
case class HelloMaster(partitionId: Int, slaveRef: ActorRef)

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
case object ReadyToFinish

/**
 * This message is sent externally to the slave, providing a shared queue for
 * consuming work. In this case, the response is the work to be consumed.
 */
case class WorkQueue(q: ConcurrentLinkedQueue[StealWorkResponse])

/**
 * Message passed between the actors participating in a work-stealing event.
 * This message is forwarded until some work could be stealed.
 */
case class StealWorkRequest(thief: ActorRef, nextPivot: Int, lastPivot: Int)

/**
 * Response message to the sender of a 'StealWork'. If available, the work will
 * be serialized as an array of bytes.
 */
case class StealWorkResponse(workOpt: Option[Array[Byte]], numPeers: Int)

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
case class Stats(validSubgraphs: Long)

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
class MasterActor(masterPath: String, numSlaves: Int)
    extends MSActor(masterPath) {

  override def masterRef: ActorRef = self

  private var slaves: Array[ActorRef] = new Array[ActorRef](numSlaves)

  private var slavesRouter: Router = new Router(new BroadcastRoutingLogic())

  private var registeredSlaves: Int = 0

  private var validSubgraphs: Long = 0

  private var nextReportValidSubgraphs: Long = 1000000

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

    case Log(msg) =>
      logInfo(msg)

    case Stats(_validSubgraphs) =>
      validSubgraphs += _validSubgraphs
      if (validSubgraphs >= nextReportValidSubgraphs) {
        logInfo(s"Stats(${masterPath}) validSubgraphs=${validSubgraphs}")
        nextReportValidSubgraphs += 1000000
      }

    case Reset =>
      slavesRouter.route(PoisonPill, self)
      slavesRouter = new Router(new BroadcastRoutingLogic())
      slaves = new Array[ActorRef](numSlaves)
      registeredSlaves = 0

    case Terminated(p: ActorRef) =>

    case _ =>
  }
}

/**
 * Slave actor
 */
class SlaveActor [E <: Subgraph](
    partitionId: Int,
    computation: Computation[E],
    masterPath: String) extends MSActor(masterPath) {

  private val config: Configuration[E] = computation.getConfig()

  private var _masterRef: ActorRef = _

  override def masterRef: ActorRef = _masterRef

  private var slaves: Array[ActorRef] = _

  private var pivots: Array[ActorRef] = _
  
  private var localPivotIdx: Int = -1

  private var outbox: ConcurrentLinkedQueue[StealWorkResponse] = _
  
  private var slavesRouter: Router = new Router(new BroadcastRoutingLogic())

  private var readyToFinishCount: Int = 0

  private val emptyResponse = StealWorkResponse(None, 0)

  private val infoPeriod = computation.getConfig().getInfoPeriod()
  
  private val externalWsEnabled = computation.getConfig().externalWsEnabled()

  val prefix: String = {
    val ExecutorPrefix = """slave-actor-(\d+)-(\d+)-(\d+)-(\d+)""".r
    self.path.name match {
      case ExecutorPrefix(configId, step, partitionId, actorId) =>
        s"slave-actor-${configId}-${step}"
      case other =>
        throw new RuntimeException(s"Invalid path for executor actor: ${other}")
    }
  }

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
      slavesRouter.route(ReadyToFinish, self)

    case ReadyToFinish =>
      readyToFinishCount += 1

    case StealWork =>
      sendEmptyResponse(self)

    case ActorIdentity(`masterPath`, None) =>
      logDebug(s"Remote actor not available: $masterPath")

    case ReceiveTimeout =>
      sendIdentifyRequest()

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
      slavesRouter.route(ReadyToFinish, self)

    case ReadyToFinish =>
      readyToFinishCount += 1

    case ReportStats =>
      reportStats()

    case inMsg @ StealWork =>
      if (slaves != null) {
        if (externalWsEnabled) {
          val numPivots = pivots.length
          val nextPivot = (localPivotIdx + 1) % numPivots
          val lastPivot = (localPivotIdx + numPivots - 1) % numPivots
          //val nextPivot = ThreadLocalRandom.current().nextInt(numPivots)
          //val lastPivot = (nextPivot + numPivots - 1) % numPivots
          val msg = StealWorkRequest(self, nextPivot, lastPivot)
          val aRef = pivots(nextPivot)
          aRef ! msg
          logDebug(s"${inMsg}: ${self} sending ${msg} to ${aRef}")
        } else {
          val msg = StealWorkResponse(None, readyToFinishCount)
          self ! msg
          logDebug(s"${inMsg}: ${self} sending ${msg} to ${self}")
        }
      } else {
        sendEmptyResponse(self)
      }

    case inMsg @ StealWorkRequest(
        thief, nextPivot, lastPivot) if nextPivot == lastPivot =>
      if (slaves != null) {
        val workOpt = ActorMessageSystem.tryStealFromLocal(computation)
        if (workOpt != null &&
            (workOpt.isDefined || readyToFinishCount == slaves.length)) {
          val msg = StealWorkResponse(workOpt, slaves.length)
          thief ! msg
          logDebug(s"${inMsg}: ${self} sending ${msg} to ${thief}")
        } else {
          sendEmptyResponse(thief)
        }
      } else {
        sendEmptyResponse(thief)
      }

    case inMsg @ StealWorkRequest(thief, thisPivot, lastPivot) =>
      if (slaves != null) {
        val workOpt = ActorMessageSystem.tryStealFromLocal(computation)
        if (workOpt != null && workOpt.isDefined) {
          val msg = StealWorkResponse(workOpt, slaves.length)
          thief ! msg
          logDebug(s"${inMsg}: ${self} sending ${msg} to ${thief}")
        } else {
          val numPivots = pivots.length
          val nextPivot = (thisPivot + 1) % numPivots
          val msg = StealWorkRequest(thief, nextPivot, lastPivot)
          val p = pivots(nextPivot)
          p ! msg
          logDebug(s"${inMsg}: ${self} forwarded ${msg} to ${p}")
        }
      } else {
        sendEmptyResponse(thief)
      }

    case inMsg: StealWorkResponse =>
      outbox.add(inMsg)

    case inMsg @ Log(msg) =>
      masterRef ! inMsg

    case Terminated(`actor`) =>
      sendIdentifyRequest()
      context.become(identifying)
      logInfo(s"Master ${actor} terminated")

    case ReceiveTimeout =>
      // ignore
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

  private def reportStats(): Unit = {
    val engine = computation.getExecutionEngine().
      asInstanceOf[SparkFromScratchEngine[E]]

    masterRef ! Log(
      s"StatsReport{" +
      s"step=${computation.getStep}," +
      s"partitionId=${computation.getPartitionId}," +
      s"${engine.getStatsAccumulators}," +
      reportMemoryStats() + "}")
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
    logInfo(s"SlaveActor stopped step=${computation.getStep}" +
      s" partitionId=${partitionId} actor=${self}")
    computation.getConfig().taskCheckOut()
    SparkFromScratchEngine.unregisterComputation(computation)
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
      Props(classOf[MasterActor], remotePath, engine.numPartitions).
        withDispatcher("akka.actor.default-dispatcher"),
      s"master-actor-${engine.config.getId}-${engine.step}")
  }

  def createActor [E <: Subgraph] (engine: SparkEngine[E]): ActorRef = {
    val remotePath = s"akka.tcp://fractal-msgsys@" +
      s"${engine.configuration.getMasterHostname}:2552" +
      s"/user/master-actor-${engine.configuration.getId}" +
      s"-${engine.step}"
    val slaveActorRef = akkaSys(engine).actorOf(
      Props(classOf[SlaveActor[E]],
        engine.partitionId, engine.computation, remotePath).
        withDispatcher("akka.actor.default-dispatcher"),
      s"slave-actor" +
        s"-${engine.configuration.getId}-${engine.step}" +
        s"-${engine.partitionId}-${getNextActorId}")

    // wait for this slave to know the all other slaves before proceed
    slaveActorRef.synchronized {
      while (engine.configuration.taskCounter() == 0) {
        slaveActorRef.wait()
      }
    }

    // ensure that local computation structures are properly created
    SparkFromScratchEngine.createComputationsMap(
      engine.step, engine.configuration.taskCounter())

    slaveActorRef
  }

  /**
   */
  def tryStealFromLocal [E <: Subgraph] (
      c: Computation[E]): Option[Array[Byte]] = {
    val computations = SparkFromScratchEngine.localComputations[E](c.getStep())
    val numComputations = computations.length

    val gtagBatchLow = c.getConfig().getWsBatchSizeLow()
    val gtagBatchHigh = c.getConfig().getWsBatchSizeHigh()
    val batchSize = ThreadLocalRandom.current().nextInt(
      gtagBatchHigh - gtagBatchLow + 1) + gtagBatchLow

    var missingComputation = false

    var i = ThreadLocalRandom.current().nextInt(numComputations)
    val offset = i + numComputations
    while (i < offset) {
      val currComp = computations(i % numComputations)
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
        oos.writeInt(prefix.getUnchecked(i))
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


      currIterator.set(curr, subgraph)
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
    gtagExecutorActor: ActorRef,
    remoteWorkQueue: ConcurrentLinkedQueue[StealWorkResponse],
    callback: (SubgraphEnumerator[S], Long) => Unit =
      (e: SubgraphEnumerator[S], ret: Long) => {}) extends Logging {

  def workStealingCompute(c: Computation[S]): Unit = {
    implicit val executionContext = ActorMessageSystem.akkaExecutorContext
    var workStealed = true
    @volatile var remoteRequestSent = false
    var requestsBalance = 0
    var remoteCancellable = null.asInstanceOf[Cancellable]
    val internalWsEnabled = c.getConfig().internalWsEnabled()
    val externalWsEnabled = c.getConfig().externalWsEnabled()
    var internalSteals = 0L
    var externalSteals = 0L

    // the local communication slave must know where to produce request
    // responses, this is a shared work queue to be consumed by this and
    // produced by the local slave
    gtagExecutorActor ! WorkQueue(remoteWorkQueue)

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
        gtagExecutorActor ! StealWork
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

              if (response.numPeers == c.getNumberPartitions) {
                gtagExecutorActor ! Log(s"AttemptFinishingExecutor" +
                  s" step=${c.getStep}" +
                  s" partitionId=${c.getPartitionId}" +
                  s" responseNumPeers=${response.numPeers}" +
                  s" numPartitionsTotal=${c.getNumberPartitions}" +
                  s" requestsBalance=${requestsBalance}")
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

    gtagExecutorActor ! Log(s"FinishingExecutor" +
      s" step=${c.getStep}" +
      s" partitionId=${c.getPartitionId}" +
      s" internalSteals=${internalSteals}" +
      s" externalSteals=${externalSteals}" +
      s" requestsBalance=${requestsBalance}" +
      s" numPartitionsTotal=${c.getNumberPartitions}" + 
      s" wastedIterations=${wastedIterations}")

  }

  private def workStealingComputeLocal(c: Computation[S]): Long = {
    val computations = SparkFromScratchEngine.localComputations[S](c.getStep())

    var internalSteals = 0L
    var _internalSteals = 0L
    do {
      _internalSteals = 0L
      var i = 0
      while (i < computations.length) {
        val currComp = computations(i)
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
        i += 1
      }
      internalSteals += _internalSteals
    } while (_internalSteals > 0)

    internalSteals
  }
}
