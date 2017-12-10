package io.arabesque.computation

import java.io._
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Properties, Iterator => JavaIterator}

import akka.actor._
import akka.dispatch.MessageDispatcher
import akka.pattern.ask
import akka.util.Timeout
import com.koloboke.collect.set.hash.HashIntSets
import com.typesafe.config.{Config, ConfigFactory}
import io.arabesque.conf.Configuration
import io.arabesque.embedding.Embedding
import io.arabesque.utils.Logging
import io.arabesque.utils.collection.IntArrayList
import io.arabesque.utils.pool.HashIntSetPool

import scala.collection.JavaConversions._
import scala.collection.mutable.Set
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success}

/**
 * Message sent by the master at the end of each superstep
 */
case object Reset

/**
 * Message sent by the master and executors to spread actor references of the
 * peers participating in the communication
 */
case class Hello(peers: Set[ActorRef])

/**
 * Message sent to the local gtag actor to start a remote work-stealing event
 */
case object StealWork

/**
 * Message passed between the actors participating in a work-stealing event.
 * This message is forwarded until some work could be stealed.
 */
case class StealWorkRequest(thief: ActorRef,
  remainingPeers: List[ActorRef], numPeers: Int)

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
case class Stats(validEmbeddings: Long)

/**
 * Gtag actor
 */
abstract class MSActor(masterPath: String) extends Actor with Logging {
  /**
   * Akka reference of the master. Master here is used as a rendevouz point to
   * spread actor references between peers.
   */
  def gtagMasterRef: ActorRef

  /**
   * Peers known by this actor.
   */
  def peers: Set[ActorRef]

  logInfo(s"Actor ${self} started")
}

/**
 * Star master actor
 */
class MasterActor(masterPath: String, numExecutors: Int)
    extends MSActor(masterPath) {

  override def gtagMasterRef: ActorRef = self

  override val peers: Set[ActorRef] = Set[ActorRef]()

  var validEmbeddings: Long = 0
  var nextReportValidEmbeddings: Long = 1000000

  def receive = {
    case Hello(_peers) =>
      peers ++= _peers
      // _peers.foreach (p => context.watch(p))

      if (peers.size >= numExecutors) {
        val helloMsg = Hello(peers)
        peers.foreach (p => p ! helloMsg)
        logInfo(s"Publishing ${numExecutors} peers.")
      }

      if (!peers.isEmpty) {
        logInfo(s"${self} knows ${peers.size} peers.")
      }

    case Log(msg) =>
      logInfo(msg)

    case Stats(_validEmbeddings) =>
      validEmbeddings += _validEmbeddings
      if (validEmbeddings >= nextReportValidEmbeddings) {
        logInfo(s"Stats(${masterPath}) validEmbeddings=${validEmbeddings}")
        nextReportValidEmbeddings += 1000000
      }

    case Reset =>
      peers.foreach(p => p ! PoisonPill)
      peers.clear

    case Terminated(p: ActorRef) =>
      logInfo(s"Removing ${p} from active peers")
      // context.unwatch(p)
      peers -= p

    case _ =>
  }
}

/**
 * Gtag executors
 */
class SlaveActor [E <: Embedding](
    partitionId: Int,
    computation: Computation[E],
    masterPath: String) extends MSActor(masterPath) {

  private var _gtagMasterRef: ActorRef = _

  override def gtagMasterRef: ActorRef = _gtagMasterRef

  override val peers: Set[ActorRef] = Set[ActorRef](self)

  val prefix: String = {
    val ExecutorPrefix = """gtag-executor-actor-(\d+)-(\d+)-(\d+)-(\d+)""".r
    self.path.name match {
      case ExecutorPrefix(configId, step, partitionId, actorId) =>
        s"gtag-executor-actor-${configId}-${step}"
      case other =>
        throw new RuntimeException(s"Invalid path for executor actor: ${other}")
    }
  }

  private val random = new Random(partitionId)

  sendIdentifyRequest()
  reportStats()
  // printStats()

  def receive = identifying

  def identifying: Actor.Receive = {
    case ActorIdentity(`masterPath`, Some(actor)) =>
      _gtagMasterRef = actor
      // context.watch(actor)
      context.become(active(actor))
      gtagMasterRef ! Hello(peers)
      logInfo(s"${self} knows master: ${gtagMasterRef}")

    case ActorIdentity(`masterPath`, None) =>
      logDebug(s"Remote actor not available: $masterPath")

    case ReceiveTimeout =>
      sendIdentifyRequest()

    case StealWork =>
      sender ! StealWorkResponse(None, peers.size)
      logDebug(s"${self} sending empty response to ${sender}." +
        s" Reason: actor is currently in the identification state")

    case StealWorkRequest(thief, remainingPeers, numPeers) =>
      thief ! StealWorkResponse(None, numPeers)
      logDebug(s"${self} sending empty response to ${thief}." +
        s" Reason: actor is currently in the identification state")

    case other =>
      logDebug(s"${self} is not ready. Ignoring message: ${other}.")
  }

  def active(actor: ActorRef): Actor.Receive = {
    case Hello(_peers) =>
      peers ++= (_peers.filter(p => p.path.name startsWith prefix))
      if (!peers.isEmpty) {
        val numLocalPeers = peers.filter (
          p => p.path.address.equals(self.path.address)).size()
        computation.getConfig().taskCheckIn(0, numLocalPeers)
        logInfo(s"${self} knows ${peers.size} peers (${numLocalPeers} local).")
        self.synchronized {
          self.notify()
        }
      }

    case ReportStats =>
      val atomicValidEmbeddings = computation.getExecutionEngine().
        asInstanceOf[SparkFromScratchEngine[E]].validEmbeddings

      if (atomicValidEmbeddings != null) {
        val validEmbeddings = atomicValidEmbeddings.get()
        gtagMasterRef ! Stats(validEmbeddings)
        atomicValidEmbeddings.addAndGet(-validEmbeddings)
      }

    case inMsg @ StealWork =>
      //var remainingPeers = random.shuffle((peers - self).toSeq).toList
      var remainingPeers = random.shuffle(
        (peers - self).
          filter (p => !p.path.address.equals(self.path.address)).toSeq).toList

      if (!remainingPeers.isEmpty) {
        val aRef = remainingPeers.head
        remainingPeers = remainingPeers.tail
        val msg = StealWorkRequest(sender(), remainingPeers, peers.size)
        aRef ! msg
        logDebug(s"${inMsg}: ${self} sending ${msg} to ${aRef}. ")
      } else {
        val msg = StealWorkResponse(None, peers.size)
        sender ! msg
        logDebug(s"${inMsg}: ${self} sending ${msg} to ${sender}")
      }

    case inMsg @ StealWorkRequest(thief, List(), numPeers) =>
      val msg = StealWorkResponse(
        GtagMessagingSystem.tryStealFromLocal(computation, random), numPeers)
      thief ! msg
      logDebug(s"${inMsg}: ${self} sending ${msg} to ${thief}")

    case inMsg @ StealWorkRequest(thief, p :: remainingPeers, numPeers) =>
      val workOpt = GtagMessagingSystem.tryStealFromLocal(computation, random)
      if (workOpt.isDefined) {
        val msg = StealWorkResponse(workOpt, numPeers)
        thief ! msg
        logDebug(s"${inMsg}: ${self} sending ${msg} to ${thief}")
      } else {
        val msg = StealWorkRequest(thief, remainingPeers, numPeers)
        p ! msg
        logDebug(s"${inMsg}: ${self} forwarded ${msg} to ${p}")
      }

    case inMsg @ Log(msg) =>
      gtagMasterRef ! inMsg

    case Terminated(`actor`) =>
      // context.unwatch(actor)
      sendIdentifyRequest()
      context.become(identifying)
      logInfo(s"Master ${actor} terminated")

    case ReceiveTimeout =>
    // ignore
  }

  private def sendIdentifyRequest(): Unit = {
    logInfo(s"${self} sending identification to master")
    context.actorSelection(masterPath) ! Identify(masterPath)
    import context.dispatcher
    context.system.scheduler.scheduleOnce(3 seconds, self, ReceiveTimeout)
  }

  private def reportStats(): Unit = {
    import context.dispatcher
    context.system.scheduler.schedule(3 seconds, 3 seconds, self, ReportStats)
  }

  override def postStop(): Unit = {
    logInfo(s"SlaveActor stopped step=${computation.getStep}" +
      s" partitionId=${partitionId} actor=${self}")
    computation.getConfig().taskCheckOut()
  }


  private def printStats(): Unit = {
    val unit = Configuration.GB
    def scale(n: Double): Double = n / unit
    val runtime = Runtime.getRuntime()
    val maxMemory = runtime.maxMemory()
    val totalMemory = runtime.totalMemory()
    val freeMemory = runtime.freeMemory()
    val usedMemory = totalMemory - freeMemory
    val step = computation.getStep()

    logInfo (s"MemoryStats(GB) step=${step}" +
      s" partitionId=${partitionId}" +
      s" maxMemory=${scale(maxMemory)} totalMemory=${scale(totalMemory)}" +
      s" freeMemory=${scale(freeMemory)} usedMemory=${scale(usedMemory)}")

    import context.dispatcher
    context.system.scheduler.scheduleOnce(3 seconds, self, ReceiveTimeout)
  }
}

object GtagMessagingSystem extends Logging {
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
    props.setProperty("akka.remote.netty.tcp.port", "0")
    val as = ActorSystem("arabesque-gtag-system",
      config = Some(getAkkaConfig(props)))
    _akkaSysOpt = Option(as)
    _executorAkkaSysOpt = Option(as)
    logInfo(s"Started akka-sys: ${as} - executor - waiting for messages")
    as
  }

  private lazy val _masterAkkaSys: ActorSystem = {
    val props = getDefaultProperties
    props.setProperty("akka.remote.netty.tcp.port", "2552")
    val as = ActorSystem("arabesque-gtag-system",
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
    val remotePath = s"akka.tcp://arabesque-gtag-system@" +
      s"${engine.config.getMasterHostname}:2552" +
      s"/user/gtag-master-actor-${engine.config.getId}-${engine.superstep}"
    akkaSys(engine).actorOf(
      Props(classOf[MasterActor], remotePath, engine.numPartitions).
        withDispatcher("akka.actor.default-dispatcher"),
      s"gtag-master-actor-${engine.config.getId}-${engine.superstep}")
  }

  def createActor [E <: Embedding] (engine: SparkEngine[E]): ActorRef = {
    val remotePath = s"akka.tcp://arabesque-gtag-system@" +
      s"${engine.configuration.getMasterHostname}:2552" +
      s"/user/gtag-master-actor-${engine.configuration.getId}" +
      s"-${engine.superstep}"
    val slaveActorRef = akkaSys(engine).actorOf(
      Props(classOf[SlaveActor[E]],
        engine.partitionId, engine.computation, remotePath).
        withDispatcher("akka.actor.default-dispatcher"),
      s"gtag-executor-actor" +
      s"-${engine.configuration.getId}-${engine.superstep}" +
      s"-${engine.partitionId}-${getNextActorId}")
    slaveActorRef.synchronized {
      while (engine.configuration.taskCounter() == 0) {
        slaveActorRef.wait()
      }
    }
    slaveActorRef
  }

  /**
   */
  def tryStealFromLocal [E <: Embedding] (c: Computation[E], random: Random)
    : Option[Array[Byte]] = {
    val computations = random.shuffle(
      SparkFromScratchEngine.activeComputations.
      filter { case ((s,p),_) =>
        s == c.getStep() && p == c.getPartitionId
      }.map(_._2).toSeq.asInstanceOf[Seq[Computation[E]]])

    assert (computations.length <= 1)

    val gtagBatchLow = c.getConfig().getGtagBatchSizeLow()
    val gtagBatchHigh = c.getConfig().getGtagBatchSizeHigh()
    val batchSize = random.nextInt(
      gtagBatchHigh - gtagBatchLow + 1) + gtagBatchLow

    var i = 0
    while (i < computations.length) {
      val currComp = computations(i)
      val consumer = currComp.forkConsumer()
      if (consumer != null) {
        val ebytesOpt = serializeEmbeddingBatch(consumer, batchSize)
        currComp.joinConsumer(consumer)
        return ebytesOpt
      }
      i += 1
    }

    None
  }

  /**
   */
  def serializeEmbeddingBatch [E <: Embedding] (
      consumer: EmbeddingIterator[E],
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

  def tryStealWorkFromRemote [E <: Embedding] (c: Computation[E])
    : (Int,Future[_]) = {
    val gtagActorRef = c.getExecutionEngine().
    asInstanceOf[SparkFromScratchEngine[E]].gtagActorRef
    val requestId = getNextRequestId
    //logInfo (s"RequestSent step=${c.getStep} partitionsId=${c.getPartitionId}" +
    //  s" requestId=${requestId}")

    implicit val executionContext = akkaExecutorContext
    implicit val timeout = Timeout(15 seconds)
    (requestId, gtagActorRef ? StealWork)
  }

  def deserializeEmbeddingBatch [E <: Embedding] (buf: Array[Byte],
        c: Computation[E]): EmbeddingIterator[E] = {
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
      assert (curr != null, s"label=${label} ${c}")

      // prefix
      val prefixSize = ois.readInt()
      val embedding = c.getConfig().createEmbedding[E]
      var i = 0
      while (i < prefixSize) {
        embedding.addWord(ois.readInt())
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

      val currIterator = curr.asInstanceOf[BasicComputation[E]].
        getEmbeddingIterator()
       
      currIterator.set(curr, embedding, wordIds)

    } finally {
      ois.close
    }
  }
}

class WorkStealingSystem [E <: Embedding] (
    processCompute: (JavaIterator[E],Computation[E]) => Long,
    gtagExecutorActor: ActorRef,
    remoteWorkQueue: ConcurrentLinkedQueue[StealWorkResponse],
    callback: (EmbeddingIterator[E], Long) => Unit =
      (e: EmbeddingIterator[E], ret: Long) => {}) {

  def workStealingCompute(c: Computation[E]): Unit = {
    implicit val executionContext = GtagMessagingSystem.akkaExecutorContext
    implicit val timeout = Timeout(15 seconds)
    var workStealed = true
    @volatile var remoteRequestSent = false
    var remoteCancellable = null.asInstanceOf[Cancellable]

    /**
     * This callback is scheduled right after a remote stealing request, as
     * a retrying mechanism.
     */
    def requestExpirer: Unit = {
      remoteRequestSent = false
    }

    /**
     * Try to steal work from remote workers. Additionally, it schedules a
     * request expirerer.
     */
    def handleRemoteSteal: Unit = GtagMessagingSystem.akkaSysOpt match {
      case Some(akkaSys) =>
        if (remoteCancellable != null) {
          remoteCancellable.cancel()
        }

        val (requestId, requestFuture) =
          GtagMessagingSystem.tryStealWorkFromRemote(c)
        requestFuture.onComplete {
          case Success(response: StealWorkResponse) =>
            assert (remoteWorkQueue.add(response))

          case Success(response) =>
            throw new RuntimeException(s"Unknown response: ${response}")

          case Failure(e) =>
            throw e
        }
        remoteRequestSent = true

        remoteCancellable = akkaSys.scheduler.scheduleOnce(15 seconds)(
          requestExpirer)(executionContext)

        case None =>
          // do nothing
    }

    do {
      val workStealedLocally = workStealingComputeLocal(c)

      if (!workStealedLocally && !remoteRequestSent) {
        handleRemoteSteal
      }

      val response = remoteWorkQueue.poll()
      if (response != null) {
        response.workOpt match {
          case Some(embeddingBatchBytes) =>
            val consumer = GtagMessagingSystem.
            deserializeEmbeddingBatch(embeddingBatchBytes, c)
            val computation = consumer.getComputation()

            val start = System.currentTimeMillis
            val numStealedWords = consumer.getWordIds().size()
            gtagExecutorActor ! Log(s"WorkStealedRemote" +
              s" step=${c.getStep}" +
              s" stealedByPartitionId=${computation.getPartitionId}" +
              s" prefix=${consumer.getPrefix()}" +
              s" numStealedWords=${numStealedWords}")

            val ret = processCompute(consumer, computation)
            callback(consumer, ret)

            val end = System.currentTimeMillis
            gtagExecutorActor ! Log(s"WorkStealedAndProcessedRemote" +
              s" step=${c.getStep}" +
              s" stealedByPartitionId=${computation.getPartitionId}" +
              s" prefix=${consumer.getPrefix()}" +
              s" numStealedWords=${numStealedWords}" +
              s" elapsed=${(end - start)}ms")

            if (remoteCancellable != null) {
              remoteCancellable.cancel()
            }

            remoteRequestSent = false

          case None =>
            if (response.numPeers == c.getNumberPartitions) {
              gtagExecutorActor ! Log(s"AttemptFinishingExecutor" +
                s" step=${c.getStep}" +
                s" partitionId=${c.getPartitionId}" +
                s" responseNumPeers=${response.numPeers}" +
                s" numPartitionsTotal=${c.getNumberPartitions}")
              workStealed = false
            }
            if (remoteCancellable != null) {
              remoteCancellable.cancel()
            }
            remoteRequestSent = false
        }
      }
    } while (workStealed || !remoteWorkQueue.isEmpty)

    gtagExecutorActor ! Log(s"FinishingExecutor" +
      s" step=${c.getStep}" +
      s" partitionId=${c.getPartitionId}" +
      s" numPartitionsTotal=${c.getNumberPartitions}")
  }

  private def workStealingComputeLocal(c: Computation[E]): Boolean = {
    val random = new Random(c.getPartitionId)
    val computations = random.shuffle(
      SparkFromScratchEngine.activeComputations.filter {
        case ((s,p),_) => s == c.getStep() && p != c.getPartitionId
      }.map(_._2).toSeq.asInstanceOf[Seq[Computation[E]]]
    )

    var stealed = false
    var i = 0
    while (i < computations.length) {
      val currComp = computations(i)
      val consumer = currComp.forkConsumer()
      if (consumer != null) {
        stealed = true
        val label = consumer.computationLabel()

        // reach proper computation depth w.r.t. the stealed work
        var curr = c
        while (curr != null && curr.computationLabel() != label) {
          curr = curr.nextComputation()
        }

        assert (curr != null, s"label=${label} ${c}")

        val start = System.currentTimeMillis
        //gtagExecutorActor ! Log(s"WorkStealedLocal step=${c.getStep}" +
        //  s" stealedByPartitionId=${curr.getPartitionId}" +
        //  s" stealedFromPartitionId=${currComp.getPartitionId}" +
        //  s" computationPos=${i}/${computations.length}"
        //  )

        val ret = processCompute(consumer, curr)
        callback(consumer, ret)

        val end = System.currentTimeMillis
        //gtagExecutorActor ! Log(s"WorkStealedAndProcessedLocal" +
        //  s" step=${c.getStep}" +
        //  s" stealedByPartitionId=${curr.getPartitionId}" +
        //  s" stealedFromPartitionId=${currComp.getPartitionId}" +
        //  s" elapsed=${(end - start)}ms")

        currComp.joinConsumer(consumer)
      }
      i += 1
    }

    // stealed || computations.length < c.getNumberPartitions() - 1
    stealed
  }
}
