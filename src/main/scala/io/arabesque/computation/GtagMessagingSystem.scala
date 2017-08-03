package io.arabesque.computation

import akka.actor._
import akka.dispatch.MessageDispatcher
import akka.pattern.ask
import akka.util.Timeout

import com.typesafe.config.{Config, ConfigFactory}

import io.arabesque.embedding.Embedding
import io.arabesque.utils.Logging
import io.arabesque.utils.collection.IntArrayList

import java.io._
import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable.Set
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Random

/**
 * Gtag messages
 */
sealed trait GtagMessage

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
 * Gtag actor
 */
abstract class GtagActor(masterPath: String) extends Actor with Logging {
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
 * Gtag master
 */
class GtagMasterActor(masterPath: String, numExecutors: Int)
    extends GtagActor(masterPath) {

  override def gtagMasterRef: ActorRef = self

  override val peers: Set[ActorRef] = Set[ActorRef]()

  def receive = {
    case Hello(_peers) =>
      peers ++= _peers
      _peers.foreach (p => context.watch(p))
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

    case Reset =>
      peers.foreach(p => p ! PoisonPill)
      peers.clear
    
    case Terminated(p: ActorRef) =>
      logInfo(s"Removing ${p} from active peers")
      peers -= p

    case _ =>
  }
}

/**
 * Gtag executors
 */
class GtagExecutorActor [E <: Embedding] (
    partitionId: Int,
    computation: Computation[E],
    masterPath: String) extends GtagActor(masterPath) {
  
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
  
  def receive = identifying

  def identifying: Actor.Receive = {
    case ActorIdentity(`masterPath`, Some(actor)) =>
      _gtagMasterRef = actor
      context.watch(actor)
      context.become(active(actor))
      gtagMasterRef ! Hello(peers)
      logInfo(s"${self} knows master: ${gtagMasterRef}")

    case ActorIdentity(`masterPath`, None) =>
      logInfo(s"Remote actor not available: $masterPath")

    case ReceiveTimeout =>
      sendIdentifyRequest()

    case StealWork =>
      sender ! StealWorkResponse(None, peers.size)
      logInfo(s"${self} sending empty response to ${sender}." +
        s" Reason: actor is currently in the identification state")
    
    case StealWorkRequest(thief, remainingPeers, numPeers) =>
      thief ! StealWorkResponse(None, numPeers)
      logInfo(s"${self} sending empty response to ${thief}." +
        s" Reason: actor is currently in the identification state")

    case other =>
      logInfo(s"${self} is not ready. Ignoring message: ${other}.")
  }

  def active(actor: ActorRef): Actor.Receive = {
    case Hello(_peers) =>
      peers ++= (_peers.filter(p => p.path.name startsWith prefix))
      if (!peers.isEmpty) {
        logInfo(s"${self} knows ${peers.size} peers.")
      }

    case inMsg @ StealWork =>
      var remainingPeers = random.shuffle((peers - self).toSeq).toList

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
      logInfo(s"Master ${actor} terminated")
      sendIdentifyRequest()
      context.become(identifying)

    case ReceiveTimeout =>
    // ignore
  }

  private def sendIdentifyRequest(): Unit = {
    logInfo(s"${self} sending identification to master")
    context.actorSelection(masterPath) ! Identify(masterPath)
    import context.dispatcher
    context.system.scheduler.scheduleOnce(3 seconds, self, ReceiveTimeout)
  }
}

object GtagMessagingSystem extends Logging {
  private var _akkaSysOpt: Option[ActorSystem] = None

  private val nextActorId: AtomicInteger = new AtomicInteger(0)
  
  def getNextActorId: Int = nextActorId.getAndIncrement

  private val nextRequestId: AtomicInteger = new AtomicInteger(0)
  
  def getNextRequestId: Int = nextRequestId.getAndIncrement

  def akkaSysOpt: Option[ActorSystem] = _akkaSysOpt

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
    logInfo(s"Started akka-sys: ${as} - executor - waiting for messages")
    as
  }

  private lazy val _masterAkkaSys: ActorSystem = {
    val props = getDefaultProperties
    props.setProperty("akka.remote.netty.tcp.port", "2552")
    val as = ActorSystem("arabesque-gtag-system",
      config = Some(getAkkaConfig(props)))
    _akkaSysOpt = Option(as)
    logInfo(s"Started akka-sys: ${as} - master - waiting for messages")
    as
  }

  def akkaSys(engine: SparkGtagMasterEngine[_]): ActorSystem = synchronized {
    _masterAkkaSys
  }

  def akkaSys(engine: SparkGtagEngine[_]): ActorSystem = synchronized {
    _executorAkkaSys
  }

  def createActor(engine: SparkGtagMasterEngine[_]): ActorRef = {
    val remotePath = s"akka.tcp://arabesque-gtag-system@" +
      s"${engine.config.getMasterHostname}:2552" +
      s"/user/gtag-master-actor-${engine.config.getId}-${engine.superstep}"
    akkaSys(engine).actorOf(
      Props(classOf[GtagMasterActor], remotePath, engine.numPartitions).
        withDispatcher("akka.actor.default-dispatcher"),
      s"gtag-master-actor-${engine.config.getId}-${engine.superstep}")
  }

  def createActor [E <: Embedding] (engine: SparkGtagEngine[E]): ActorRef = {
    val remotePath = s"akka.tcp://arabesque-gtag-system@" +
      s"${engine.configuration.getMasterHostname}:2552" +
      s"/user/gtag-master-actor-${engine.configuration.getId}" +
      s"-${engine.superstep}"
    akkaSys(engine).actorOf(
      Props(classOf[GtagExecutorActor[E]],
        engine.partitionId, engine.computation, remotePath).
        withDispatcher("akka.actor.default-dispatcher"),
      s"gtag-executor-actor" +
      s"-${engine.configuration.getId}-${engine.superstep}" +
      s"-${engine.partitionId}-${getNextActorId}")
  }

  /**
   */
  def tryStealFromLocal [E <: Embedding] (c: Computation[E], random: Random)
    : Option[Array[Byte]] = {
    val computations = random.shuffle(
      SparkGtagEngine.activeComputations.
      filter { case ((s,p),_) =>
        s == c.getStep() && p == c.getPartitionId
      }.map(_._2).toSeq.asInstanceOf[Seq[Computation[E]]])

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
    asInstanceOf[SparkGtagEngine[E]].gtagActorRef
    val requestId = getNextRequestId
    logInfo (s"RequestSent step=${c.getStep} partitionsId=${c.getPartitionId}" +
      s" requestId=${requestId}")

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
      val wordIds = new IntArrayList(batchSize)
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
