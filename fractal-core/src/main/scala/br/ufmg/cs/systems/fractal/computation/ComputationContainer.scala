package br.ufmg.cs.systems.fractal.computation

import java.util.concurrent.atomic.AtomicInteger

import br.ufmg.cs.systems.fractal.Primitive
import br.ufmg.cs.systems.fractal.conf.Configuration
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.{Logging, WordFilterFunc}
import com.koloboke.collect.IntCollection

import scala.collection.mutable.Stack

// TODO: Refactor me!
sealed trait ComputationContainer [E <: Subgraph] extends Computation[E]
    with Logging {

  val callerSite: String = {
    val calls = Thread.currentThread().getStackTrace()
    def findCaller: String = {
      var i = 0
      while (i < calls.length) {
        if (calls(i).getClassName equals "br.ufmg.cs.systems.fractal.fractalResult") {
          return calls(i).getMethodName
        }
        i += 1
      }
      null
    }
    findCaller
  }

  val containerId: Int = ComputationContainer.nextContainerId.getAndIncrement

  val primitiveOpt: Option[Primitive]

  val computationLabelOpt: Option[String]

  val patternOpt: Option[Pattern]

  val processOpt: Option[(E,Computation[E]) => Unit]

  val filterOpt: Option[(E,Computation[E]) => Boolean]
  
  val wordFilterOpt: Option[WordFilterFunc[E]]

  val getPossibleExtensionsOpt: Option[(E,Computation[E]) => IntCollection]

  val initOpt: Option[(Computation[E]) => Unit]

  val initAggregationsOpt: Option[(Computation[E]) => Unit]

  val finishOpt: Option[(Computation[E]) => Unit]

  val expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]]

  val shouldBypassOpt: Option[Boolean]
  
  val processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long]

  val nextComputationOpt: Option[Computation[E]]
 
  @transient lazy val lastComputation: ComputationContainer[E] = {
    nextComputationOpt match {
      case Some(nextComputation: ComputationContainer[E]) =>
        nextComputation.lastComputation
      case _ =>
        this
    }
  }

  def withComputationAppended(lastComputation: Computation[E])
    : ComputationContainer[E]

  def withPrimitive(p: Primitive): ComputationContainer[E]
  
  def withComputationLabel(label: String): ComputationContainer[E]

  def asLastComputation: ComputationContainer[E]
  
  def take(n: Int): ComputationContainer[E]

  def shallowCopy(
      primitiveOpt: Option[Primitive] =
        primitiveOpt,
      computationLabelOpt: Option[String] =
        computationLabelOpt,
      patternOpt: Option[Pattern] =
        patternOpt,
      processOpt: Option[(E,Computation[E]) => Unit] =
        processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] =
        filterOpt,
      wordFilterOpt: Option[WordFilterFunc[E]] =
        wordFilterOpt,
      getPossibleExtensionsOpt: Option[(E,Computation[E]) => IntCollection] =
        getPossibleExtensionsOpt,
      initOpt: Option[(Computation[E]) => Unit] =
        initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] =
        initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] =
        finishOpt,
      expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] =
        expandComputeOpt,
      shouldBypassOpt: Option[Boolean] =
        shouldBypassOpt,
      processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
        processComputeOpt,
      nextComputationOpt: Option[Computation[E]] =
        nextComputationOpt
    ): ComputationContainer[E]

  def withNewFunctions(
      primitiveOpt: Option[Primitive] =
        primitiveOpt,
      computationLabelOpt: Option[String] =
        computationLabelOpt,
      patternOpt: Option[Pattern] =
        patternOpt,
      processOpt: Option[(E,Computation[E]) => Unit] =
        processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] =
        filterOpt,
      wordFilterOpt: Option[WordFilterFunc[E]] =
        wordFilterOpt,
      getPossibleExtensionsOpt: Option[(E,Computation[E]) => IntCollection] =
        getPossibleExtensionsOpt,
      initOpt: Option[(Computation[E]) => Unit] =
        initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] =
        initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] =
        finishOpt,
      expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] =
        expandComputeOpt,
      shouldBypassOpt: Option[Boolean] =
        shouldBypassOpt,
      processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
        processComputeOpt
    ): ComputationContainer[E]

  def withNewFunctionsAll(
      primitiveOpt: Option[Primitive] =
        primitiveOpt,
      computationLabelOpt: Option[String] =
        computationLabelOpt,
      patternOpt: Option[Pattern] =
        patternOpt,
      processOpt: Option[(E,Computation[E]) => Unit] =
        processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] =
        filterOpt,
      wordFilterOpt: Option[WordFilterFunc[E]] =
        wordFilterOpt,
      getPossibleExtensionsOpt: Option[(E,Computation[E]) => IntCollection] =
        getPossibleExtensionsOpt,
      initOpt: Option[(Computation[E]) => Unit] =
        initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] =
        initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] =
        finishOpt,
      expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] =
        expandComputeOpt,
      shouldBypassOpt: Option[Boolean] =
        shouldBypassOpt,
      processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
        processComputeOpt
    ): ComputationContainer[E]

  def shallowCopy(): ComputationContainer[E]
  
  def clear(): ComputationContainer[E] = withNewFunctions (
    primitiveOpt = None,
    computationLabelOpt = None,
    patternOpt = None,
    processOpt = Some((e,c) => {}),
    filterOpt = Some((e,c) => true),
    getPossibleExtensionsOpt = None,
    //expandComputeOpt = Some((e,c) => Iterator.empty)
    expandComputeOpt = None
    )

  @transient lazy val computationRepr: Array[String] = {
    Array(
      shouldBypassOpt.map(b => s"bypass=${b}").getOrElse("bypass=false"),
      expandComputeOpt.map(_ => "ec").getOrElse("_"),
      getPossibleExtensionsOpt.map(_ => "ex").getOrElse("_"),
      wordFilterOpt.map(_ => "wf").getOrElse("_"),
      filterOpt.map(_ => "f").getOrElse("_"),
      processComputeOpt.map(_ => "pc").getOrElse("_"),
      processOpt.map(_ => "p").getOrElse("_"))
  }

  override def toString: String = {
    //s"${primitiveOpt.getOrElse(Primitive.None).name()}" +
    //s"${nextComputationOpt.map(c => c.toString).getOrElse("")}"
    s" CC[${containerId}]" +
    s"[${computationLabel()}]" +
    s"(${computationRepr.mkString(",")})" +
    s"${nextComputationOpt.map(c => "::" + c.toString).getOrElse("")}"
  }
}

case class EComputationContainer [E <: EdgeInducedSubgraph] (
    primitiveOpt: Option[Primitive] = None,
    computationLabelOpt: Option[String] = None,
    patternOpt: Option[Pattern] = None,
    processOpt: Option[(E,Computation[E]) => Unit] = None,
    filterOpt: Option[(E,Computation[E]) => Boolean] = None,
    wordFilterOpt: Option[WordFilterFunc[E]] = None,
    getPossibleExtensionsOpt: Option[(E,Computation[E]) => IntCollection] = None,
    initOpt: Option[(Computation[E]) => Unit] = None,
    initAggregationsOpt: Option[(Computation[E]) => Unit] = None,
    finishOpt: Option[(Computation[E]) => Unit] = None,
    expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] = None,
    shouldBypassOpt: Option[Boolean] = None,
    processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
      None,
    nextComputationOpt: Option[Computation[E]] = None)
  extends EdgeInducedComputation[E] with ComputationContainer[E] {
  
  private val pconfigOpt: Option[Configuration[E]] =
    patternOpt.map(_.getConfig().asInstanceOf[Configuration[E]])

  @transient private lazy val _computationLabel: String =
    computationLabelOpt.getOrElse (containerId.toString)

  @transient private lazy val _primitive: Primitive =
    primitiveOpt.getOrElse (Primitive.None)
  
  @transient private lazy val _pattern: Pattern =
    patternOpt.getOrElse(null)

  @transient private lazy val _process: (E,Computation[E]) => Unit =
    processOpt.getOrElse ((e: E, c: Computation[E]) => super.process (e))
  
  @transient private lazy val _filter: (E,Computation[E]) => Boolean =
    filterOpt.getOrElse ((e: E, c: Computation[E]) => super.filter (e))
  
  @transient private lazy val _wordFilter: WordFilterFunc[E] = {
    wordFilterOpt.getOrElse (
      new WordFilterFunc [E] {
        def apply(e: E, w: Int, c: Computation[E]): Boolean = {
          e.isCanonicalSubgraphWithWord(w)
        }
      }
    )
  }

  @transient private lazy val _getPossibleExtensions: (E,Computation[E]) => IntCollection =
    getPossibleExtensionsOpt.getOrElse (
      (e: E, c: Computation[E]) => super.getPossibleExtensions (e)
    )
  
  @transient private lazy val _init
    : (Configuration[E], Computation[E]) => Unit = initOpt match {

    case Some(thisInit) =>
      (config: Configuration[E], c: Computation[E]) => {
        super.init(config)
        thisInit(c)
      }

    case None =>
      (config:Configuration[E], c: Computation[E]) => {
        super.init(config)
      }
  }

  @transient private lazy val _initAggregations
    : (Configuration[E], Computation[E]) => Unit = {
    initAggregationsOpt match {
      case Some(thisInitAggregations) =>
        (config: Configuration[E], c: Computation[E]) => {
          super.initAggregations(config)
          thisInitAggregations(c)
        }

      case None =>
        (config: Configuration[E], c: Computation[E]) => {
          super.initAggregations(config)
        }
    }
  }

  @transient private lazy val _finish
    : (Computation[E]) => Unit = finishOpt match {
    case Some(thisFinish) =>
      (c: Computation[E]) => {super.finish(); thisFinish(c)}
    case None =>
      (c: Computation[E]) => {super.finish()}
  }

  @transient private lazy val _expandCompute
    : (E,Computation[E]) => SubgraphEnumerator[E] =
    expandComputeOpt.getOrElse (
      (e: E, c: Computation[E]) => super.expandCompute(e)
    )

  @transient private lazy val _shouldBypass: Boolean = {
    shouldBypassOpt.getOrElse(super.shouldBypass())
  }

  @transient private lazy val _processCompute
    : (SubgraphEnumerator[E],Computation[E]) => Long =
    processComputeOpt.getOrElse (
      (iter: SubgraphEnumerator[E], c: Computation[E]) => -1
    )

  @transient private lazy val _nextComputation
    : Computation[E] = nextComputationOpt match {
    case Some(nc) => nc
    case None => null
  }

  def shallowCopy(): ComputationContainer[E] = nextComputationOpt match {
    case Some(nextComputation : ComputationContainer[E]) =>
      this.copy(
        primitiveOpt =
          Option(primitiveOpt.getOrElse(Primitive.None)),
        computationLabelOpt =
          Option(computationLabelOpt.getOrElse(containerId.toString)),
        nextComputationOpt = Option(nextComputation.shallowCopy()))
    case None =>
      this.copy(
        primitiveOpt =
          Option(primitiveOpt.getOrElse(Primitive.None)),
        computationLabelOpt =
          Option(computationLabelOpt.getOrElse(containerId.toString)))
    case _ =>
      throw new RuntimeException(s"Next computation should be a container")
  }

  def shallowCopy(
      primitiveOpt: Option[Primitive] =
        primitiveOpt,
      computationLabelOpt: Option[String] =
        computationLabelOpt,
      patternOpt: Option[Pattern] =
        patternOpt,
      processOpt: Option[(E,Computation[E]) => Unit] =
        processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] =
        filterOpt,
      wordFilterOpt: Option[WordFilterFunc[E]] =
        wordFilterOpt,
      getPossibleExtensionsOpt: Option[(E,Computation[E]) => IntCollection] =
        getPossibleExtensionsOpt,
      initOpt: Option[(Computation[E]) => Unit] =
        initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] =
        initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] =
        finishOpt,
      expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] =
        expandComputeOpt,
      shouldBypassOpt: Option[Boolean] =
        shouldBypassOpt,
      processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
        processComputeOpt,
      nextComputationOpt: Option[Computation[E]] =
        nextComputationOpt)
    : ComputationContainer[E] = {
    this.copy(
      primitiveOpt = primitiveOpt,
      computationLabelOpt = computationLabelOpt,
      patternOpt = patternOpt,
      processOpt = processOpt,
      filterOpt = filterOpt,
      wordFilterOpt = wordFilterOpt,
      getPossibleExtensionsOpt = getPossibleExtensionsOpt,
      initOpt = initOpt,
      initAggregationsOpt = initAggregationsOpt,
      finishOpt = finishOpt,
      expandComputeOpt = expandComputeOpt,
      shouldBypassOpt = shouldBypassOpt,
      processComputeOpt = processComputeOpt,
      nextComputationOpt = nextComputationOpt
    )
  }

  def withNewFunctions(
      primitiveOpt: Option[Primitive] = 
        primitiveOpt,
      computationLabelOpt: Option[String] =
        lastComputation.computationLabelOpt,
      patternOpt: Option[Pattern] =
        patternOpt,
      processOpt: Option[(E,Computation[E]) => Unit] =
        lastComputation.processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] =
        lastComputation.filterOpt,
      wordFilterOpt: Option[WordFilterFunc[E]] =
        lastComputation.wordFilterOpt,
      getPossibleExtensionsOpt: Option[(E,Computation[E]) => IntCollection] =
        lastComputation.getPossibleExtensionsOpt,
      initOpt: Option[(Computation[E]) => Unit] =
        lastComputation.initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] =
        lastComputation.initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] =
        lastComputation.finishOpt,
      expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] =
        lastComputation.expandComputeOpt,
      shouldBypassOpt: Option[Boolean] =
        lastComputation.shouldBypassOpt,
      processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
        lastComputation.processComputeOpt)
    : ComputationContainer[E] = {

    val comps = new Stack[EComputationContainer[E]]()
    var currOpt: Option[EComputationContainer[E]] = Option(this)
    while (currOpt.isDefined) {
      comps.push(currOpt.get)
      currOpt = currOpt.get.nextComputationOpt.
        asInstanceOf[Option[EComputationContainer[E]]]
    }

    var lastComp = comps.pop()
    lastComp = lastComp.copy(primitiveOpt, computationLabelOpt, patternOpt,
        processOpt, filterOpt, wordFilterOpt,
        getPossibleExtensionsOpt,
        initOpt, initAggregationsOpt, finishOpt,
        expandComputeOpt, shouldBypassOpt, processComputeOpt)
    
    while (!comps.isEmpty) {
      lastComp = comps.pop().copy(nextComputationOpt = Some(lastComp))
    }
    
    lastComp
  }

  def withNewFunctionsAll(
      primitiveOpt: Option[Primitive] =
        primitiveOpt,
      computationLabelOpt: Option[String] =
        computationLabelOpt,
      patternOpt: Option[Pattern] =
        patternOpt,
      processOpt: Option[(E,Computation[E]) => Unit] =
        processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] =
        filterOpt,
      wordFilterOpt: Option[WordFilterFunc[E]] =
        wordFilterOpt,
      getPossibleExtensionsOpt: Option[(E,Computation[E]) => IntCollection] =
        getPossibleExtensionsOpt,
      initOpt: Option[(Computation[E]) => Unit] =
        initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] =
        initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] =
        finishOpt,
      expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] =
        expandComputeOpt,
      shouldBypassOpt: Option[Boolean] =
        shouldBypassOpt,
      processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
        processComputeOpt)
    : ComputationContainer[E] = nextComputationOpt match {
    case Some(nextComputation) =>
      val nextComp = nextComputation.asInstanceOf[ComputationContainer[E]].
      withNewFunctionsAll (primitiveOpt, computationLabelOpt, patternOpt,
        processOpt, filterOpt, wordFilterOpt,
        getPossibleExtensionsOpt,
        initOpt, initAggregationsOpt,
        finishOpt, expandComputeOpt, shouldBypassOpt, processComputeOpt)
      this.copy (primitiveOpt = primitiveOpt,
        computationLabelOpt = computationLabelOpt,
        patternOpt = patternOpt,
        processOpt = processOpt, filterOpt = filterOpt,
        wordFilterOpt = wordFilterOpt,
        getPossibleExtensionsOpt = getPossibleExtensionsOpt,
        initOpt = initOpt, initAggregationsOpt = initAggregationsOpt,
        finishOpt = finishOpt, expandComputeOpt = expandComputeOpt,
        shouldBypassOpt = shouldBypassOpt,
        processComputeOpt = processComputeOpt,
        nextComputationOpt = Some(nextComp))

    case None =>
      this.copy (primitiveOpt = primitiveOpt,
        computationLabelOpt = computationLabelOpt,
        patternOpt = patternOpt,
        processOpt = processOpt, filterOpt = filterOpt,
        wordFilterOpt = wordFilterOpt,
        getPossibleExtensionsOpt = getPossibleExtensionsOpt,
        initOpt = initOpt, initAggregationsOpt = initAggregationsOpt,
        finishOpt = finishOpt, expandComputeOpt = expandComputeOpt,
        shouldBypassOpt = shouldBypassOpt,
        processComputeOpt = processComputeOpt, nextComputationOpt = None)
  }
  
  def withComputationAppended(lastComputation: Computation[E])
    : ComputationContainer[E] = nextComputationOpt match {
      case Some(nextComputation) =>
        val container = nextComputation.asInstanceOf[ComputationContainer[E]]
        val _nextComputation = container.
          withComputationAppended(lastComputation)
        this.copy(nextComputationOpt = Option(_nextComputation))
      case None =>
        this.copy(nextComputationOpt = Option(lastComputation))
  }

  def withPrimitive(p: Primitive): ComputationContainer[E] = {
    this.copy (primitiveOpt = Option(p))
  }
  
  def withComputationLabel(label: String): ComputationContainer[E] = {
    this.copy (computationLabelOpt = Option(label))
  }

  def asLastComputation: ComputationContainer[E] = {
    this.copy(nextComputationOpt = None)
  }

  def take(n: Int): ComputationContainer[E] = {
    if (n <= 1) {
      this.asLastComputation
    } else {
      nextComputationOpt match {
        case Some(nextComputation : ComputationContainer[E]) =>
          val _nextComputation = nextComputation.take(n - 1)
          this.copy(nextComputationOpt = Some(_nextComputation))
        case _ =>
          this.asLastComputation
      }
    }
  }

  override def computationLabel(): String = _computationLabel

  override def process(e: E): Unit = _process (e, this)

  override def filter(e: E): Boolean = _filter (e, this)
  
  override def filter(e: E, w: Int): Boolean = _wordFilter (e, w, this)

  override def getPossibleExtensions(e: E): IntCollection =
    _getPossibleExtensions (e, this)

  override def init(config: Configuration[E]): Unit = _init (config, this)

  override def initAggregations(config: Configuration[E]): Unit =
    _initAggregations (config, this)

  override def finish(): Unit = _finish (this)
  
  override def expandCompute(e: E): SubgraphEnumerator[E] =
    _expandCompute (e, this)

  override def shouldBypass(): Boolean = _shouldBypass

  override def processCompute(iter: SubgraphEnumerator[E]) =
    _processCompute (iter, this)
  
  override def nextComputation(): Computation[E] = _nextComputation
  
  override def getPattern(): Pattern = _pattern
  
  //override def toString: String = s"E${super.toString}"
}

case class VComputationContainer [E <: VertexInducedSubgraph] (
    primitiveOpt: Option[Primitive] = None,
    computationLabelOpt: Option[String] = None,
    patternOpt: Option[Pattern] = None,
    processOpt: Option[(E,Computation[E]) => Unit] = None,
    filterOpt: Option[(E,Computation[E]) => Boolean] = None,
    wordFilterOpt: Option[WordFilterFunc[E]] = None,
    getPossibleExtensionsOpt: Option[(E,Computation[E]) => IntCollection] = None,
    initOpt: Option[(Computation[E]) => Unit] = None,
    initAggregationsOpt: Option[(Computation[E]) => Unit] = None,
    finishOpt: Option[(Computation[E]) => Unit] = None,
    expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] = None,
    shouldBypassOpt: Option[Boolean] = None,
    processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
      None,
    nextComputationOpt: Option[Computation[E]] = None)
  extends VertexInducedComputation[E] with ComputationContainer[E] {
  
  private val pconfigOpt: Option[Configuration[E]] =
    patternOpt.map(_.getConfig().asInstanceOf[Configuration[E]])

  @transient private lazy val _primitive: Primitive =
    primitiveOpt.getOrElse (Primitive.None)

  @transient private lazy val _computationLabel: String =
    computationLabelOpt.getOrElse (containerId.toString)
  
  @transient private lazy val _pattern: Pattern =
    patternOpt.getOrElse(null)

  @transient private lazy val _process: (E,Computation[E]) => Unit =
    processOpt.getOrElse ((e: E, c: Computation[E]) => super.process (e))
  
  @transient private lazy val _filter: (E,Computation[E]) => Boolean =
    filterOpt.getOrElse ((e: E, c: Computation[E]) => super.filter (e))
  
  @transient private lazy val _wordFilter: WordFilterFunc[E] = {
    wordFilterOpt.getOrElse (
      new WordFilterFunc [E] {
        def apply(e: E, w: Int, c: Computation[E]): Boolean = {
          true
        }
      }
    )
  }

  @transient private lazy val _getPossibleExtensions: (E,Computation[E]) => IntCollection =
    getPossibleExtensionsOpt.getOrElse (
      (e: E, c: Computation[E]) => super.getPossibleExtensions (e))
  
  @transient private lazy val _init
    : (Configuration[E], Computation[E]) => Unit = initOpt match {
    case Some(thisInit) =>
      (config: Configuration[E], c: Computation[E]) => {
        super.init(config); thisInit(c)
      }
    case None =>
      (config: Configuration[E], c: Computation[E]) => {
        super.init(config)
      }
  }

  @transient private lazy val _initAggregations
    : (Configuration[E], Computation[E]) => Unit = initAggregationsOpt match {
    case Some(thisInitAggregations) =>
      (config: Configuration[E], c: Computation[E]) => {
        super.initAggregations(config)
        thisInitAggregations(c)
      }

    case None =>
      (config: Configuration[E], c: Computation[E]) => {
        super.initAggregations(config)
      }
  }

  @transient private lazy val _finish
    : (Computation[E]) => Unit = finishOpt match {
    case Some(thisFinish) =>
      (c: Computation[E]) => {super.finish(); thisFinish(c)}
    case None =>
      (c: Computation[E]) => {super.finish()}
  }
  
  @transient private lazy val _expandCompute
    : (E,Computation[E]) => SubgraphEnumerator[E] =
    expandComputeOpt.getOrElse (
      (e: E, c: Computation[E]) => super.expandCompute(e)
    )

  @transient private lazy val _shouldBypass: Boolean = {
    shouldBypassOpt.getOrElse(super.shouldBypass())
  }
  
  @transient private lazy val _processCompute
    : (SubgraphEnumerator[E],Computation[E]) => Long =
    processComputeOpt.getOrElse (
      (iter: SubgraphEnumerator[E], c: Computation[E]) => -1
    )
  
  @transient private lazy val _nextComputation
    : Computation[E] = nextComputationOpt match {
    case Some(nc) => nc
    case None => null
  }

  def shallowCopy(): ComputationContainer[E] = nextComputationOpt match {
    case Some(nextComputation : ComputationContainer[E]) =>
      this.copy(
        primitiveOpt =
          Option(primitiveOpt.getOrElse(Primitive.None)),
        computationLabelOpt =
          Option(computationLabelOpt.getOrElse(containerId.toString)),
        nextComputationOpt = Option(nextComputation.shallowCopy()))
    case None =>
      this.copy(
        primitiveOpt =
          Option(primitiveOpt.getOrElse(Primitive.None)),
        computationLabelOpt =
          Option(computationLabelOpt.getOrElse(containerId.toString)))
    case _ =>
      throw new RuntimeException(s"Next computation should be a container")
  }
  
  def shallowCopy(
      primitiveOpt: Option[Primitive] =
        primitiveOpt,
      computationLabelOpt: Option[String] =
        computationLabelOpt,
      patternOpt: Option[Pattern] =
        patternOpt,
      processOpt: Option[(E,Computation[E]) => Unit] =
        processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] =
        filterOpt,
      wordFilterOpt: Option[WordFilterFunc[E]] =
        wordFilterOpt,
      getPossibleExtensionsOpt: Option[(E,Computation[E]) => IntCollection] =
        getPossibleExtensionsOpt,
      initOpt: Option[(Computation[E]) => Unit] =
        initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] =
        initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] =
        finishOpt,
      expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] =
        expandComputeOpt,
      shouldBypassOpt: Option[Boolean] =
        shouldBypassOpt,
      processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
        processComputeOpt,
      nextComputationOpt: Option[Computation[E]] =
        nextComputationOpt)
    : ComputationContainer[E] = {
    this.copy(
      primitiveOpt = primitiveOpt,
      computationLabelOpt = computationLabelOpt,
      patternOpt = patternOpt,
      processOpt = processOpt,
      filterOpt = filterOpt,
      wordFilterOpt = wordFilterOpt,
      getPossibleExtensionsOpt = getPossibleExtensionsOpt,
      initOpt = initOpt,
      initAggregationsOpt = initAggregationsOpt,
      finishOpt = finishOpt,
      expandComputeOpt = expandComputeOpt,
      shouldBypassOpt = shouldBypassOpt,
      processComputeOpt = processComputeOpt,
      nextComputationOpt = nextComputationOpt
    )
  }

  def withNewFunctions(
      primitiveOpt: Option[Primitive] =
        primitiveOpt,
      computationLabelOpt: Option[String] =
        lastComputation.computationLabelOpt,
      patternOpt: Option[Pattern] =
        patternOpt,
      processOpt: Option[(E,Computation[E]) => Unit] =
        lastComputation.processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] =
        lastComputation.filterOpt,
      wordFilterOpt: Option[WordFilterFunc[E]] =
        lastComputation.wordFilterOpt,
      getPossibleExtensionsOpt: Option[(E,Computation[E]) => IntCollection] =
        lastComputation.getPossibleExtensionsOpt,
      initOpt: Option[(Computation[E]) => Unit] =
        lastComputation.initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] =
        lastComputation.initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] =
        lastComputation.finishOpt,
      expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] =
        lastComputation.expandComputeOpt,
      shouldBypassOpt: Option[Boolean] =
        lastComputation.shouldBypassOpt,
      processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
        lastComputation.processComputeOpt)
    : ComputationContainer[E] = {

    val comps = new Stack[VComputationContainer[E]]()
    var currOpt: Option[VComputationContainer[E]] = Option(this)
    while (currOpt.isDefined) {
      comps.push(currOpt.get)
      currOpt = currOpt.get.nextComputationOpt.
        asInstanceOf[Option[VComputationContainer[E]]]
    }

    var lastComp = comps.pop()
    lastComp = lastComp.copy(primitiveOpt, computationLabelOpt, patternOpt,
        processOpt, filterOpt, wordFilterOpt,
        getPossibleExtensionsOpt,
        initOpt, initAggregationsOpt, finishOpt,
        expandComputeOpt, shouldBypassOpt, processComputeOpt)
    
    while (!comps.isEmpty) {
      lastComp = comps.pop().copy(nextComputationOpt = Some(lastComp))
    }
    
    lastComp
  }

  def withNewFunctionsAll(
      primitiveOpt: Option[Primitive] =
        primitiveOpt,
      computationLabelOpt: Option[String] =
        computationLabelOpt,
      patternOpt: Option[Pattern] =
        patternOpt,
      processOpt: Option[(E,Computation[E]) => Unit] =
        processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] =
        filterOpt,
      wordFilterOpt: Option[WordFilterFunc[E]] =
        wordFilterOpt,
      getPossibleExtensionsOpt: Option[(E,Computation[E]) => IntCollection] =
        getPossibleExtensionsOpt,
      initOpt: Option[(Computation[E]) => Unit] =
        initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] =
        initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] =
        finishOpt,
      expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] =
        expandComputeOpt,
      shouldBypassOpt: Option[Boolean] =
        shouldBypassOpt,
      processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
        processComputeOpt)
    : ComputationContainer[E] = nextComputationOpt match {
    case Some(nextComputation) =>
      val nextComp = nextComputation.asInstanceOf[ComputationContainer[E]].
      withNewFunctionsAll (primitiveOpt, computationLabelOpt, patternOpt,
        processOpt, filterOpt, wordFilterOpt,
        getPossibleExtensionsOpt,
        initOpt, initAggregationsOpt,
        finishOpt, expandComputeOpt, shouldBypassOpt, processComputeOpt)
      this.copy (primitiveOpt = primitiveOpt,
        computationLabelOpt = computationLabelOpt,
        patternOpt = patternOpt,
        processOpt = processOpt, filterOpt = filterOpt,
        wordFilterOpt = wordFilterOpt,
        getPossibleExtensionsOpt = getPossibleExtensionsOpt,
        initOpt = initOpt, initAggregationsOpt = initAggregationsOpt,
        finishOpt = finishOpt, expandComputeOpt = expandComputeOpt,
        shouldBypassOpt = shouldBypassOpt,
        processComputeOpt = processComputeOpt,
        nextComputationOpt = Some(nextComp))

    case None =>
      this.copy (primitiveOpt = primitiveOpt,
        computationLabelOpt = computationLabelOpt,
        patternOpt = patternOpt,
        processOpt = processOpt, filterOpt = filterOpt,
        wordFilterOpt = wordFilterOpt,
        getPossibleExtensionsOpt = getPossibleExtensionsOpt,
        initOpt = initOpt, initAggregationsOpt = initAggregationsOpt,
        finishOpt = finishOpt, expandComputeOpt = expandComputeOpt,
        shouldBypassOpt = shouldBypassOpt,
        processComputeOpt = processComputeOpt, nextComputationOpt = None)
  }
  
  def withComputationAppended(lastComputation: Computation[E])
    : ComputationContainer[E] = nextComputationOpt match {
      case Some(nextComputation) =>
        val container = nextComputation.asInstanceOf[ComputationContainer[E]]
        val _nextComputation = container.
          withComputationAppended(lastComputation)
        this.copy(nextComputationOpt = Option(_nextComputation))
      case None =>
        this.copy(nextComputationOpt = Option(lastComputation))
  }

  def withPrimitive(p: Primitive): ComputationContainer[E] = {
    this.copy (primitiveOpt = Option(p))
  }
  
  def withComputationLabel(label: String): ComputationContainer[E] = {
    this.copy (computationLabelOpt = Option(label))
  }
  
  def asLastComputation: ComputationContainer[E] = {
    this.copy(nextComputationOpt = None)
  }
  
  def take(n: Int): ComputationContainer[E] = {
    if (n <= 1) {
      this.asLastComputation
    } else {
      nextComputationOpt match {
        case Some(nextComputation : ComputationContainer[E]) =>
          val _nextComputation = nextComputation.take(n - 1)
          this.copy(nextComputationOpt = Some(_nextComputation))
        case _ =>
          this.asLastComputation
      }
    }
  }

  override def computationLabel(): String = _computationLabel

  override def process(e: E): Unit = _process (e, this)

  override def filter(e: E): Boolean = _filter (e, this)
  
  override def filter(e: E, w: Int): Boolean = _wordFilter (e, w, this)

  override def getPossibleExtensions(e: E): IntCollection =
    _getPossibleExtensions (e, this)

  override def init(config: Configuration[E]): Unit = _init (config, this)

  override def initAggregations(config: Configuration[E]): Unit =
    _initAggregations (config, this)

  override def finish(): Unit = _finish (this)

  override def expandCompute(e: E): SubgraphEnumerator[E] =
    _expandCompute (e, this)

  override def shouldBypass(): Boolean = _shouldBypass
  
  override def processCompute(iter: SubgraphEnumerator[E]) =
    _processCompute (iter, this)
  
  override def nextComputation(): Computation[E] = _nextComputation
  
  override def getPattern(): Pattern = _pattern
  
  //override def toString: String = s"V${super.toString}"
}

case class VEComputationContainer [E <: PatternInducedSubgraph](
    primitiveOpt: Option[Primitive] = None,
    computationLabelOpt: Option[String] = None,
    patternOpt: Option[Pattern] = None,
    processOpt: Option[(E,Computation[E]) => Unit] = None,
    filterOpt: Option[(E,Computation[E]) => Boolean] = None,
    wordFilterOpt: Option[WordFilterFunc[E]] = None,
    getPossibleExtensionsOpt: Option[(E,Computation[E]) => IntCollection] = None,
    initOpt: Option[(Computation[E]) => Unit] = None,
    initAggregationsOpt: Option[(Computation[E]) => Unit] = None,
    finishOpt: Option[(Computation[E]) => Unit] = None,
    expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] = None,
    shouldBypassOpt: Option[Boolean] = None,
    processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
      None,
    nextComputationOpt: Option[Computation[E]] = None)
  extends PatternInducedComputation[E] with ComputationContainer[E] {
  
  private val pconfigOpt: Option[Configuration[E]] =
    patternOpt.map(_.getConfig().asInstanceOf[Configuration[E]])

  @transient private lazy val _primitive: Primitive =
    primitiveOpt.getOrElse (Primitive.None)

  @transient private lazy val _computationLabel: String =
    computationLabelOpt.getOrElse (containerId.toString)
  
  @transient private lazy val _pattern: Pattern =
    patternOpt.getOrElse(null)

  @transient private lazy val _process: (E,Computation[E]) => Unit =
    processOpt.getOrElse ((e: E, c: Computation[E]) => super.process (e))
  
  @transient private lazy val _filter: (E,Computation[E]) => Boolean =
    filterOpt.getOrElse ((e: E, c: Computation[E]) => super.filter (e))
  
  @transient private lazy val _wordFilter: WordFilterFunc[E] = {
    wordFilterOpt.getOrElse (
      new WordFilterFunc [E] {
        def apply(e: E, w: Int, c: Computation[E]): Boolean = {
          e.isCanonicalSubgraphWithWord(w)
        }
      }
    )
  }

  @transient private lazy val _getPossibleExtensions: (E,Computation[E]) => IntCollection =
    getPossibleExtensionsOpt.getOrElse (
      (e: E, c: Computation[E]) => super.getPossibleExtensions (e)
    )
  
  @transient private lazy val _init
    : (Configuration[E], Computation[E]) => Unit = initOpt match {

    case Some(thisInit) =>
      (config: Configuration[E], c: Computation[E]) => {
        super.init(config)
        thisInit(c)
      }

    case None =>
      (config:Configuration[E], c: Computation[E]) => {
        super.init(config)
      }
  }

  @transient private lazy val _initAggregations
    : (Configuration[E], Computation[E]) => Unit = {
    initAggregationsOpt match {
      case Some(thisInitAggregations) =>
        (config: Configuration[E], c: Computation[E]) => {
          super.initAggregations(config)
          thisInitAggregations(c)
        }

      case None =>
        (config: Configuration[E], c: Computation[E]) => {
          super.initAggregations(config)
        }
    }
  }

  @transient private lazy val _finish
    : (Computation[E]) => Unit = finishOpt match {
    case Some(thisFinish) =>
      (c: Computation[E]) => {super.finish(); thisFinish(c)}
    case None =>
      (c: Computation[E]) => {super.finish()}
  }

  @transient private lazy val _expandCompute
    : (E,Computation[E]) => SubgraphEnumerator[E] =
    expandComputeOpt.getOrElse (
      (e: E, c: Computation[E]) => super.expandCompute(e)
    )

  @transient private lazy val _shouldBypass: Boolean = {
    shouldBypassOpt.getOrElse(super.shouldBypass())
  }

  @transient private lazy val _processCompute
    : (SubgraphEnumerator[E],Computation[E]) => Long =
    processComputeOpt.getOrElse (
      (iter: SubgraphEnumerator[E], c: Computation[E]) => -1
    )

  @transient private lazy val _nextComputation
    : Computation[E] = nextComputationOpt match {
    case Some(nc) => nc
    case None => null
  }

  def shallowCopy(): ComputationContainer[E] = nextComputationOpt match {
    case Some(nextComputation : ComputationContainer[E]) =>
      this.copy(
        primitiveOpt =
          Option(primitiveOpt.getOrElse(Primitive.None)),
        computationLabelOpt =
          Option(computationLabelOpt.getOrElse(containerId.toString)),
        nextComputationOpt = Option(nextComputation.shallowCopy()))
    case None =>
      this.copy(
        primitiveOpt =
          Option(primitiveOpt.getOrElse(Primitive.None)),
        computationLabelOpt =
          Option(computationLabelOpt.getOrElse(containerId.toString)))
    case _ =>
      throw new RuntimeException(s"Next computation should be a container")
  }

  def shallowCopy(
      primitiveOpt: Option[Primitive] =
        primitiveOpt,
      computationLabelOpt: Option[String] =
        computationLabelOpt,
      patternOpt: Option[Pattern] =
        patternOpt,
      processOpt: Option[(E,Computation[E]) => Unit] =
        processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] =
        filterOpt,
      wordFilterOpt: Option[WordFilterFunc[E]] =
        wordFilterOpt,
      getPossibleExtensionsOpt: Option[(E,Computation[E]) => IntCollection] =
        getPossibleExtensionsOpt,
      initOpt: Option[(Computation[E]) => Unit] =
        initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] =
        initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] =
        finishOpt,
      expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] =
        expandComputeOpt,
      shouldBypassOpt: Option[Boolean] =
        shouldBypassOpt,
      processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
        processComputeOpt,
      nextComputationOpt: Option[Computation[E]] =
        nextComputationOpt)
    : ComputationContainer[E] = {
    this.copy(
      primitiveOpt = primitiveOpt,
      computationLabelOpt = computationLabelOpt,
      patternOpt = patternOpt,
      processOpt = processOpt,
      filterOpt = filterOpt,
      wordFilterOpt = wordFilterOpt,
      getPossibleExtensionsOpt = getPossibleExtensionsOpt,
      initOpt = initOpt,
      initAggregationsOpt = initAggregationsOpt,
      finishOpt = finishOpt,
      expandComputeOpt = expandComputeOpt,
      shouldBypassOpt = shouldBypassOpt,
      processComputeOpt = processComputeOpt,
      nextComputationOpt = nextComputationOpt
    )
  }

  def withNewFunctions(
      primitiveOpt: Option[Primitive] =
        primitiveOpt,
      computationLabelOpt: Option[String] =
        lastComputation.computationLabelOpt,
        patternOpt: Option[Pattern] =
          patternOpt,
      processOpt: Option[(E,Computation[E]) => Unit] =
        lastComputation.processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] =
        lastComputation.filterOpt,
      wordFilterOpt: Option[WordFilterFunc[E]] =
        lastComputation.wordFilterOpt,
      getPossibleExtensionsOpt: Option[(E,Computation[E]) => IntCollection] =
        lastComputation.getPossibleExtensionsOpt,
      initOpt: Option[(Computation[E]) => Unit] =
        lastComputation.initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] =
        lastComputation.initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] =
        lastComputation.finishOpt,
      expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] =
        lastComputation.expandComputeOpt,
      shouldBypassOpt: Option[Boolean] =
        lastComputation.shouldBypassOpt,
      processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
        lastComputation.processComputeOpt)
    : ComputationContainer[E] = {

    val comps = new Stack[VEComputationContainer[E]]()
    var currOpt: Option[VEComputationContainer[E]] = Option(this)
    while (currOpt.isDefined) {
      comps.push(currOpt.get)
      currOpt = currOpt.get.nextComputationOpt.
        asInstanceOf[Option[VEComputationContainer[E]]]
    }

    var lastComp = comps.pop()
    lastComp = lastComp.copy(primitiveOpt, computationLabelOpt, patternOpt,
        processOpt, filterOpt, wordFilterOpt,
        getPossibleExtensionsOpt,
        initOpt, initAggregationsOpt, finishOpt,
        expandComputeOpt, shouldBypassOpt, processComputeOpt)
    
    while (!comps.isEmpty) {
      lastComp = comps.pop().copy(nextComputationOpt = Some(lastComp))
    }
    
    lastComp
  }

  def withNewFunctionsAll(
      primitiveOpt: Option[Primitive] =
        primitiveOpt,
      computationLabelOpt: Option[String] =
        computationLabelOpt,
      patternOpt: Option[Pattern] =
        patternOpt,
      processOpt: Option[(E,Computation[E]) => Unit] =
        processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] =
        filterOpt,
      wordFilterOpt: Option[WordFilterFunc[E]] =
        wordFilterOpt,
      getPossibleExtensionsOpt: Option[(E,Computation[E]) => IntCollection] =
        getPossibleExtensionsOpt,
      initOpt: Option[(Computation[E]) => Unit] =
        initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] =
        initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] =
        finishOpt,
      expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] =
        expandComputeOpt,
      shouldBypassOpt: Option[Boolean] =
        shouldBypassOpt,
      processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
        processComputeOpt)
    : ComputationContainer[E] = nextComputationOpt match {
    case Some(nextComputation) =>
      val nextComp = nextComputation.asInstanceOf[ComputationContainer[E]].
      withNewFunctionsAll (primitiveOpt, computationLabelOpt, patternOpt,
        processOpt, filterOpt, wordFilterOpt,
        getPossibleExtensionsOpt,
        initOpt, initAggregationsOpt,
        finishOpt, expandComputeOpt, shouldBypassOpt, processComputeOpt)
      this.copy (primitiveOpt = primitiveOpt,
        computationLabelOpt = computationLabelOpt,
        patternOpt = patternOpt,
        processOpt = processOpt, filterOpt = filterOpt,
        wordFilterOpt = wordFilterOpt,
        getPossibleExtensionsOpt = getPossibleExtensionsOpt,
        initOpt = initOpt, initAggregationsOpt = initAggregationsOpt,
        finishOpt = finishOpt, expandComputeOpt = expandComputeOpt,
        shouldBypassOpt = shouldBypassOpt,
        processComputeOpt = processComputeOpt,
        nextComputationOpt = Some(nextComp))

    case None =>
      this.copy (primitiveOpt = primitiveOpt,
        computationLabelOpt = computationLabelOpt,
        patternOpt = patternOpt,
        processOpt = processOpt, filterOpt = filterOpt,
        wordFilterOpt = wordFilterOpt,
        getPossibleExtensionsOpt = getPossibleExtensionsOpt,
        initOpt = initOpt, initAggregationsOpt = initAggregationsOpt,
        finishOpt = finishOpt, expandComputeOpt = expandComputeOpt,
        shouldBypassOpt = shouldBypassOpt,
        processComputeOpt = processComputeOpt, nextComputationOpt = None)
  }
  
  def withComputationAppended(lastComputation: Computation[E])
    : ComputationContainer[E] = nextComputationOpt match {
      case Some(nextComputation) =>
        val container = nextComputation.asInstanceOf[ComputationContainer[E]]
        val _nextComputation = container.
          withComputationAppended(lastComputation)
        this.copy(nextComputationOpt = Option(_nextComputation))
      case None =>
        this.copy(nextComputationOpt = Option(lastComputation))
  }

  def withPrimitive(p: Primitive): ComputationContainer[E] = {
    this.copy (primitiveOpt = Option(p))
  }
  
  def withComputationLabel(label: String): ComputationContainer[E] = {
    this.copy (computationLabelOpt = Option(label))
  }

  def asLastComputation: ComputationContainer[E] = {
    this.copy(nextComputationOpt = None)
  }

  def take(n: Int): ComputationContainer[E] = {
    if (n <= 1) {
      this.asLastComputation
    } else {
      nextComputationOpt match {
        case Some(nextComputation : ComputationContainer[E]) =>
          val _nextComputation = nextComputation.take(n - 1)
          this.copy(nextComputationOpt = Some(_nextComputation))
        case _ =>
          this.asLastComputation
      }
    }
  }

  override def computationLabel(): String = _computationLabel

  override def process(e: E): Unit = _process (e, this)

  override def filter(e: E): Boolean = _filter (e, this)
  
  override def filter(e: E, w: Int): Boolean = _wordFilter (e, w, this)

  override def getPossibleExtensions(e: E): IntCollection =
    _getPossibleExtensions (e, this)

  override def init(config: Configuration[E]): Unit = {
    val pattern = getPattern()
    if (pattern != null) {
      pconfigOpt match {
        case Some(conf) =>
          conf.initialize()
          pattern.init(conf)
        case None =>
          throw new RuntimeException(
            s"Invalid state, pattern configuration is missing.")
      }
    }
    _init (config, this)
  }

  override def initAggregations(config: Configuration[E]): Unit =
    _initAggregations (config, this)

  override def finish(): Unit = _finish (this)
  
  override def expandCompute(e: E): SubgraphEnumerator[E] =
    _expandCompute (e, this)

  override def shouldBypass(): Boolean = _shouldBypass

  override def processCompute(iter: SubgraphEnumerator[E]) =
    _processCompute (iter, this)

  override def nextComputation(): Computation[E] = _nextComputation

  override def getPattern(): Pattern = _pattern
  
  //override def toString: String = s"E${super.toString}"
}

object ComputationContainer {
  val nextContainerId = new AtomicInteger(0)
}


