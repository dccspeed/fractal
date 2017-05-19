package io.arabesque.computation

import io.arabesque.conf.Configuration
import io.arabesque.embedding._
import io.arabesque.pattern.Pattern
import io.arabesque.utils.Logging

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable.Stack

// TODO: Refactor me!
sealed trait ComputationContainer [E <: Embedding] extends Computation[E] 
    with Logging {

  val callerSite: String = {
    val calls = Thread.currentThread().getStackTrace()
    def findCaller: String = {
      var i = 0
      while (i < calls.length) {
        if (calls(i).getClassName equals "io.arabesque.ArabesqueResult") {
          return calls(i).getMethodName
        }
        i += 1
      }
      null
    }
    findCaller
  }

  val containerId: Int = ComputationContainer.nextContainerId.getAndIncrement

  val processOpt: Option[(E,Computation[E]) => Unit]

  val filterOpt: Option[(E,Computation[E]) => Boolean]

  val shouldExpandOpt: Option[(E,Computation[E]) => Boolean]

  val aggregationFilterOpt: Option[(E,Computation[E]) => Boolean]

  val pAggregationFilterOpt: Option[(Pattern,Computation[E]) => Boolean]

  val aggregationProcessOpt: Option[(E,Computation[E]) => Unit]

  val handleNoExpansionsOpt: Option[(E,Computation[E]) => Unit]

  val initOpt: Option[(Computation[E]) => Unit]

  val initAggregationsOpt: Option[(Computation[E]) => Unit]

  val finishOpt: Option[(Computation[E]) => Unit]

  val expandComputeOpt: Option[(E,Computation[E]) => Iterator[E]]

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

  def asLastComputation: ComputationContainer[E]
  
  def take(n: Int): ComputationContainer[E]

  def withNewFunctions(
      processOpt: Option[(E,Computation[E]) => Unit] =
        processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] =
        filterOpt,
      shouldExpandOpt: Option[(E,Computation[E]) => Boolean] =
        shouldExpandOpt,
      aggregationFilterOpt: Option[(E,Computation[E]) => Boolean] =
        aggregationFilterOpt,
      pAggregationFilterOpt: Option[(Pattern,Computation[E]) => Boolean] =
        pAggregationFilterOpt,
      aggregationProcessOpt: Option[(E,Computation[E]) => Unit] =
        aggregationProcessOpt,
      handleNoExpansionsOpt: Option[(E,Computation[E]) => Unit] =
        handleNoExpansionsOpt,
      initOpt: Option[(Computation[E]) => Unit] =
        initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] =
        initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] =
        finishOpt,
      expandComputeOpt: Option[(E,Computation[E]) => Iterator[E]] =
        expandComputeOpt
    ): ComputationContainer[E]

  def shallowCopy(): ComputationContainer[E] = this.withNewFunctions()

  def clear(): ComputationContainer[E] = withNewFunctions (
    processOpt = Some((e,c) => {}),
    filterOpt = Some((e,c) => true),
    shouldExpandOpt = Some((e,c) => true),
    aggregationFilterOpt = Some((e,c) => true),
    pAggregationFilterOpt = Some((e,c) => true),
    aggregationProcessOpt = Some((e,c) => {}),
    handleNoExpansionsOpt = Some((e,c) => {}),
    expandComputeOpt = Some((e,c) => Iterator.empty)
    )

  override def toString: String = {
    // s"CC[${containerId},${callerSite}]" +
    s"CC[${containerId}]" +
    s"(${expandComputeOpt.map(_ => "expandCompute->").getOrElse("")}" +
    s"${filterOpt.map(_ => "filter->").getOrElse("")}" +
    s"${processOpt.map(_ => "process").getOrElse("")})" +
    s"${nextComputationOpt.map(c => "-->" + c.toString).getOrElse("")}"
  }
}

case class EComputationContainer [E <: EdgeInducedEmbedding] (
    processOpt: Option[(E,Computation[E]) => Unit] = None,
    filterOpt: Option[(E,Computation[E]) => Boolean] = None,
    shouldExpandOpt: Option[(E,Computation[E]) => Boolean] = None,
    aggregationFilterOpt: Option[(E,Computation[E]) => Boolean] = None,
    pAggregationFilterOpt: Option[(Pattern,Computation[E]) => Boolean] = None,
    aggregationProcessOpt: Option[(E,Computation[E]) => Unit] = None,
    handleNoExpansionsOpt: Option[(E,Computation[E]) => Unit] = None,
    initOpt: Option[(Computation[E]) => Unit] = None,
    initAggregationsOpt: Option[(Computation[E]) => Unit] = None,
    finishOpt: Option[(Computation[E]) => Unit] = None,
    expandComputeOpt: Option[(E,Computation[E]) => Iterator[E]] = None,
    nextComputationOpt: Option[Computation[E]] = None)
  extends EdgeInducedComputation[E] with ComputationContainer[E] {

  @transient private lazy val _process: (E,Computation[E]) => Unit =
    processOpt.getOrElse ((e: E, c: Computation[E]) => super.process (e))
  
  @transient private lazy val _filter: (E,Computation[E]) => Boolean =
    filterOpt.getOrElse ((e: E, c: Computation[E]) => super.filter (e))

  @transient private lazy val _shouldExpand: (E,Computation[E]) => Boolean =
    shouldExpandOpt.getOrElse (
      (e: E, c: Computation[E]) => super.shouldExpand(e)
    )
  
  @transient private lazy val _aggregationFilter
    : (E,Computation[E]) => Boolean =
    aggregationFilterOpt.getOrElse (
      (e: E, c: Computation[E]) => super.aggregationFilter (e)
    )
  
  @transient private lazy val _pAggregationFilter
    : (Pattern,Computation[E]) => Boolean = pAggregationFilterOpt.getOrElse (
      (p: Pattern, c: Computation[E]) => super.aggregationFilter (p)
    )
  
  @transient private lazy val _aggregationProcess: (E,Computation[E]) => Unit =
    aggregationProcessOpt.getOrElse (
      (e: E, c: Computation[E]) => super.aggregationProcess (e)
    )
  
  @transient private lazy val _handleNoExpansions: (E,Computation[E]) => Unit =
    handleNoExpansionsOpt.getOrElse (
      (e: E, c: Computation[E]) => super.handleNoExpansions (e)
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
    : (Configuration[E], Computation[E]) => Unit =
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

  @transient private lazy val _finish
    : (Computation[E]) => Unit = finishOpt match {
    case Some(thisFinish) =>
      (c: Computation[E]) => {super.finish(); thisFinish(c)}
    case None =>
      (c: Computation[E]) => {super.finish()}
  }

  @transient private lazy val _expandCompute
    : (E,Computation[E]) => Iterator[E] =
    expandComputeOpt.getOrElse (
      (e: E, c: Computation[E]) => super.expandCompute(e)
    )

  @transient private lazy val _nextComputation
    : Computation[E] = nextComputationOpt match {
    case Some(nc) => nc
    case None => null
  }

  def withNewFunctions(
      processOpt: Option[(E,Computation[E]) => Unit] =
        processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] =
        filterOpt,
      shouldExpandOpt: Option[(E,Computation[E]) => Boolean] =
        shouldExpandOpt,
      aggregationFilterOpt: Option[(E,Computation[E]) => Boolean] =
        aggregationFilterOpt,
      pAggregationFilterOpt: Option[(Pattern,Computation[E]) => Boolean] =
        pAggregationFilterOpt,
      aggregationProcessOpt: Option[(E,Computation[E]) => Unit] =
        aggregationProcessOpt,
      handleNoExpansionsOpt: Option[(E,Computation[E]) => Unit] = 
        handleNoExpansionsOpt,
      initOpt: Option[(Computation[E]) => Unit] =
        initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] =
        initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] =
        finishOpt,
      expandComputeOpt: Option[(E,Computation[E]) => Iterator[E]] =
        expandComputeOpt)
    : ComputationContainer[E] = nextComputationOpt match {
    case Some(nextComputation) =>
      val nextComp = nextComputation.asInstanceOf[ComputationContainer[E]].
      withNewFunctions (
        processOpt, filterOpt, shouldExpandOpt, aggregationFilterOpt,
        pAggregationFilterOpt, aggregationProcessOpt,
        handleNoExpansionsOpt, initOpt, initAggregationsOpt,
        finishOpt, expandComputeOpt)
      this.copy (nextComputationOpt = Some(nextComp))

    case None =>
      this.copy (processOpt = processOpt, filterOpt = filterOpt,
        shouldExpandOpt = shouldExpandOpt,
        aggregationFilterOpt = aggregationFilterOpt,
        pAggregationFilterOpt = pAggregationFilterOpt,
        aggregationProcessOpt = aggregationProcessOpt,
        handleNoExpansionsOpt = handleNoExpansionsOpt,
        initOpt = initOpt, initAggregationsOpt = initAggregationsOpt,
        finishOpt = finishOpt, expandComputeOpt = expandComputeOpt,
        nextComputationOpt = None)
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

  override def process(e: E): Unit = _process (e, this)

  override def filter(e: E): Boolean = _filter (e, this)

  override def shouldExpand(e: E): Boolean = _shouldExpand (e, this)

  override def aggregationFilter(e: E): Boolean = _aggregationFilter (e, this)

  override def aggregationFilter(p: Pattern): Boolean =
    _pAggregationFilter (p, this)

  override def aggregationProcess(e: E): Unit = _aggregationProcess (e, this)

  override def handleNoExpansions(e: E): Unit = _handleNoExpansions (e, this)

  override def init(config: Configuration[E]): Unit = _init (config, this)

  override def initAggregations(config: Configuration[E]): Unit =
    _initAggregations (config, this)

  override def finish(): Unit = _finish (this)
  
  override def expandCompute(e: E): java.util.Iterator[E] =
    _expandCompute (e, this).asJava

  override def nextComputation(): Computation[E] = _nextComputation
  
  override def toString: String = s"E${super.toString}"
}


case class VComputationContainer [E <: VertexInducedEmbedding] (
    processOpt: Option[(E,Computation[E]) => Unit] = None,
    filterOpt: Option[(E,Computation[E]) => Boolean] = None,
    shouldExpandOpt: Option[(E,Computation[E]) => Boolean] = None,
    aggregationFilterOpt: Option[(E,Computation[E]) => Boolean] = None,
    pAggregationFilterOpt: Option[(Pattern,Computation[E]) => Boolean] = None,
    aggregationProcessOpt: Option[(E,Computation[E]) => Unit] = None,
    handleNoExpansionsOpt: Option[(E,Computation[E]) => Unit] = None,
    initOpt: Option[(Computation[E]) => Unit] = None,
    initAggregationsOpt: Option[(Computation[E]) => Unit] = None,
    finishOpt: Option[(Computation[E]) => Unit] = None,
    expandComputeOpt: Option[(E,Computation[E]) => Iterator[E]] = None,
    nextComputationOpt: Option[Computation[E]] = None)
  extends VertexInducedComputation[E] with ComputationContainer[E] {

  @transient private lazy val _process: (E,Computation[E]) => Unit =
    processOpt.getOrElse ((e: E, c: Computation[E]) => super.process (e))
  
  @transient private lazy val _filter: (E,Computation[E]) => Boolean =
    filterOpt.getOrElse ((e: E, c: Computation[E]) => super.filter (e))

  @transient private lazy val _shouldExpand: (E,Computation[E]) => Boolean =
    shouldExpandOpt.getOrElse ((e: E, c: Computation[E]) => super.shouldExpand(e))
  
  @transient private lazy val _aggregationFilter: (E,Computation[E]) => Boolean =
    aggregationFilterOpt.getOrElse ((e: E, c: Computation[E]) => super.aggregationFilter (e))
  
  @transient private lazy val _pAggregationFilter: (Pattern,Computation[E]) => Boolean =
    pAggregationFilterOpt.getOrElse ((p: Pattern, c: Computation[E]) => super.aggregationFilter (p))
  
  @transient private lazy val _aggregationProcess: (E,Computation[E]) => Unit =
    aggregationProcessOpt.getOrElse ((e: E, c: Computation[E]) => super.aggregationProcess (e))
  
  @transient private lazy val _handleNoExpansions: (E,Computation[E]) => Unit =
    handleNoExpansionsOpt.getOrElse ((e: E, c: Computation[E]) => super.handleNoExpansions (e))
  
  @transient private lazy val _init: (Configuration[E], Computation[E]) => Unit = initOpt match {
    case Some(thisInit) =>
      (config: Configuration[E], c: Computation[E]) => {super.init(config); thisInit(c)}
    case None =>
      (config: Configuration[E], c: Computation[E]) => {super.init(config)}
  }

  @transient private lazy val _initAggregations: (Configuration[E], Computation[E]) => Unit =
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

  @transient private lazy val _finish: (Computation[E]) => Unit = finishOpt match {
    case Some(thisFinish) =>
      (c: Computation[E]) => {super.finish(); thisFinish(c)}
    case None =>
      (c: Computation[E]) => {super.finish()}
  }
  
  @transient private lazy val _expandCompute
    : (E,Computation[E]) => Iterator[E] =
    expandComputeOpt.getOrElse (
      (e: E, c: Computation[E]) => super.expandCompute(e)
    )
  
  @transient private lazy val _nextComputation
    : Computation[E] = nextComputationOpt match {
    case Some(nc) => nc
    case None => null
  }


  def withNewFunctions(
      processOpt: Option[(E,Computation[E]) => Unit] =
        lastComputation.processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] =
        lastComputation.filterOpt,
      shouldExpandOpt: Option[(E,Computation[E]) => Boolean] =
        lastComputation.shouldExpandOpt,
      aggregationFilterOpt: Option[(E,Computation[E]) => Boolean] =
        lastComputation.aggregationFilterOpt,
      pAggregationFilterOpt: Option[(Pattern,Computation[E]) => Boolean] =
        lastComputation.pAggregationFilterOpt,
      aggregationProcessOpt: Option[(E,Computation[E]) => Unit] =
        lastComputation.aggregationProcessOpt,
      handleNoExpansionsOpt: Option[(E,Computation[E]) => Unit] = 
        lastComputation.handleNoExpansionsOpt,
      initOpt: Option[(Computation[E]) => Unit] =
        lastComputation.initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] =
        lastComputation.initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] =
        lastComputation.finishOpt,
      expandComputeOpt: Option[(E,Computation[E]) => Iterator[E]] =
        lastComputation.expandComputeOpt)
    : ComputationContainer[E] = {

    val comps = new Stack[VComputationContainer[E]]()
    var currOpt: Option[VComputationContainer[E]] = Option(this)
    while (currOpt.isDefined) {
      comps.push(currOpt.get)
      currOpt = currOpt.get.nextComputationOpt.
        asInstanceOf[Option[VComputationContainer[E]]]
    }

    var lastComp = comps.pop()
    lastComp = lastComp.copy(processOpt, filterOpt, shouldExpandOpt,
        aggregationFilterOpt, pAggregationFilterOpt, aggregationProcessOpt,
        handleNoExpansionsOpt, initOpt, initAggregationsOpt, finishOpt,
        expandComputeOpt)

    while (!comps.isEmpty) {
      lastComp = comps.pop().copy(nextComputationOpt = Some(lastComp))
    }

    lastComp
  }
  
  def withComputationAppended(lastComputation: Computation[E])
    : ComputationContainer[E] = nextComputationOpt match {
      case Some(nextComputation) =>
        val container = nextComputation.asInstanceOf[ComputationContainer[E]]
        val _nextComputation = container.
          withComputationAppended(lastComputation)
        this.copy(nextComputationOpt = Some(_nextComputation))
      case None =>
        this.copy(nextComputationOpt = Some(lastComputation))
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

  override def process(e: E): Unit = _process (e, this)

  override def filter(e: E): Boolean = _filter (e, this)

  override def shouldExpand(e: E): Boolean = _shouldExpand (e, this)

  override def aggregationFilter(e: E): Boolean = _aggregationFilter (e, this)

  override def aggregationFilter(p: Pattern): Boolean =
    _pAggregationFilter (p, this)

  override def aggregationProcess(e: E): Unit = _aggregationProcess (e, this)

  override def handleNoExpansions(e: E): Unit = _handleNoExpansions (e, this)

  override def init(config: Configuration[E]): Unit = _init (config, this)

  override def initAggregations(config: Configuration[E]): Unit =
    _initAggregations (config, this)

  override def finish(): Unit = _finish (this)

  override def expandCompute(e: E): java.util.Iterator[E] =
    _expandCompute (e, this).asJava
  
  override def nextComputation(): Computation[E] = _nextComputation
  
  override def toString: String = s"V${super.toString}"
}

object ComputationContainer {
  val nextContainerId = new AtomicInteger(0)
}
