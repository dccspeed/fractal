package br.ufmg.cs.systems.fractal.computation

import java.util.concurrent.atomic.AtomicInteger

import br.ufmg.cs.systems.fractal.Primitive
import br.ufmg.cs.systems.fractal.conf.Configuration
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.Logging

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

   val initOpt: Option[(Computation[E]) => Unit]

   val initAggregationsOpt: Option[(Computation[E]) => Unit]

   val finishOpt: Option[(Computation[E]) => Unit]

   val expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]]

   val shouldBypassOpt: Option[Boolean]

   val processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long]

   val nextComputationOpt: Option[Computation[E]]

   @transient lazy val lastComputationContainer: ComputationContainer[E] = {
      nextComputationOpt match {
         case Some(nextComputation: ComputationContainer[E]) =>
            nextComputation.lastComputationContainer
         case _ =>
            this
      }
   }

   override def lastComputation(): Computation[E] = lastComputationContainer

   def initContainerFunctions(): Unit

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
      // When creating an empty computation for pattern-induced fractoids, new
      // containers lose this pattern.
      //patternOpt = None,
      processOpt = Some((e,c) => {}),
      filterOpt = Some((e,c) => true),
      //expandComputeOpt = Some((e,c) => Iterator.empty)
      expandComputeOpt = None
   )

   @transient lazy val computationRepr: Array[String] = {
      Array(
         shouldBypassOpt.map(b => s"bypass=${b}").getOrElse("bypass=false"),
         expandComputeOpt.map(_ => "ec").getOrElse("_"),
         filterOpt.map(_ => "f").getOrElse("_"),
         processComputeOpt.map(_.toString()).getOrElse("_"),
         processOpt.map(_ => "p").getOrElse("_"))
   }

   def primitives(): Array[Primitive] = nextComputationOpt match {
      case Some(c: ComputationContainer[E]) =>
         Array(primitiveOpt.getOrElse(Primitive.None)) ++ c.primitives()
      case Some(c) =>
         throw new RuntimeException(s"Computation ${c} is not a container.")
      case None =>
         Array(primitiveOpt.getOrElse(Primitive.None))
   }

   def toStringPrimitives: String = {
      var curr = this
      var lastPrimitive = curr.primitiveOpt.getOrElse(Primitive.None)
      var lastPrimitiveCount = 1
      var str = ""
      curr = curr.nextComputationOpt.getOrElse(null)
         .asInstanceOf[ComputationContainer[E]]
      while (curr != null) {
         val primitive = curr.primitiveOpt.getOrElse(Primitive.None)
         if (lastPrimitive != primitive) {
            str = s"${str}${lastPrimitive.name()}(${lastPrimitiveCount})"
            lastPrimitive = primitive
            lastPrimitiveCount = 1
         } else {
            lastPrimitiveCount += 1
         }
         curr = curr.nextComputationOpt.getOrElse(null)
            .asInstanceOf[ComputationContainer[E]]
      }
      s"${str}${lastPrimitive.name()}(${lastPrimitiveCount})"
   }

   override def toString: String = {
      toStringPrimitives
      //s"CC[${primitiveOpt.getOrElse(Primitive.None).name()}]" +
         //s"[${containerId}]" +
         //s"[${computationLabel()}]" +
         //s"(${computationRepr.mkString(",")})" +
         //s"${nextComputationOpt.map(c => "::" + c.toString).getOrElse("")}"
   }
}

case class EComputationContainer [E <: EdgeInducedSubgraph] (
                                                               primitiveOpt: Option[Primitive] = None,
                                                               computationLabelOpt: Option[String] = None,
                                                               patternOpt: Option[Pattern] = None,
                                                               processOpt: Option[(E,Computation[E]) => Unit] = None,
                                                               filterOpt: Option[(E,Computation[E]) => Boolean] = None,
                                                               initOpt: Option[(Computation[E]) => Unit] = None,
                                                               initAggregationsOpt: Option[(Computation[E]) => Unit] = None,
                                                               finishOpt: Option[(Computation[E]) => Unit] = None,
                                                               expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] = None,
                                                               shouldBypassOpt: Option[Boolean] = None,
                                                               processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
                                                               None,
                                                               nextComputationOpt: Option[Computation[E]] = None)
   extends EdgeInducedComputation[E] with ComputationContainer[E] {

   initContainerFunctions()

   private var _primitive: Primitive = _
   private var _computationLabel: String = _
   private var _pattern: Pattern = _
   private var _process: (E,Computation[E]) => Unit = _
   private var _filter: (E,Computation[E]) => Boolean = _
   private var _init: (Configuration[E], Computation[E]) => Unit = _
   private var _initAggregations: (Configuration[E], Computation[E]) => Unit = _
   private var _finish: (Computation[E]) => Unit = _
   private var _expandCompute: (E,Computation[E]) => SubgraphEnumerator[E] = _
   private var _shouldBypass: Boolean = _
   private var _processCompute
   : (SubgraphEnumerator[E],Computation[E]) => Long = _
   private var _nextComputation: Computation[E] = _

   override def computationLabel(): String = _computationLabel
   override def process(e: E): Unit = _process (e, this)
   override def filter(e: E): Boolean = _filter (e, this)
   override def init(config: Configuration[E]): Unit = _init (config, this)
   override def initAggregations(config: Configuration[E]): Unit =
      _initAggregations (config, this)
   override def finish(): Unit = _finish (this)
   override def expandCompute(e: E): SubgraphEnumerator[E] =
      _expandCompute(e, this)
   override def shouldBypass(): Boolean = _shouldBypass
   override def processCompute(iter: SubgraphEnumerator[E]) =
      _processCompute (iter, this)
   override def nextComputation(): Computation[E] = _nextComputation
   override def getPattern(): Pattern = _pattern
   override def primitive(): Primitive = _primitive

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
                          lastComputationContainer.primitiveOpt,
                          computationLabelOpt: Option[String] =
                          lastComputationContainer.computationLabelOpt,
                          patternOpt: Option[Pattern] =
                          patternOpt,
                          processOpt: Option[(E,Computation[E]) => Unit] =
                          lastComputationContainer.processOpt,
                          filterOpt: Option[(E,Computation[E]) => Boolean] =
                          lastComputationContainer.filterOpt,
                          initOpt: Option[(Computation[E]) => Unit] =
                          lastComputationContainer.initOpt,
                          initAggregationsOpt: Option[(Computation[E]) => Unit] =
                          lastComputationContainer.initAggregationsOpt,
                          finishOpt: Option[(Computation[E]) => Unit] =
                          lastComputationContainer.finishOpt,
                          expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] =
                          lastComputationContainer.expandComputeOpt,
                          shouldBypassOpt: Option[Boolean] =
                          lastComputationContainer.shouldBypassOpt,
                          processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
                          lastComputationContainer.processComputeOpt)
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
         processOpt, filterOpt,
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
               processOpt, filterOpt,
               initOpt, initAggregationsOpt,
               finishOpt, expandComputeOpt, shouldBypassOpt, processComputeOpt)
         this.copy (primitiveOpt = primitiveOpt,
            computationLabelOpt = computationLabelOpt,
            patternOpt = patternOpt,
            processOpt = processOpt, filterOpt = filterOpt,
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

   override def initContainerFunctions(): Unit = {
      _processCompute = processComputeOpt.getOrElse ((_,_) => -1)
      _nextComputation = nextComputationOpt.getOrElse(null)
      _shouldBypass = shouldBypassOpt.getOrElse(super.shouldBypass())
      _expandCompute = expandComputeOpt.getOrElse (
         (e: E, c: Computation[E]) => super.expandCompute(e)
      )
      _finish = finishOpt match {
         case Some(thisFinish) =>
            (c: Computation[E]) => {super.finish(); thisFinish(c)}
         case None =>
            (c: Computation[E]) => {super.finish()}
      }
      _initAggregations = initAggregationsOpt match {
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
      _init = initOpt match {
         case Some(thisInit) =>
            (config: Configuration[E], c: Computation[E]) => {
               super.init(config); thisInit(c)
            }
         case None =>
            (config: Configuration[E], c: Computation[E]) => {
               super.init(config)
            }
      }
      _filter =  filterOpt
         .getOrElse ((e: E, c: Computation[E]) => super.filter(e))
      _process = processOpt
         .getOrElse ((e: E, c: Computation[E]) => super.process (e))
      _pattern = patternOpt.getOrElse(null)
      _computationLabel = computationLabelOpt.getOrElse (containerId.toString)
      _primitive = primitiveOpt.getOrElse (Primitive.None)
   }
}

case class VComputationContainer [E <: VertexInducedSubgraph] (
                                                                 primitiveOpt: Option[Primitive] = None,
                                                                 computationLabelOpt: Option[String] = None,
                                                                 patternOpt: Option[Pattern] = None,
                                                                 processOpt: Option[(E,Computation[E]) => Unit] = None,
                                                                 filterOpt: Option[(E,Computation[E]) => Boolean] = None,
                                                                 initOpt: Option[(Computation[E]) => Unit] = None,
                                                                 initAggregationsOpt: Option[(Computation[E]) => Unit] = None,
                                                                 finishOpt: Option[(Computation[E]) => Unit] = None,
                                                                 expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] = None,
                                                                 shouldBypassOpt: Option[Boolean] = None,
                                                                 processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
                                                                 None,
                                                                 nextComputationOpt: Option[Computation[E]] = None)
   extends VertexInducedComputation[E] with ComputationContainer[E] {

   initContainerFunctions()

   private var _primitive: Primitive = _
   private var _computationLabel: String = _
   private var _pattern: Pattern = _
   private var _process: (E,Computation[E]) => Unit = _
   private var _filter: (E,Computation[E]) => Boolean = _
   private var _init: (Configuration[E], Computation[E]) => Unit = _
   private var _initAggregations: (Configuration[E], Computation[E]) => Unit = _
   private var _finish: (Computation[E]) => Unit = _
   private var _expandCompute: (E,Computation[E]) => SubgraphEnumerator[E] = _
   private var _shouldBypass: Boolean = _
   private var _processCompute
   : (SubgraphEnumerator[E],Computation[E]) => Long = _
   private var _nextComputation: Computation[E] = _

   override def computationLabel(): String = _computationLabel
   override def process(e: E): Unit = _process (e, this)
   override def filter(e: E): Boolean = _filter (e, this)
   override def init(config: Configuration[E]): Unit = _init (config, this)
   override def initAggregations(config: Configuration[E]): Unit =
      _initAggregations (config, this)
   override def finish(): Unit = _finish (this)
   override def expandCompute(e: E): SubgraphEnumerator[E] =
      _expandCompute(e, this)
   override def shouldBypass(): Boolean = _shouldBypass
   override def processCompute(iter: SubgraphEnumerator[E]) =
      _processCompute (iter, this)
   override def nextComputation(): Computation[E] = _nextComputation
   override def getPattern(): Pattern = _pattern
   override def primitive(): Primitive = _primitive

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
                          lastComputationContainer.primitiveOpt,
                          computationLabelOpt: Option[String] =
                          lastComputationContainer.computationLabelOpt,
                          patternOpt: Option[Pattern] =
                          patternOpt,
                          processOpt: Option[(E,Computation[E]) => Unit] =
                          lastComputationContainer.processOpt,
                          filterOpt: Option[(E,Computation[E]) => Boolean] =
                          lastComputationContainer.filterOpt,
                          initOpt: Option[(Computation[E]) => Unit] =
                          lastComputationContainer.initOpt,
                          initAggregationsOpt: Option[(Computation[E]) => Unit] =
                          lastComputationContainer.initAggregationsOpt,
                          finishOpt: Option[(Computation[E]) => Unit] =
                          lastComputationContainer.finishOpt,
                          expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] =
                          lastComputationContainer.expandComputeOpt,
                          shouldBypassOpt: Option[Boolean] =
                          lastComputationContainer.shouldBypassOpt,
                          processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
                          lastComputationContainer.processComputeOpt)
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
         processOpt, filterOpt,
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
               processOpt, filterOpt,
               initOpt, initAggregationsOpt,
               finishOpt, expandComputeOpt, shouldBypassOpt, processComputeOpt)
         this.copy (primitiveOpt = primitiveOpt,
            computationLabelOpt = computationLabelOpt,
            patternOpt = patternOpt,
            processOpt = processOpt, filterOpt = filterOpt,
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

   override def initContainerFunctions(): Unit = {
      _processCompute = processComputeOpt.getOrElse ((_,_) => -1)
      _nextComputation = nextComputationOpt.getOrElse(null)
      _shouldBypass = shouldBypassOpt.getOrElse(super.shouldBypass())
      _expandCompute = expandComputeOpt.getOrElse (
         (e: E, c: Computation[E]) => super.expandCompute(e)
      )
      _finish = finishOpt match {
         case Some(thisFinish) =>
            (c: Computation[E]) => {super.finish(); thisFinish(c)}
         case None =>
            (c: Computation[E]) => {super.finish()}
      }
      _initAggregations = initAggregationsOpt match {
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
      _init = initOpt match {
         case Some(thisInit) =>
            (config: Configuration[E], c: Computation[E]) => {
               super.init(config); thisInit(c)
            }
         case None =>
            (config: Configuration[E], c: Computation[E]) => {
               super.init(config)
            }
      }
      _filter =  filterOpt
         .getOrElse ((e: E, c: Computation[E]) => super.filter(e))
      _process = processOpt
         .getOrElse ((e: E, c: Computation[E]) => super.process (e))
      _pattern = patternOpt.getOrElse(null)
      _computationLabel = computationLabelOpt.getOrElse (containerId.toString)
      _primitive = primitiveOpt.getOrElse (Primitive.None)
   }
}

case class VEComputationContainer [E <: PatternInducedSubgraph](
                                                                  primitiveOpt: Option[Primitive] = None,
                                                                  computationLabelOpt: Option[String] = None,
                                                                  patternOpt: Option[Pattern] = None,
                                                                  processOpt: Option[(E,Computation[E]) => Unit] = None,
                                                                  filterOpt: Option[(E,Computation[E]) => Boolean] = None,
                                                                  initOpt: Option[(Computation[E]) => Unit] = None,
                                                                  initAggregationsOpt: Option[(Computation[E]) => Unit] = None,
                                                                  finishOpt: Option[(Computation[E]) => Unit] = None,
                                                                  expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] = None,
                                                                  shouldBypassOpt: Option[Boolean] = None,
                                                                  processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
                                                                  None,
                                                                  nextComputationOpt: Option[Computation[E]] = None)
   extends PatternInducedComputation[E] with ComputationContainer[E] {

   initContainerFunctions()

   private var _primitive: Primitive = _
   private var _computationLabel: String = _
   private var _pattern: Pattern = _
   private var _process: (E,Computation[E]) => Unit = _
   private var _filter: (E,Computation[E]) => Boolean = _
   private var _patternInit: (Configuration[E], Computation[E]) => Unit = _
   private var _init: (Configuration[E], Computation[E]) => Unit = _
   private var _initAggregations: (Configuration[E], Computation[E]) => Unit = _
   private var _finish: (Computation[E]) => Unit = _
   private var _expandCompute: (E,Computation[E]) => SubgraphEnumerator[E] = _
   private var _shouldBypass: Boolean = _
   private var _processCompute
   : (SubgraphEnumerator[E],Computation[E]) => Long = _
   private var _nextComputation: Computation[E] = _

   override def computationLabel(): String = _computationLabel
   override def process(e: E): Unit = _process (e, this)
   override def filter(e: E): Boolean = _filter (e, this)
   override def init(config: Configuration[E]): Unit = {
      _patternInit(config, this)
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
   override def primitive(): Primitive = _primitive

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
                          lastComputationContainer.primitiveOpt,
                          computationLabelOpt: Option[String] =
                          lastComputationContainer.computationLabelOpt,
                          patternOpt: Option[Pattern] =
                          patternOpt,
                          processOpt: Option[(E,Computation[E]) => Unit] =
                          lastComputationContainer.processOpt,
                          filterOpt: Option[(E,Computation[E]) => Boolean] =
                          lastComputationContainer.filterOpt,
                          initOpt: Option[(Computation[E]) => Unit] =
                          lastComputationContainer.initOpt,
                          initAggregationsOpt: Option[(Computation[E]) => Unit] =
                          lastComputationContainer.initAggregationsOpt,
                          finishOpt: Option[(Computation[E]) => Unit] =
                          lastComputationContainer.finishOpt,
                          expandComputeOpt: Option[(E,Computation[E]) => SubgraphEnumerator[E]] =
                          lastComputationContainer.expandComputeOpt,
                          shouldBypassOpt: Option[Boolean] =
                          lastComputationContainer.shouldBypassOpt,
                          processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
                          lastComputationContainer.processComputeOpt)
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
         processOpt, filterOpt,
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
               processOpt, filterOpt,
               initOpt, initAggregationsOpt,
               finishOpt, expandComputeOpt, shouldBypassOpt, processComputeOpt)
         this.copy (primitiveOpt = primitiveOpt,
            computationLabelOpt = computationLabelOpt,
            patternOpt = patternOpt,
            processOpt = processOpt, filterOpt = filterOpt,
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


   override def initContainerFunctions(): Unit = {
      _processCompute = processComputeOpt.getOrElse ((_,_) => -1)
      _nextComputation = nextComputationOpt.getOrElse(null)
      _shouldBypass = shouldBypassOpt.getOrElse(super.shouldBypass())
      _expandCompute = expandComputeOpt.getOrElse (
         (e: E, c: Computation[E]) => super.expandCompute(e)
      )
      _finish = finishOpt match {
         case Some(thisFinish) =>
            (c: Computation[E]) => {super.finish(); thisFinish(c)}
         case None =>
            (c: Computation[E]) => {super.finish()}
      }
      _initAggregations = initAggregationsOpt match {
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
      _init = initOpt match {
         case Some(thisInit) =>
            (config: Configuration[E], c: Computation[E]) => {
               super.init(config); thisInit(c)
            }
         case None =>
            (config: Configuration[E], c: Computation[E]) => {
               super.init(config)
            }
      }
      _patternInit = (config: Configuration[E], c: Computation[E]) => {
         val pattern = getPattern()
         if (pattern != null) {
            patternOpt.map(_.getConfig().asInstanceOf[Configuration[E]]) match {
               case Some(conf) =>
                  //conf.initialize()
                  pattern.init(config)
               case None =>
                  throw new RuntimeException(
                     s"Invalid state, pattern configuration is missing.")
            }
         }
      }
      _filter =  filterOpt
         .getOrElse ((e: E, c: Computation[E]) => super.filter(e))
      _process = processOpt
         .getOrElse ((e: E, c: Computation[E]) => super.process (e))
      _pattern = patternOpt.getOrElse(null)
      _computationLabel = computationLabelOpt.getOrElse (containerId.toString)
      _primitive = primitiveOpt.getOrElse (Primitive.None)
   }
}

object ComputationContainer {
   val nextContainerId = new AtomicInteger(0)
}


