package br.ufmg.cs.systems.fractal.computation

import java.util.concurrent.atomic.AtomicInteger

import br.ufmg.cs.systems.fractal.Primitive
import br.ufmg.cs.systems.fractal.computation.vertexinduced.VertexInducedComputation
import br.ufmg.cs.systems.fractal.conf.Configuration
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.Logging

import scala.collection.mutable.Stack

// TODO: Refactor me!
sealed trait ComputationContainer [E <: Subgraph] extends Computation[E]
   with Logging {

   val containerId: Int = ComputationContainer.nextContainerId
      .getAndIncrement

   val computationLabelOpt: Option[String]

   val patternOpt: Option[Pattern]

   val processOpt: Option[(E,Computation[E]) => Unit]

   val filterOpt: Option[(E,Computation[E]) => Boolean]

   val initOpt: Option[(Computation[E]) => Unit]

   val initAggregationsOpt: Option[(Computation[E]) => Unit]

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
                     primitive: Primitive = primitive,
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
                     shouldBypassOpt: Option[Boolean] =
                     shouldBypassOpt,
                     processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
                     processComputeOpt,
                     nextComputationOpt: Option[Computation[E]] =
                     nextComputationOpt
                  ): ComputationContainer[E]

   def withNewFunctions(
                          primitive: Primitive = primitive,
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
                          shouldBypassOpt: Option[Boolean] =
                          shouldBypassOpt,
                          processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
                          processComputeOpt
                       ): ComputationContainer[E]

   def withNewFunctionsAll(
                             primitive: Primitive = primitive,
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
                             shouldBypassOpt: Option[Boolean] =
                             shouldBypassOpt,
                             processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
                             processComputeOpt
                          ): ComputationContainer[E]

   def shallowCopy(): ComputationContainer[E]

   def clear(): ComputationContainer[E] = withNewFunctions (
      primitive = Primitive.None,
      computationLabelOpt = None,
      // When creating an empty computation for pattern-induced fractoids, new
      // containers lose this pattern.
      //patternOpt = None,
      processOpt = Some((e,c) => {}),
      filterOpt = Some((e,c) => true)
   )

   @transient lazy val computationRepr: Array[String] = {
      Array(
         shouldBypassOpt.map(b => s"bypass=${b}").getOrElse("bypass=false"),
         filterOpt.map(_ => "f").getOrElse("_"),
         processComputeOpt.map(_.toString()).getOrElse("_"),
         processOpt.map(_ => "p").getOrElse("_"))
   }

   override def primitives(): Array[Primitive] = nextComputationOpt match {
      case Some(c: ComputationContainer[E]) =>
         Array(primitive) ++ c.primitives()
      case Some(c) =>
         throw new RuntimeException(s"Computation ${c} is not a container.")
      case None =>
         Array(primitive)
   }

   def toStringPrimitives: String = {
      var curr = this
      var lastPrimitive = curr.primitive
      var lastPrimitiveCount = 1
      var str = ""
      curr = curr.nextComputationOpt.getOrElse(null)
         .asInstanceOf[ComputationContainer[E]]
      while (curr != null) {
         val primitive = curr.primitive
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

   //override def toString: String = {
   //   toStringPrimitives
   //   //s"CC[${primitiveOpt.getOrElse(Primitive.None).name()}]" +
   //      //s"[${containerId}]" +
   //      //s"(${computationRepr.mkString(",")})" +
   //      //s"${nextComputationOpt.map(c => "::" + c.toString).getOrElse("")}"
   //}
}

case class EComputationContainer [E <: EdgeInducedSubgraph]
(primitive: Primitive = Primitive.None,
 computationLabelOpt: Option[String] = None,
 patternOpt: Option[Pattern] = None,
 processOpt: Option[(E,Computation[E]) => Unit] = None,
 filterOpt: Option[(E,Computation[E]) => Boolean] = None,
 initOpt: Option[(Computation[E]) => Unit] = None,
 initAggregationsOpt: Option[(Computation[E]) => Unit] = None,
 shouldBypassOpt: Option[Boolean] = None,
 processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
 None,
 nextComputationOpt: Option[Computation[E]] = None)
   extends EdgeInducedComputation[E] with ComputationContainer[E] {

   initContainerFunctions()

   private var _computationLabel: String = _
   private var _pattern: Pattern = _
   private var _process: (E,Computation[E]) => Unit = _
   private var _filter: (E,Computation[E]) => Boolean = _
   private var _init: (Configuration, Computation[E]) => Unit = _
   private var _initAggregations: (Configuration, Computation[E]) => Unit = _
   private var _shouldBypass: Boolean = _
   private var _processCompute
   : (SubgraphEnumerator[E],Computation[E]) => Long = _
   private var _nextComputation: Computation[E] = _

   override def process(e: E): Unit = _process (e, this)
   override def filter_FILTERING_PRIMITIVE(e: E): Boolean = _filter (e, this)
   override def init(config: Configuration): Unit = _init (config, this)
   override def initAggregations(config: Configuration): Unit =
      _initAggregations (config, this)
   override def shouldBypass(): Boolean = _shouldBypass
   override def processCompute(iter: SubgraphEnumerator[E]) =
      _processCompute (iter, this)
   override def nextComputation(): Computation[E] = _nextComputation
   override def getPattern(): Pattern = _pattern

   def shallowCopy(): ComputationContainer[E] = nextComputationOpt match {
      case Some(nextComputation : ComputationContainer[E]) =>
         this.copy(
            primitive = primitive,
            computationLabelOpt =
               Option(computationLabelOpt.getOrElse(containerId.toString)),
            nextComputationOpt = Option(nextComputation.shallowCopy()))
      case None =>
         this.copy(
            primitive = primitive,
            computationLabelOpt =
               Option(computationLabelOpt.getOrElse(containerId.toString)))
      case _ =>
         throw new RuntimeException(s"Next computation should be a container")
   }

   def shallowCopy(
                     primitive: Primitive = primitive,
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
                     shouldBypassOpt: Option[Boolean] =
                     shouldBypassOpt,
                     processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
                     processComputeOpt,
                     nextComputationOpt: Option[Computation[E]] =
                     nextComputationOpt)
   : ComputationContainer[E] = {
      this.copy(
         primitive = primitive,
         computationLabelOpt = computationLabelOpt,
         patternOpt = patternOpt,
         processOpt = processOpt,
         filterOpt = filterOpt,
         initOpt = initOpt,
         initAggregationsOpt = initAggregationsOpt,
         shouldBypassOpt = shouldBypassOpt,
         processComputeOpt = processComputeOpt,
         nextComputationOpt = nextComputationOpt
      )
   }

   def withNewFunctions(
                          primitive: Primitive =
                          lastComputationContainer.primitive,
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
      lastComp = lastComp.copy(primitive, computationLabelOpt, patternOpt,
         processOpt, filterOpt,
         initOpt, initAggregationsOpt,
         shouldBypassOpt, processComputeOpt)

      while (!comps.isEmpty) {
         lastComp = comps.pop().copy(nextComputationOpt = Some(lastComp))
      }

      lastComp
   }

   def withNewFunctionsAll(
                             primitive: Primitive = primitive,
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
                             shouldBypassOpt: Option[Boolean] =
                             shouldBypassOpt,
                             processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
                             processComputeOpt)
   : ComputationContainer[E] = nextComputationOpt match {
      case Some(nextComputation) =>
         val nextComp = nextComputation.asInstanceOf[ComputationContainer[E]].
            withNewFunctionsAll (primitive, computationLabelOpt, patternOpt,
               processOpt, filterOpt,
               initOpt, initAggregationsOpt,
               shouldBypassOpt, processComputeOpt)
         this.copy (primitive = primitive,
            computationLabelOpt = computationLabelOpt,
            patternOpt = patternOpt,
            processOpt = processOpt, filterOpt = filterOpt,
            initOpt = initOpt, initAggregationsOpt = initAggregationsOpt,
            shouldBypassOpt = shouldBypassOpt,
            processComputeOpt = processComputeOpt,
            nextComputationOpt = Some(nextComp))

      case None =>
         this.copy (primitive = primitive,
            computationLabelOpt = computationLabelOpt,
            patternOpt = patternOpt,
            processOpt = processOpt, filterOpt = filterOpt,
            initOpt = initOpt, initAggregationsOpt = initAggregationsOpt,
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
      this.copy (primitive = p)
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
      _initAggregations = initAggregationsOpt match {
         case Some(thisInitAggregations) =>
            (config: Configuration, c: Computation[E]) => {
               super.initAggregations(config)
               thisInitAggregations(c)
            }

         case None =>
            (config: Configuration, c: Computation[E]) => {
               super.initAggregations(config)
            }
      }
      _init = initOpt match {
         case Some(thisInit) =>
            (config: Configuration, c: Computation[E]) => {
               super.init(config); thisInit(c)
            }
         case None =>
            (config: Configuration, c: Computation[E]) => {
               super.init(config)
            }
      }
      _filter =  filterOpt
         .getOrElse ((e: E, c: Computation[E]) => super.filter_FILTERING_PRIMITIVE(e))
      _process = processOpt
         .getOrElse ((e: E, c: Computation[E]) => super.process (e))
      _pattern = patternOpt.getOrElse(null)
      _computationLabel = computationLabelOpt.getOrElse (containerId.toString)
   }
}

case class VComputationContainer [E <: VertexInducedSubgraph]
(primitive: Primitive = Primitive.None,
 computationLabelOpt: Option[String] = None,
 patternOpt: Option[Pattern] = None,
 processOpt: Option[(E,Computation[E]) => Unit] = None,
 filterOpt: Option[(E,Computation[E]) => Boolean] = None,
 initOpt: Option[(Computation[E]) => Unit] = None,
 initAggregationsOpt: Option[(Computation[E]) => Unit] = None,
 shouldBypassOpt: Option[Boolean] = None,
 processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] = None,
 nextComputationOpt: Option[Computation[E]] = None)
   extends VertexInducedComputation[E] with ComputationContainer[E] {

   initContainerFunctions()

   private var _computationLabel: String = _
   private var _pattern: Pattern = _
   private var _process: (E,Computation[E]) => Unit = _
   private var _filter: (E,Computation[E]) => Boolean = _
   private var _init: (Configuration, Computation[E]) => Unit = _
   private var _initAggregations: (Configuration, Computation[E]) => Unit = _
   private var _shouldBypass: Boolean = _
   private var _processCompute: (SubgraphEnumerator[E],Computation[E]) => Long = _
   private var _nextComputation: Computation[E] = _

   override def process(e: E): Unit = _process (e, this)
   override def filter_FILTERING_PRIMITIVE(e: E): Boolean = _filter (e, this)
   override def init(config: Configuration): Unit = _init (config, this)
   override def initAggregations(config: Configuration): Unit =
      _initAggregations (config, this)
   override def shouldBypass(): Boolean = _shouldBypass
   override def processCompute(iter: SubgraphEnumerator[E]) =
      _processCompute (iter, this)
   override def nextComputation(): Computation[E] = _nextComputation
   override def getPattern(): Pattern = _pattern

   def shallowCopy(): ComputationContainer[E] = nextComputationOpt match {
      case Some(nextComputation : ComputationContainer[E]) =>
         this.copy(
            primitive = Primitive.None,
            computationLabelOpt =
               Option(computationLabelOpt.getOrElse(containerId.toString)),
            nextComputationOpt = Option(nextComputation.shallowCopy()))
      case None =>
         this.copy(
            primitive = Primitive.None,
            computationLabelOpt =
               Option(computationLabelOpt.getOrElse(containerId.toString)))
      case _ =>
         throw new RuntimeException(s"Next computation should be a container")
   }

   def shallowCopy(
                     primitive: Primitive = primitive,
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
                     shouldBypassOpt: Option[Boolean] =
                     shouldBypassOpt,
                     processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
                     processComputeOpt,
                     nextComputationOpt: Option[Computation[E]] =
                     nextComputationOpt)
   : ComputationContainer[E] = {
      this.copy(
         primitive = primitive,
         computationLabelOpt = computationLabelOpt,
         patternOpt = patternOpt,
         processOpt = processOpt,
         filterOpt = filterOpt,
         initOpt = initOpt,
         initAggregationsOpt = initAggregationsOpt,
         shouldBypassOpt = shouldBypassOpt,
         processComputeOpt = processComputeOpt,
         nextComputationOpt = nextComputationOpt
      )
   }

   def withNewFunctions(
                          primitive: Primitive =
                          lastComputationContainer.primitive,
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
      lastComp = lastComp.copy(primitive, computationLabelOpt, patternOpt,
         processOpt, filterOpt,
         initOpt, initAggregationsOpt,
         shouldBypassOpt, processComputeOpt)

      while (!comps.isEmpty) {
         lastComp = comps.pop().copy(nextComputationOpt = Some(lastComp))
      }

      lastComp
   }

   def withNewFunctionsAll(
                             primitive: Primitive = primitive,
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
                             shouldBypassOpt: Option[Boolean] =
                             shouldBypassOpt,
                             processComputeOpt: Option[(SubgraphEnumerator[E],Computation[E]) => Long] =
                             processComputeOpt)
   : ComputationContainer[E] = nextComputationOpt match {
      case Some(nextComputation) =>
         val nextComp = nextComputation.asInstanceOf[ComputationContainer[E]].
            withNewFunctionsAll (primitive, computationLabelOpt, patternOpt,
               processOpt, filterOpt,
               initOpt, initAggregationsOpt,
               shouldBypassOpt, processComputeOpt)
         this.copy (primitive = primitive,
            computationLabelOpt = computationLabelOpt,
            patternOpt = patternOpt,
            processOpt = processOpt, filterOpt = filterOpt,
            initOpt = initOpt, initAggregationsOpt = initAggregationsOpt,
            shouldBypassOpt = shouldBypassOpt,
            processComputeOpt = processComputeOpt,
            nextComputationOpt = Some(nextComp))

      case None =>
         this.copy (primitive = primitive,
            computationLabelOpt = computationLabelOpt,
            patternOpt = patternOpt,
            processOpt = processOpt, filterOpt = filterOpt,
            initOpt = initOpt, initAggregationsOpt = initAggregationsOpt,
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
      this.copy (primitive = p)
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
      _initAggregations = initAggregationsOpt match {
         case Some(thisInitAggregations) =>
            (config: Configuration, c: Computation[E]) => {
               super.initAggregations(config)
               thisInitAggregations(c)
            }

         case None =>
            (config: Configuration, c: Computation[E]) => {
               super.initAggregations(config)
            }
      }
      _init = initOpt match {
         case Some(thisInit) =>
            (config: Configuration, c: Computation[E]) => {
               super.init(config); thisInit(c)
            }
         case None =>
            (config: Configuration, c: Computation[E]) => {
               super.init(config)
            }
      }
      _filter =  filterOpt
         .getOrElse ((e: E, c: Computation[E]) => super.filter_FILTERING_PRIMITIVE(e))
      _process = processOpt
         .getOrElse ((e: E, c: Computation[E]) => super.process (e))
      _pattern = patternOpt.getOrElse(null)
      _computationLabel = computationLabelOpt.getOrElse (containerId.toString)
   }
}

case class VEComputationContainer [S <: PatternInducedSubgraph](
                                                                  primitive: Primitive = Primitive.None,
                                                                  computationLabelOpt: Option[String] = None,
                                                                  patternOpt: Option[Pattern] = None,
                                                                  processOpt: Option[(S,Computation[S]) => Unit] = None,
                                                                  filterOpt: Option[(S,Computation[S]) => Boolean] = None,
                                                                  initOpt: Option[(Computation[S]) => Unit] = None,
                                                                  initAggregationsOpt: Option[(Computation[S]) => Unit] = None,
                                                                  shouldBypassOpt: Option[Boolean] = None,
                                                                  processComputeOpt: Option[(SubgraphEnumerator[S],Computation[S]) => Long] =
                                                                  None,
                                                                  nextComputationOpt: Option[Computation[S]] = None)
   extends PatternInducedComputation[S] with ComputationContainer[S] {

   initContainerFunctions()

   private var _computationLabel: String = _
   private var _pattern: Pattern = _
   private var _process: (S,Computation[S]) => Unit = _
   private var _filter: (S,Computation[S]) => Boolean = _
   private var _patternInit: (Configuration, Computation[S]) => Unit = _
   private var _init: (Configuration, Computation[S]) => Unit = _
   private var _initAggregations: (Configuration, Computation[S]) => Unit = _
   private var _shouldBypass: Boolean = _
   private var _processCompute
   : (SubgraphEnumerator[S],Computation[S]) => Long = _
   private var _nextComputation: Computation[S] = _

   override def process(e: S): Unit = _process (e, this)
   override def filter_FILTERING_PRIMITIVE(e: S): Boolean = _filter (e, this)
   override def init(config: Configuration): Unit = {
      _patternInit(config, this)
      _init (config, this)
   }
   override def initAggregations(config: Configuration): Unit =
      _initAggregations (config, this)
   override def shouldBypass(): Boolean = _shouldBypass
   override def processCompute(iter: SubgraphEnumerator[S]) =
      _processCompute (iter, this)
   override def nextComputation(): Computation[S] = _nextComputation
   override def getPattern(): Pattern = _pattern

   def shallowCopy(): ComputationContainer[S] = nextComputationOpt match {
      case Some(nextComputation : ComputationContainer[S]) =>
         this.copy(
            primitive = primitive,
            computationLabelOpt =
               Option(computationLabelOpt.getOrElse(containerId.toString)),
            nextComputationOpt = Option(nextComputation.shallowCopy()))
      case None =>
         this.copy(
            primitive = primitive,
            computationLabelOpt =
               Option(computationLabelOpt.getOrElse(containerId.toString)))
      case _ =>
         throw new RuntimeException(s"Next computation should be a container")
   }

   def shallowCopy(
                     primitive: Primitive = primitive,
                     computationLabelOpt: Option[String] =
                     computationLabelOpt,
                     patternOpt: Option[Pattern] =
                     patternOpt,
                     processOpt: Option[(S,Computation[S]) => Unit] =
                     processOpt,
                     filterOpt: Option[(S,Computation[S]) => Boolean] =
                     filterOpt,
                     initOpt: Option[(Computation[S]) => Unit] =
                     initOpt,
                     initAggregationsOpt: Option[(Computation[S]) => Unit] =
                     initAggregationsOpt,
                     shouldBypassOpt: Option[Boolean] =
                     shouldBypassOpt,
                     processComputeOpt: Option[(SubgraphEnumerator[S],Computation[S]) => Long] =
                     processComputeOpt,
                     nextComputationOpt: Option[Computation[S]] =
                     nextComputationOpt)
   : ComputationContainer[S] = {
      this.copy(
         primitive = primitive,
         computationLabelOpt = computationLabelOpt,
         patternOpt = patternOpt,
         processOpt = processOpt,
         filterOpt = filterOpt,
         initOpt = initOpt,
         initAggregationsOpt = initAggregationsOpt,
         shouldBypassOpt = shouldBypassOpt,
         processComputeOpt = processComputeOpt,
         nextComputationOpt = nextComputationOpt
      )
   }

   def withNewFunctions(
                          primitive: Primitive =
                          lastComputationContainer.primitive,
                          computationLabelOpt: Option[String] =
                          lastComputationContainer.computationLabelOpt,
                          patternOpt: Option[Pattern] =
                          patternOpt,
                          processOpt: Option[(S,Computation[S]) => Unit] =
                          lastComputationContainer.processOpt,
                          filterOpt: Option[(S,Computation[S]) => Boolean] =
                          lastComputationContainer.filterOpt,
                          initOpt: Option[(Computation[S]) => Unit] =
                          lastComputationContainer.initOpt,
                          initAggregationsOpt: Option[(Computation[S]) => Unit] =
                          lastComputationContainer.initAggregationsOpt,
                          shouldBypassOpt: Option[Boolean] =
                          lastComputationContainer.shouldBypassOpt,
                          processComputeOpt: Option[(SubgraphEnumerator[S],Computation[S]) => Long] =
                          lastComputationContainer.processComputeOpt)
   : ComputationContainer[S] = {

      val comps = new Stack[VEComputationContainer[S]]()
      var currOpt: Option[VEComputationContainer[S]] = Option(this)
      while (currOpt.isDefined) {
         comps.push(currOpt.get)
         currOpt = currOpt.get.nextComputationOpt.
            asInstanceOf[Option[VEComputationContainer[S]]]
      }

      var lastComp = comps.pop()
      lastComp = lastComp.copy(primitive, computationLabelOpt, patternOpt,
         processOpt, filterOpt,
         initOpt, initAggregationsOpt,
         shouldBypassOpt, processComputeOpt)

      while (!comps.isEmpty) {
         lastComp = comps.pop().copy(nextComputationOpt = Some(lastComp))
      }

      lastComp
   }

   def withNewFunctionsAll(
                             primitive: Primitive = primitive,
                             computationLabelOpt: Option[String] =
                             computationLabelOpt,
                             patternOpt: Option[Pattern] =
                             patternOpt,
                             processOpt: Option[(S,Computation[S]) => Unit] =
                             processOpt,
                             filterOpt: Option[(S,Computation[S]) => Boolean] =
                             filterOpt,
                             initOpt: Option[(Computation[S]) => Unit] =
                             initOpt,
                             initAggregationsOpt: Option[(Computation[S]) => Unit] =
                             initAggregationsOpt,
                             shouldBypassOpt: Option[Boolean] =
                             shouldBypassOpt,
                             processComputeOpt: Option[(SubgraphEnumerator[S],Computation[S]) => Long] =
                             processComputeOpt)
   : ComputationContainer[S] = nextComputationOpt match {
      case Some(nextComputation) =>
         val nextComp = nextComputation.asInstanceOf[ComputationContainer[S]].
            withNewFunctionsAll (primitive, computationLabelOpt, patternOpt,
               processOpt, filterOpt,
               initOpt, initAggregationsOpt,
               shouldBypassOpt, processComputeOpt)
         this.copy (primitive = primitive,
            computationLabelOpt = computationLabelOpt,
            patternOpt = patternOpt,
            processOpt = processOpt, filterOpt = filterOpt,
            initOpt = initOpt, initAggregationsOpt = initAggregationsOpt,
            shouldBypassOpt = shouldBypassOpt,
            processComputeOpt = processComputeOpt,
            nextComputationOpt = Some(nextComp))

      case None =>
         this.copy (primitive = primitive,
            computationLabelOpt = computationLabelOpt,
            patternOpt = patternOpt,
            processOpt = processOpt, filterOpt = filterOpt,
            initOpt = initOpt, initAggregationsOpt = initAggregationsOpt,
            shouldBypassOpt = shouldBypassOpt,
            processComputeOpt = processComputeOpt, nextComputationOpt = None)
   }

   def withComputationAppended(lastComputation: Computation[S])
   : ComputationContainer[S] = nextComputationOpt match {
      case Some(nextComputation) =>
         val container = nextComputation.asInstanceOf[ComputationContainer[S]]
         val _nextComputation = container.
            withComputationAppended(lastComputation)
         this.copy(nextComputationOpt = Option(_nextComputation))
      case None =>
         this.copy(nextComputationOpt = Option(lastComputation))
   }

   def withPrimitive(p: Primitive): ComputationContainer[S] = {
      this.copy (primitive = p)
   }

   def withComputationLabel(label: String): ComputationContainer[S] = {
      this.copy (computationLabelOpt = Option(label))
   }

   def asLastComputation: ComputationContainer[S] = {
      this.copy(nextComputationOpt = None)
   }

   def take(n: Int): ComputationContainer[S] = {
      if (n <= 1) {
         this.asLastComputation
      } else {
         nextComputationOpt match {
            case Some(nextComputation : ComputationContainer[S]) =>
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
      _initAggregations = initAggregationsOpt match {
         case Some(thisInitAggregations) =>
            (config: Configuration, c: Computation[S]) => {
               super.initAggregations(config)
               thisInitAggregations(c)
            }

         case None =>
            (config: Configuration, c: Computation[S]) => {
               super.initAggregations(config)
            }
      }
      _init = initOpt match {
         case Some(thisInit) =>
            (config: Configuration, c: Computation[S]) => {
               super.init(config); thisInit(c)
            }
         case None =>
            (config: Configuration, c: Computation[S]) => {
               super.init(config)
            }
      }
      _patternInit = (config: Configuration, c: Computation[S]) => {
         val pattern = getPattern()
         if (pattern != null) {
            patternOpt.map(_.getConfig().asInstanceOf[Configuration]) match {
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
         .getOrElse ((e: S, c: Computation[S]) => super.filter_FILTERING_PRIMITIVE(e))
      _process = processOpt
         .getOrElse ((e: S, c: Computation[S]) => super.process (e))
      _pattern = patternOpt.getOrElse(null)
      _computationLabel = computationLabelOpt.getOrElse (containerId.toString)
   }
}

object ComputationContainer {
   val nextContainerId = new AtomicInteger(0)
}


