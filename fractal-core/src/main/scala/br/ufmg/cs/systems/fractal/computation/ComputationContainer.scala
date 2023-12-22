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
sealed trait ComputationContainer [S <: Subgraph] extends Computation[S]
   with Logging {

   val containerId: Int = ComputationContainer.nextContainerId
      .getAndIncrement

   val patternOpt: Option[Pattern]

   val processOpt: Option[(S,Computation[S]) => Unit]

   val filterOpt: Option[(S,Computation[S]) => Boolean]

   val initOpt: Option[(Computation[S]) => Unit]

   val initAggregationsOpt: Option[(Computation[S]) => Unit]

   val processComputeOpt: Option[(SubgraphEnumerator[S],Computation[S]) => Long]

   val subgraphClassOpt: Option[Class[_ <: Subgraph]]

   val subgraphEnumeratorClassOpt: Option[Class[_ <: SubgraphEnumerator[S]]]

   val nextComputationOpt: Option[Computation[S]]

   @transient lazy val lastComputationContainer: ComputationContainer[S] = {
      nextComputationOpt match {
         case Some(nextComputation: ComputationContainer[S]) =>
            nextComputation.lastComputationContainer
         case _ =>
            this
      }
   }

   override def lastComputation(): Computation[S] = lastComputationContainer

   def initContainerFunctions(): Unit

   def withComputationAppended(lastComputation: Computation[S])
   : ComputationContainer[S]

   def withPrimitive(p: Primitive): ComputationContainer[S]

   def asLastComputation: ComputationContainer[S]

   def take(n: Int): ComputationContainer[S]

   def shallowCopy(
                     primitive: Primitive = primitive,
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
                     processComputeOpt: Option[(SubgraphEnumerator[S],Computation[S]) => Long] =
                     processComputeOpt,
                     subgraphClassOpt: Option[Class[_ <: Subgraph]] =
                     subgraphClassOpt,
                     subgraphEnumeratorClassOpt: Option[Class[_ <: SubgraphEnumerator[S]]] =
                     subgraphEnumeratorClassOpt,
                     nextComputationOpt: Option[Computation[S]] =
                     nextComputationOpt
                  ): ComputationContainer[S]

   def withNewFunctions(
                          primitive: Primitive = primitive,
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
                          processComputeOpt: Option[(SubgraphEnumerator[S],Computation[S]) => Long] =
                          processComputeOpt,
                          subgraphClassOpt: Option[Class[_ <: Subgraph]] =
                          subgraphClassOpt,
                          subgraphEnumeratorClassOpt: Option[Class[_ <: SubgraphEnumerator[S]]] =
                          subgraphEnumeratorClassOpt
   ): ComputationContainer[S]

   def shallowCopy(): ComputationContainer[S]

   def clear(): ComputationContainer[S] = withNewFunctions (
      primitive = Primitive.None,
      // When creating an empty computation for pattern-induced fractoids, new
      // containers lose this pattern.
      //patternOpt = None,
      processOpt = Some((e,c) => {}),
      filterOpt = Some((e,c) => true)
   )

   @transient lazy val computationRepr: Array[String] = {
      Array(
         filterOpt.map(_ => "f").getOrElse("_"),
         processComputeOpt.map(_.toString()).getOrElse("_"),
         processOpt.map(_ => "p").getOrElse("_"))
   }

   override def primitives(): Array[Primitive] = nextComputationOpt match {
      case Some(c: ComputationContainer[S]) =>
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
         .asInstanceOf[ComputationContainer[S]]
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
            .asInstanceOf[ComputationContainer[S]]
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

case class EComputationContainer [S <: EdgeInducedSubgraph]
(primitive: Primitive = Primitive.None,
 patternOpt: Option[Pattern] = None,
 processOpt: Option[(S,Computation[S]) => Unit] = None,
 filterOpt: Option[(S,Computation[S]) => Boolean] = None,
 initOpt: Option[(Computation[S]) => Unit] = None,
 initAggregationsOpt: Option[(Computation[S]) => Unit] = None,
 processComputeOpt: Option[(SubgraphEnumerator[S],Computation[S]) => Long] = None,
 subgraphClassOpt: Option[Class[_ <: Subgraph]] = None,
 subgraphEnumeratorClassOpt: Option[Class[_ <: SubgraphEnumerator[S]]] = None,
 nextComputationOpt: Option[Computation[S]] = None)
   extends EdgeInducedComputation[S] with ComputationContainer[S] {

   initContainerFunctions()

   private var _computationLabel: String = _
   private var _pattern: Pattern = _
   private var _process: (S,Computation[S]) => Unit = _
   private var _filter: (S,Computation[S]) => Boolean = _
   private var _init: (Configuration, Computation[S]) => Unit = _
   private var _initAggregations: (Configuration, Computation[S]) => Unit = _
   private var _processCompute
   : (SubgraphEnumerator[S],Computation[S]) => Long = _
   private var _subgraphClass: Class[_ <: Subgraph] = _
   private var _subgraphEnumeratorClass: Class[_ <: SubgraphEnumerator[S]] = _
   private var _nextComputation: Computation[S] = _

   override def process(e: S): Unit = _process (e, this)
   override def filter_FILTERING_PRIMITIVE(e: S): Boolean = _filter (e, this)
   override def init(config: Configuration): Unit = _init (config, this)
   override def initAggregations(config: Configuration): Unit =
      _initAggregations (config, this)
   override def processCompute(iter: SubgraphEnumerator[S]) =
      _processCompute (iter, this)
   override def nextComputation(): Computation[S] = _nextComputation
   override def getPattern(): Pattern = _pattern
   override def getSubgraphClass: Class[_ <: Subgraph] = _subgraphClass
   override def getSubgraphEnumeratorClass: Class[_ <: SubgraphEnumerator[S]] =
      _subgraphEnumeratorClass

   def shallowCopy(): ComputationContainer[S] = nextComputationOpt match {
      case Some(nextComputation : ComputationContainer[S]) =>
         this.copy(
            primitive = primitive,
            nextComputationOpt = Option(nextComputation.shallowCopy()))
      case None =>
         this.copy(primitive = primitive)
      case _ =>
         throw new RuntimeException(s"Next computation should be a container")
   }

   def shallowCopy(
                     primitive: Primitive = primitive,
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
                     processComputeOpt: Option[(SubgraphEnumerator[S],Computation[S]) => Long] =
                     processComputeOpt,
                     subgraphClassOpt: Option[Class[_ <: Subgraph]] =
                     subgraphClassOpt,
                     subgraphEnumeratorClassOpt: Option[Class[_ <: SubgraphEnumerator[S]]] =
                     subgraphEnumeratorClassOpt,
                     nextComputationOpt: Option[Computation[S]] =
                     nextComputationOpt)
   : ComputationContainer[S] = {
      this.copy(
         primitive = primitive,
         patternOpt = patternOpt,
         processOpt = processOpt,
         filterOpt = filterOpt,
         initOpt = initOpt,
         initAggregationsOpt = initAggregationsOpt,
         processComputeOpt = processComputeOpt,
         subgraphClassOpt = subgraphClassOpt,
         subgraphEnumeratorClassOpt = subgraphEnumeratorClassOpt,
         nextComputationOpt = nextComputationOpt
      )
   }

   def withNewFunctions(
                          primitive: Primitive =
                          lastComputationContainer.primitive,
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
                          processComputeOpt: Option[(SubgraphEnumerator[S],Computation[S]) => Long] =
                          lastComputationContainer.processComputeOpt,
                          subgraphClassOpt: Option[Class[_ <: Subgraph]] =
                          lastComputationContainer.subgraphClassOpt,
                          subgraphEnumeratorClassOpt: Option[Class[_ <: SubgraphEnumerator[S]]] =
                          lastComputationContainer.subgraphEnumeratorClassOpt
                       ): ComputationContainer[S] = {

      val comps = new Stack[EComputationContainer[S]]()
      var currOpt: Option[EComputationContainer[S]] = Option(this)
      while (currOpt.isDefined) {
         comps.push(currOpt.get)
         currOpt = currOpt.get.nextComputationOpt.
            asInstanceOf[Option[EComputationContainer[S]]]
      }

      var lastComp = comps.pop()
      lastComp = lastComp.copy(primitive, patternOpt,
         processOpt, filterOpt,
         initOpt, initAggregationsOpt,
         processComputeOpt,
         subgraphClassOpt, subgraphEnumeratorClassOpt)

      while (!comps.isEmpty) {
         lastComp = comps.pop().copy(nextComputationOpt = Some(lastComp))
      }

      lastComp
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
      _filter =  filterOpt
         .getOrElse ((e: S, c: Computation[S]) => super.filter_FILTERING_PRIMITIVE(e))
      _process = processOpt
         .getOrElse ((e: S, c: Computation[S]) => super.process (e))
      _pattern = patternOpt.getOrElse(null)
      _subgraphClass = subgraphClassOpt.getOrElse(classOf[EdgeInducedSubgraph])
      _subgraphEnumeratorClass = subgraphEnumeratorClassOpt
         .getOrElse(classOf[SubgraphEnumerator[S]])
   }
}

case class VComputationContainer [S <: VertexInducedSubgraph]
(primitive: Primitive = Primitive.None,
 patternOpt: Option[Pattern] = None,
 processOpt: Option[(S,Computation[S]) => Unit] = None,
 filterOpt: Option[(S,Computation[S]) => Boolean] = None,
 initOpt: Option[(Computation[S]) => Unit] = None,
 initAggregationsOpt: Option[(Computation[S]) => Unit] = None,
 processComputeOpt: Option[(SubgraphEnumerator[S],Computation[S]) => Long] = None,
 subgraphClassOpt: Option[Class[_ <: Subgraph]] = None,
 subgraphEnumeratorClassOpt: Option[Class[_ <: SubgraphEnumerator[S]]] = None,
 nextComputationOpt: Option[Computation[S]] = None
) extends VertexInducedComputation[S] with ComputationContainer[S] {

   initContainerFunctions()

   private var _computationLabel: String = _
   private var _pattern: Pattern = _
   private var _process: (S,Computation[S]) => Unit = _
   private var _filter: (S,Computation[S]) => Boolean = _
   private var _init: (Configuration, Computation[S]) => Unit = _
   private var _initAggregations: (Configuration, Computation[S]) => Unit = _
   private var _processCompute: (SubgraphEnumerator[S],Computation[S]) => Long = _
   private var _nextComputation: Computation[S] = _
   private var _subgraphClass: Class[_ <: Subgraph] = _
   private var _subgraphEnumeratorClass: Class[_ <: SubgraphEnumerator[S]] = _

   override def process(e: S): Unit = _process (e, this)
   override def filter_FILTERING_PRIMITIVE(e: S): Boolean = _filter (e, this)
   override def init(config: Configuration): Unit = _init (config, this)
   override def initAggregations(config: Configuration): Unit =
      _initAggregations (config, this)
   override def processCompute(iter: SubgraphEnumerator[S]) =
      _processCompute (iter, this)
   override def nextComputation(): Computation[S] = _nextComputation
   override def getPattern(): Pattern = _pattern
   override def getSubgraphClass: Class[_ <: Subgraph] = _subgraphClass
   override def getSubgraphEnumeratorClass: Class[_ <: SubgraphEnumerator[S]] =
      _subgraphEnumeratorClass

   def shallowCopy(): ComputationContainer[S] = nextComputationOpt match {
      case Some(nextComputation : ComputationContainer[S]) =>
         this.copy(
            primitive = Primitive.None,
            nextComputationOpt = Option(nextComputation.shallowCopy()))
      case None =>
         this.copy(primitive = Primitive.None)
      case _ =>
         throw new RuntimeException(s"Next computation should be a container")
   }

   def shallowCopy(
                     primitive: Primitive = primitive,
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
                     processComputeOpt: Option[(SubgraphEnumerator[S],Computation[S]) => Long] =
                     processComputeOpt,
                     subgraphClassOpt: Option[Class[_ <: Subgraph]] =
                     subgraphClassOpt,
                     subgraphEnumeratorClassOpt: Option[Class[_ <: SubgraphEnumerator[S]]] =
                     subgraphEnumeratorClassOpt,
                     nextComputationOpt: Option[Computation[S]] =
                     nextComputationOpt
                  )
   : ComputationContainer[S] = {
      this.copy(
         primitive = primitive,
         patternOpt = patternOpt,
         processOpt = processOpt,
         filterOpt = filterOpt,
         initOpt = initOpt,
         initAggregationsOpt = initAggregationsOpt,
         processComputeOpt = processComputeOpt,
         subgraphClassOpt = subgraphClassOpt,
         subgraphEnumeratorClassOpt = subgraphEnumeratorClassOpt,
         nextComputationOpt = nextComputationOpt
      )
   }

   def withNewFunctions(
                          primitive: Primitive =
                          lastComputationContainer.primitive,
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
                          processComputeOpt: Option[(SubgraphEnumerator[S],Computation[S]) => Long] =
                          lastComputationContainer.processComputeOpt,
                          subgraphClassOpt: Option[Class[_ <: Subgraph]] =
                          lastComputationContainer.subgraphClassOpt,
                          subgraphEnumeratorClassOpt: Option[Class[_ <: SubgraphEnumerator[S]]] =
                          lastComputationContainer.subgraphEnumeratorClassOpt
                       ): ComputationContainer[S] = {

      val comps = new Stack[VComputationContainer[S]]()
      var currOpt: Option[VComputationContainer[S]] = Option(this)
      while (currOpt.isDefined) {
         comps.push(currOpt.get)
         currOpt = currOpt.get.nextComputationOpt.
            asInstanceOf[Option[VComputationContainer[S]]]
      }

      var lastComp = comps.pop()
      lastComp = lastComp.copy(primitive, patternOpt,
         processOpt, filterOpt,
         initOpt, initAggregationsOpt,
         processComputeOpt,
         subgraphClassOpt, subgraphEnumeratorClassOpt)

      while (!comps.isEmpty) {
         lastComp = comps.pop().copy(nextComputationOpt = Some(lastComp))
      }

      lastComp
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
      _filter =  filterOpt
         .getOrElse ((e: S, c: Computation[S]) => super.filter_FILTERING_PRIMITIVE(e))
      _process = processOpt
         .getOrElse ((e: S, c: Computation[S]) => super.process (e))
      _pattern = patternOpt.getOrElse(null)
      _subgraphClass = subgraphClassOpt.getOrElse(classOf[VertexInducedSubgraph])
      _subgraphEnumeratorClass = subgraphEnumeratorClassOpt.getOrElse(classOf[SubgraphEnumerator[S]])
   }
}

case class PComputationContainer [S <: PatternInducedSubgraph]
(
   primitive: Primitive = Primitive.None,
   patternOpt: Option[Pattern] = None,
   processOpt: Option[(S,Computation[S]) => Unit] = None,
   filterOpt: Option[(S,Computation[S]) => Boolean] = None,
   initOpt: Option[(Computation[S]) => Unit] = None,
   initAggregationsOpt: Option[(Computation[S]) => Unit] = None,
   processComputeOpt: Option[(SubgraphEnumerator[S],Computation[S]) => Long] = None,
   subgraphClassOpt: Option[Class[_ <: Subgraph]] = None,
   subgraphEnumeratorClassOpt: Option[Class[_ <: SubgraphEnumerator[S]]] = None,
   nextComputationOpt: Option[Computation[S]] = None
) extends PatternInducedComputation[S] with ComputationContainer[S] {

   initContainerFunctions()

   private var _computationLabel: String = _
   private var _pattern: Pattern = _
   private var _process: (S,Computation[S]) => Unit = _
   private var _filter: (S,Computation[S]) => Boolean = _
   private var _patternInit: (Configuration, Computation[S]) => Unit = _
   private var _init: (Configuration, Computation[S]) => Unit = _
   private var _initAggregations: (Configuration, Computation[S]) => Unit = _
   private var _processCompute
   : (SubgraphEnumerator[S],Computation[S]) => Long = _
   private var _nextComputation: Computation[S] = _
   private var _subgraphClass: Class[_ <: Subgraph] = _
   private var _subgraphEnumeratorClass: Class[_ <: SubgraphEnumerator[S]] = _

   override def process(e: S): Unit = _process (e, this)
   override def filter_FILTERING_PRIMITIVE(e: S): Boolean = _filter (e, this)
   override def init(config: Configuration): Unit = {
      _patternInit(config, this)
      _init (config, this)
   }
   override def initAggregations(config: Configuration): Unit =
      _initAggregations (config, this)
   override def processCompute(iter: SubgraphEnumerator[S]) =
      _processCompute (iter, this)
   override def nextComputation(): Computation[S] = _nextComputation
   override def getPattern(): Pattern = _pattern
   override def setPattern(pattern: Pattern): Unit = {
      _pattern = pattern
   }
   override def getSubgraphClass: Class[_ <: Subgraph] = _subgraphClass
   override def getSubgraphEnumeratorClass: Class[_ <: SubgraphEnumerator[S]] =
      _subgraphEnumeratorClass

   def shallowCopy(): ComputationContainer[S] = nextComputationOpt match {
      case Some(nextComputation : ComputationContainer[S]) =>
         this.copy(
            primitive = primitive,
            nextComputationOpt = Option(nextComputation.shallowCopy()))
      case None =>
         this.copy(primitive = primitive)
      case _ =>
         throw new RuntimeException(s"Next computation should be a container")
   }

   def shallowCopy(
                     primitive: Primitive = primitive,
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
                     processComputeOpt: Option[(SubgraphEnumerator[S],Computation[S]) => Long] =
                     processComputeOpt,
                     subgraphClassOpt: Option[Class[_ <: Subgraph]] =
                     subgraphClassOpt,
                     subgraphEnumeratorClassOpt: Option[Class[_ <: SubgraphEnumerator[S]]] =
                     subgraphEnumeratorClassOpt,
                     nextComputationOpt: Option[Computation[S]] =
                     nextComputationOpt)
   : ComputationContainer[S] = {
      this.copy(
         primitive = primitive,
         patternOpt = patternOpt,
         processOpt = processOpt,
         filterOpt = filterOpt,
         initOpt = initOpt,
         initAggregationsOpt = initAggregationsOpt,
         processComputeOpt = processComputeOpt,
         subgraphClassOpt = subgraphClassOpt,
         subgraphEnumeratorClassOpt = subgraphEnumeratorClassOpt,
         nextComputationOpt = nextComputationOpt
      )
   }

   def withNewFunctions(
                          primitive: Primitive =
                          lastComputationContainer.primitive,
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
                          processComputeOpt: Option[(SubgraphEnumerator[S],Computation[S]) => Long] =
                          lastComputationContainer.processComputeOpt,
                          subgraphClassOpt: Option[Class[_ <: Subgraph]] =
                          lastComputationContainer.subgraphClassOpt,
                          subgraphEnumeratorClassOpt: Option[Class[_ <: SubgraphEnumerator[S]]] =
                          lastComputationContainer.subgraphEnumeratorClassOpt
                       ): ComputationContainer[S] = {

      val comps = new Stack[PComputationContainer[S]]()
      var currOpt: Option[PComputationContainer[S]] = Option(this)
      while (currOpt.isDefined) {
         comps.push(currOpt.get)
         currOpt = currOpt.get.nextComputationOpt.
            asInstanceOf[Option[PComputationContainer[S]]]
      }

      var lastComp = comps.pop()
      lastComp = lastComp.copy(primitive, patternOpt,
         processOpt, filterOpt,
         initOpt, initAggregationsOpt,
         processComputeOpt,
         subgraphClassOpt, subgraphEnumeratorClassOpt)

      while (!comps.isEmpty) {
         lastComp = comps.pop().copy(nextComputationOpt = Some(lastComp))
      }

      lastComp
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
      _subgraphClass = subgraphClassOpt.getOrElse(classOf[PatternInducedSubgraph])
      _subgraphEnumeratorClass = subgraphEnumeratorClassOpt
         .getOrElse(classOf[SubgraphEnumerator[S]])
   }
}

object ComputationContainer {
   val nextContainerId = new AtomicInteger(0)
}


