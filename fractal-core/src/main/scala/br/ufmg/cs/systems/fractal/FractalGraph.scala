package br.ufmg.cs.systems.fractal

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.IntConsumer

import br.ufmg.cs.systems.fractal.computation._
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.graph.{BasicMainGraph, MainGraph}
import br.ufmg.cs.systems.fractal.pattern._
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util._
import br.ufmg.cs.systems.fractal.util.collection._

import scala.collection.mutable.Map
import scala.reflect.ClassTag

/**
 * Graph used as starting point of Fractal application workflows
 * @param path graph path (file:// or hdfs://)
 * @param graphClass graph implementation
 * @param local path is local
 * @param fc fractal context
 * @param logLevel logging verbosity
 */
class FractalGraph(
                     path: String,
                     graphClass: String,
                     local: Boolean,
                     fc: FractalContext,
                     logLevel: String) extends Logging {

   private val uuid: UUID = UUID.randomUUID

   private val graphId: Int = FractalGraph.newGraphId()

   private val confs: Map[String,Any] = Map.empty

   private lazy val mainGraph: MainGraph[String,String] = {
      import Configuration._
      val config = new SparkConfiguration
      config.set ("input_graph_path", path)
      config.set ("input_graph_local", local)
      config.set ("input_graph_class", graphClass)
      config.setMainGraphClass (
         config.getClass (CONF_MAINGRAPH_CLASS, CONF_MAINGRAPH_CLASS_DEFAULT).
            asInstanceOf[Class[_ <: MainGraph[_,_]]]
      )
      config.createGraph()
      config.getMainGraph[MainGraph[String,String]]
   }

   def asPattern: Pattern = {
      val computation: Computation[EdgeInducedSubgraph] =
         new EComputationContainer()
      val config = new SparkConfiguration[EdgeInducedSubgraph]
      config.set ("input_graph_path", path)
      config.set ("input_graph_local", local)
      //config.set ("edge_labelled", true)
      config.setSubgraphClass (computation.getSubgraphClass())
      config.setMainGraphId (graphId)
      config.initialize()
      val subgraph = config.createSubgraph[EdgeInducedSubgraph]

      val graph = config.getMainGraph[BasicMainGraph[_,_]]
      graph.forEachEdge(new IntConsumer {
         override def accept(e: Int): Unit = subgraph.addWord(e)
      })

      val pattern = config.createPattern().asInstanceOf[BasicPattern]
      pattern.setSubgraph(subgraph)

      pattern
   }

   def tmpPath: String = s"${fc.tmpPath}/graph-${uuid}"

   def fractalContext: FractalContext = fc

   def this(path: String, arab: FractalContext, logLevel: String) = {
      this (path, Configuration.CONF_MAINGRAPH_CLASS_DEFAULT,
         false, arab, logLevel)
   }

   def this(path: String, arab: FractalContext) = {
      this (path, Configuration.CONF_MAINGRAPH_CLASS_DEFAULT,
         false, arab, "warn")
   }

   def this(path: String, graphClass: String,
            arab: FractalContext, logLevel: String) = {
      this (path, graphClass, false, arab, logLevel)
   }

   def this(path: String, graphClass: String,
            arab: FractalContext) = {
      this (path, graphClass, false, arab, "warn")
   }

   private def resultHandler [S <: Subgraph : ClassTag] (
                                                           config: SparkConfiguration[S])
   : Fractoid[S] = {
      config.set ("log_level", logLevel)
      confs.foreach { case (k,v) =>
         config.set(k, v)
         logInfo(s"Setting (${k},${v}) from graph")
      }
      config.setMainGraphId (graphId)
      new Fractoid [S] (this, config)
   }

   /** api for custom computations **/

   /**
    * Creates an edge-induced Fractoid with a process function
    * @param process function executed on every valid subgraph
    * @return Fractoid with the initial state of edge-induced computation
    */
   def efractoid(
                   process: (EdgeInducedSubgraph,
                      Computation[EdgeInducedSubgraph]) => Unit)
   : Fractoid[EdgeInducedSubgraph] = {
      val config = new SparkConfiguration[EdgeInducedSubgraph]
      config.set ("input_graph_path", path)
      config.set ("input_graph_local", local)
      config.set ("input_graph_class", graphClass)
      config.set ("output_path", s"${tmpPath}/edge-computation-${config.getId}")
      customComputation [EdgeInducedSubgraph] (config)
   }

   /**
    * Edge-induced fractoid without a process function
    * @return Fractoid with the initial state of edge-induced computation
    */
   def efractoid: Fractoid[EdgeInducedSubgraph] =
      efractoid (null)

   /**
    * Creates an edge-induced Fractoid with a process function
    * @param process function executed on every valid subgraph
    * @return Fractoid with the initial state of edge-induced computation
    */
   def efractoidAndExpand(
                            process: (EdgeInducedSubgraph,
                               Computation[EdgeInducedSubgraph]) => Unit)
   : Fractoid[EdgeInducedSubgraph] = {
      val computation: Computation[EdgeInducedSubgraph] =
         new EComputationContainer(processOpt = Option(process),
            primitiveOpt = Option(Primitive.E))
      val config = new SparkConfiguration[EdgeInducedSubgraph].
         withNewComputation (computation)
      config.set ("input_graph_path", path)
      config.set ("input_graph_local", local)
      config.set ("input_graph_class", graphClass)
      config.set ("output_path", s"${tmpPath}/edge-computation-${config.getId}")
      customComputation [EdgeInducedSubgraph] (config)
   }

   /**
    * Edge-induced fractoid without a process function
    * @return Fractoid with the initial state of edge-induced computation
    */
   def efractoidAndExpand: Fractoid[EdgeInducedSubgraph] =
      efractoidAndExpand (null)

   /**
    * Edge-induced fractoid without a process function and with sampling
    * @return Fractoid with the initial state of edge-induced computation
    */
   def sefractoid(fraction: Double): Fractoid[EdgeInducedSubgraph] = {
      val FRACTION_KEY = "sampling_fraction"

      logInfo(s"Sampling fractoid uniformly at random: fraction=${fraction}" +
         s" fractoid=${this}")

      efractoid.
         set("subgraph_enumerator",
            "br.ufmg.cs.systems.fractal.computation.SamplingEnumerator").
         set(FRACTION_KEY, fraction)
   }

   /**
    * Creates a vertex-induced Fractoid with a process function
    * @param process
    * @return Fractoid with the initial state of vertex-induced computation
    */
   def vfractoid(
                   process: (VertexInducedSubgraph,
                      Computation[VertexInducedSubgraph]) => Unit)
   : Fractoid[VertexInducedSubgraph] = {
      val config = new SparkConfiguration[VertexInducedSubgraph]
      config.set ("input_graph_path", path)
      config.set ("input_graph_local", local)
      config.set ("input_graph_class", graphClass)
      config.set ("output_path", s"${tmpPath}/vertex-computation-${config.getId}")
      customComputation [VertexInducedSubgraph] (config)
   }

   /**
    * Vertex-induced fractoid without a process function
    * @return Fractoid with the initial state of vertex-induced computation
    */
   def vfractoid: Fractoid[VertexInducedSubgraph] =
      vfractoid(null)

   /**
    * Creates a vertex-induced Fractoid with a process function
    * @param process
    * @return Fractoid with the initial state of vertex-induced computation
    */
   def vfractoidAndExpand(
                            process: (VertexInducedSubgraph,
                               Computation[VertexInducedSubgraph]) => Unit)
   : Fractoid[VertexInducedSubgraph] = {
      val computation: Computation[VertexInducedSubgraph] =
         new VComputationContainer(processOpt = Option(process),
            primitiveOpt = Option(Primitive.E))
      val config = new SparkConfiguration[VertexInducedSubgraph].
         withNewComputation (computation)
      config.set ("input_graph_path", path)
      config.set ("input_graph_local", local)
      config.set ("input_graph_class", graphClass)
      config.set ("output_path", s"${tmpPath}/vertex-computation-${config.getId}")
      customComputation [VertexInducedSubgraph] (config)
   }

   /**
    * Vertex-induced fractoid without a process function
    * @return Fractoid with the initial state of vertex-induced computation
    */
   def vfractoidAndExpand: Fractoid[VertexInducedSubgraph] =
      vfractoidAndExpand (null)

   /**
    * Vertex-induced fractoid without a process function and with sampling
    * @return Fractoid with the initial state of vertex-induced computation
    */
   def svfractoid(fraction: Double): Fractoid[VertexInducedSubgraph] = {
      val FRACTION_KEY = "sampling_fraction"

      logInfo(s"Sampling fractoid uniformly at random: fraction=${fraction}" +
         s" fractoid=${this}")

      vfractoid.
         set("subgraph_enumerator",
            "br.ufmg.cs.systems.fractal.computation.SamplingEnumerator").
         set(FRACTION_KEY, fraction)
   }

   /**
    * Creates a pattern-induced Fractoid with a process function from a pattern
    * @param process
    * @param pattern pattern to guide enumeration
    * @return Fractoid with the initial state of pattern-induced computation
    */
   def pfractoid(
                   process: (PatternInducedSubgraph,
                      Computation[PatternInducedSubgraph]) => Unit,
                   pattern: Pattern): Fractoid[PatternInducedSubgraph] = {
      val patternWithPlan = if (pattern.explorationPlan() == null) {
         PatternExplorationPlan.apply(pattern).get(0)
      } else {
         pattern
      }
      logInfo(s"PatternWithPlan ${patternWithPlan} plan=${patternWithPlan.explorationPlan()}" +
         s" lowerBound=${patternWithPlan.vsymmetryBreakerLowerBound()} upperBound=${patternWithPlan.vsymmetryBreakerUpperBound()}")
      val config = new SparkConfiguration[PatternInducedSubgraph]
      config.set ("pattern", patternWithPlan)
      config.set ("input_graph_path", path)
      config.set ("input_graph_local", local)
      config.set ("input_graph_class", graphClass)
      config.set ("output_path",
         s"${tmpPath}/pattern-computation-${config.getId}")
      customComputation [PatternInducedSubgraph] (config)
   }

   /**
    * Pattern-induced fractoid from a pattern template
    * @param pattern
    * @return Fractoid with the initial state of pattern-induced computation
    */
   def pfractoid(
                   pattern: Pattern): Fractoid[PatternInducedSubgraph] =
      pfractoid (null, pattern)

   /**
    * Creates a pattern-induced Fractoid with a process function from a pattern
    * @param process
    * @param pattern pattern to guide enumeration
    * @return Fractoid with the initial state of pattern-induced computation
    */
   def pfractoidAndExpand(
                            process: (PatternInducedSubgraph,
                               Computation[PatternInducedSubgraph]) => Unit,
                            pattern: Pattern): Fractoid[PatternInducedSubgraph] = {
      val patternWithPlan = if (pattern.explorationPlan() == null) {
         PatternExplorationPlan.apply(pattern).get(0)
      } else {
         pattern
      }
      logInfo(s"PatternWithPlan ${patternWithPlan} plan=${patternWithPlan.explorationPlan()}" +
         s" lowerBound=${patternWithPlan.vsymmetryBreakerLowerBound()} upperBound=${patternWithPlan.vsymmetryBreakerUpperBound()}")
      val computation: Computation[PatternInducedSubgraph] =
         new VEComputationContainer(processOpt = Option(process),
            patternOpt = Option(patternWithPlan), primitiveOpt = Option(Primitive.E))
      val config = new SparkConfiguration[PatternInducedSubgraph].
         withNewComputation (computation)
      config.set ("input_graph_path", path)
      config.set ("input_graph_local", local)
      config.set ("input_graph_class", graphClass)
      config.set ("output_path",
         s"${tmpPath}/pattern-computation-${config.getId}")
      customComputation [PatternInducedSubgraph] (config)
   }

   /**
    * Pattern-induced fractoid from a pattern template
    * @param pattern
    * @return Fractoid with the initial state of pattern-induced computation
    */
   def pfractoidAndExpand(
                            pattern: Pattern): Fractoid[PatternInducedSubgraph] =
      pfractoidAndExpand (null, pattern)

   /**
    * Pattern-induced fractoid without a process function and with sampling
    * @return Fractoid with the initial state of pattern-induced computation
    */
   def spfractoid(pattern: Pattern,
                  fraction: Double): Fractoid[PatternInducedSubgraph] = {
      val FRACTION_KEY = "sampling_fraction"

      logInfo(s"Sampling fractoid uniformly at random: fraction=${fraction}" +
         s" fractoid=${this}")

      pfractoid(pattern).
         set("subgraph_enumerator",
            "br.ufmg.cs.systems.fractal.computation.SamplingEnumerator").
         set(FRACTION_KEY, fraction)
   }

   def customComputation[S <: Subgraph : ClassTag]
   (config: SparkConfiguration[S]): Fractoid[S] = {
      resultHandler [S](config)
   }

   def set(key: String, value: Any): Unit = {
      confs.update (key, value)
   }

   override def toString(): String = s"FractalGraph(${path})"
}

object FractalGraph {
   val nextGraphId: AtomicInteger = new AtomicInteger(0)
   def newGraphId(): Int = nextGraphId.getAndIncrement()

   implicit def fgraphWithBuiltInAlgorithms(fgraph: FractalGraph): BuiltInAlgorithms = {
      new BuiltInAlgorithms(fgraph)
   }
}
