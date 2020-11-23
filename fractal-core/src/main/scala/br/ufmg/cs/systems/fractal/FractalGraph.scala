package br.ufmg.cs.systems.fractal

import java.net.InetAddress
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.IntConsumer

import br.ufmg.cs.systems.fractal.computation._
import br.ufmg.cs.systems.fractal.conf.Configuration.CONF_MASTER_HOSTNAME
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplications
import br.ufmg.cs.systems.fractal.graph.BasicMainGraph
import br.ufmg.cs.systems.fractal.pattern._
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util._

//import scala.collection.mutable.Map
import scala.reflect.ClassTag

/**
 * Graph used as starting point of Fractal application workflows
 * @param path graph path (file:// or hdfs://)
 * @param graphClass graph implementation
 * @param local path is local
 * @param fc fractal context
 * @param logLevel logging verbosity
 */
case class FractalGraph
(path: String,
 graphClass: String,
 local: Boolean,
 fc: FractalContext,
 confs: Map[String, Any],
 logLevel: String) extends Logging {

   private val config: SparkConfiguration = {
      val _config = new SparkConfiguration
      confs.foreach { case (k,v) =>
         _config.set(k, v)
         logInfo(s"Setting (${k},${v}) from graph")
      }

      _config.set ("input_graph_path", path)
      _config.set ("input_graph_local", local)
      _config.set ("input_graph_class", graphClass)
      _config.set ("log_level", logLevel)

      val numPartitions = _config.getInteger("num_partitions",
         fractalContext.sparkContext.defaultParallelism).intValue()

      _config.set("num_partitions", numPartitions)

      _config
   }

   private val vfractoidRoot: Fractoid[VertexInducedSubgraph] =
      newFractoid[VertexInducedSubgraph]

   private val efractoidRoot: Fractoid[EdgeInducedSubgraph] =
      newFractoid[EdgeInducedSubgraph]

   private val pfractoidRoot: Fractoid[PatternInducedSubgraph] =
      newFractoid[PatternInducedSubgraph]

   def asPattern: Pattern = {
      val computation: Computation[EdgeInducedSubgraph] =
          new EComputationContainer()
      val config = new SparkConfiguration
      config.set ("input_graph_path", path)
      config.set ("input_graph_local", local)
      config.set ("input_graph_class", "br.ufmg.cs.systems.fractal.graph.BasicMainGraph")
      config.setSubgraphClass (computation.getSubgraphClass())
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

   def fractalContext: FractalContext = fc

   def this(path: String, fc: FractalContext) = {
      this (path, Configuration.CONF_MAINGRAPH_CLASS_DEFAULT,
         false, fc, Map.empty, "warn")
   }

   def this(path: String, fc: FractalContext, graphClass: String) = {
      this (path, graphClass, false, fc, Map.empty, "warn")
   }

   def this(path: String, graphClass: String,
            fc: FractalContext, logLevel: String) = {
      this (path, graphClass, false, fc, Map.empty, logLevel)
   }

   private def newFractoid[S <: Subgraph : ClassTag]: Fractoid[S] = {
      new Fractoid[S](this, config)
   }

   /** api for custom computations **/

   /**
    * Creates an edge-induced Fractoid with a process function
    * @return Fractoid with the initial state of edge-induced computation
    */
   def efractoid: Fractoid[EdgeInducedSubgraph] = {
      efractoidRoot
   }

   /**
    * Edge-induced fractoid without a process function and with sampling
    * @return Fractoid with the initial state of edge-induced computation
    */
   def sefractoid(fraction: Double): Fractoid[EdgeInducedSubgraph] = {
      val FRACTION_KEY = "sampling_fraction"
      val enumeratorKey = "subgraph_enumerator"
      val enumeratorClass =
         "br.ufmg.cs.systems.fractal.computation.SamplingEnumerator"

      logInfo(s"Sampling fractoid uniformly at random: fraction=${fraction}" +
         s" fractoid=${this}")

      set(enumeratorKey, enumeratorClass)
         .set(FRACTION_KEY, fraction)
         .efractoid
   }

   /**
    * Creates a vertex-induced Fractoid with a process function
    * @return Fractoid with the initial state of vertex-induced computation
    */
   def vfractoid: Fractoid[VertexInducedSubgraph] = {
      vfractoidRoot
   }

   /**
    * Vertex-induced fractoid without a process function and with sampling
    * @return Fractoid with the initial state of vertex-induced computation
    */
   def svfractoid(fraction: Double): Fractoid[VertexInducedSubgraph] = {
      val FRACTION_KEY = "sampling_fraction"
      val enumeratorKey = "subgraph_enumerator"
      val enumeratorClass =
         "br.ufmg.cs.systems.fractal.computation.SamplingEnumerator"

      logInfo(s"Sampling fractoid uniformly at random: fraction=${fraction}" +
         s" fractoid=${this}")

      set(enumeratorKey, enumeratorClass)
         .set(FRACTION_KEY, fraction)
         .vfractoid
   }

   /**
    * Creates a pattern-induced Fractoid with a process function from a pattern
    * @param pattern pattern to guide enumeration
    * @return Fractoid with the initial state of pattern-induced computation
    */
   def pfractoid(pattern: Pattern): Fractoid[PatternInducedSubgraph] = {
      val patternWithPlan = if (pattern.explorationPlan() == null) {
         PatternExplorationPlan.apply(pattern).get(0)
      } else {
         pattern
      }
      logInfo(s"PatternWithPlan ${patternWithPlan} plan=${patternWithPlan.explorationPlan()}" +
         s" lowerBound=${patternWithPlan.vsymmetryBreakerLowerBound()} upperBound=${patternWithPlan.vsymmetryBreakerUpperBound()}")

      pfractoidRoot.copy(
         pattern = patternWithPlan
      )
   }

   /**
    * Pattern-induced fractoid without a process function and with sampling
    * @return Fractoid with the initial state of pattern-induced computation
    */
   def spfractoid(pattern: Pattern,
                  fraction: Double): Fractoid[PatternInducedSubgraph] = {
      val FRACTION_KEY = "sampling_fraction"
      val enumeratorKey = "subgraph_enumerator"
      val enumeratorClass =
         "br.ufmg.cs.systems.fractal.computation.SamplingEnumerator"

      logInfo(s"Sampling fractoid uniformly at random: fraction=${fraction}" +
         s" fractoid=${this}")

      set(enumeratorKey, enumeratorClass)
         .set(FRACTION_KEY, fraction)
         .pfractoid(pattern)
   }

   def set(key: String, value: Any): FractalGraph = {
      //confs.update (key, value)
      this.copy(confs = confs.updated(key, value))
   }

   override def toString(): String = s"FractalGraph(${path})"
}

object FractalGraph {
   val nextGraphId: AtomicInteger = new AtomicInteger(0)
   def newGraphId(): Int = nextGraphId.getAndIncrement()

   implicit def fgraphWithBuiltInAlgorithms(fgraph: FractalGraph): BuiltInApplications = {
      new BuiltInApplications(fgraph)
   }
}
