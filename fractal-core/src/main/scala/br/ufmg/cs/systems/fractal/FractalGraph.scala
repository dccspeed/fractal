package br.ufmg.cs.systems.fractal

import java.util.concurrent.atomic.AtomicInteger
import java.util.function.IntConsumer

import br.ufmg.cs.systems.fractal.computation._
import br.ufmg.cs.systems.fractal.conf.SparkConfiguration
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplications
import br.ufmg.cs.systems.fractal.graph.EdgeFilteringPredicate
import br.ufmg.cs.systems.fractal.pattern._
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util._
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList

//import scala.collection.mutable.Map
import scala.reflect.ClassTag

/**
 * Graph used as starting point of Fractal application workflows
 * @param path graph path (file:// or hdfs://)
 * @param graphClass graph implementation
 * @param fc fractal context
 * @param logLevel logging verbosity
 */
case class FractalGraph
(path: String,
 graphClass: String,
 fc: FractalContext,
 confs: Map[String, Any],
 logLevel: String,
 edgePredicate: EdgeFilteringPredicate) extends Logging {

   private val config: SparkConfiguration = {
      val _config = new SparkConfiguration
      confs.foreach { case (k,v) =>
         _config.set(k, v)
         logInfo(s"Setting (${k},${v}) from graph")
      }

      _config.set ("input_graph_path", path)
      _config.set ("input_graph_class", graphClass)
      _config.set ("log_level", logLevel)

      val numPartitions = _config.getInteger("num_partitions",
         fractalContext.sparkContext.defaultParallelism).intValue()

      _config.set("num_partitions", numPartitions)

      if (edgePredicate != null) {
         _config.set("edge_filtering_predicate", edgePredicate)
      }

      _config
   }

   private val vfractoidRoot: Fractoid[VertexInducedSubgraph] = {
      newFractoid[VertexInducedSubgraph]
   }

   private val vfractoidRootNoEdgeUpdate: Fractoid[VertexInducedSubgraph] = {
      newFractoid[VertexInducedSubgraphNoEdgeUpdate]
         .asInstanceOf[Fractoid[VertexInducedSubgraph]]
   }

   private val efractoidRoot: Fractoid[EdgeInducedSubgraph] = {
      newFractoid[EdgeInducedSubgraph]
   }

   private val pfractoidRoot: Fractoid[PatternInducedSubgraph] = {
      newFractoid[PatternInducedSubgraph]
   }

   def fractalContext: FractalContext = fc

   def this(path: String, fc: FractalContext, graphClass: String) = {
      this (path, graphClass, fc, Map.empty, "warn", null)
   }

   def this(path: String, graphClass: String,
            fc: FractalContext, logLevel: String) = {
      this (path, graphClass, fc, Map.empty, logLevel, null)
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
    * Creates a vertex-induced Fractoid
    * @return Fractoid with the initial state of vertex-induced computation
    */
   def vfractoid: Fractoid[VertexInducedSubgraph] = {
      vfractoidRoot
   }
   /**
    * Creates a vertex-induced Fractoid without edge update
    * @return Fractoid with the initial state of vertex-induced computation
    */
   def vfractoidNoEdgeUpdate: Fractoid[VertexInducedSubgraph] = {
      vfractoidRootNoEdgeUpdate
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

   def set(key: String, value: Any): FractalGraph = {
      this.copy(confs = confs.updated(key, value))
   }

   def filterEdges
   (edgeFilterFunc: (Int,IntArrayList,Int,IntArrayList,Int,IntArrayList) => Boolean)
   : FractalGraph
   = {
      val currEdgePredicate = edgePredicate

      val newEdgePredicate = if (currEdgePredicate == null) {
         new EdgeFilteringPredicate {
            override def test(u: Int, uLabels: IntArrayList,
                              v: Int, vLabels: IntArrayList,
                              e: Int, eLabels: IntArrayList): Boolean = {
               edgeFilterFunc(u, uLabels, v, vLabels, e, eLabels)
            }
         }
      } else {
         new EdgeFilteringPredicate {
            override def test(u: Int, uLabels: IntArrayList,
                              v: Int, vLabels: IntArrayList,
                              e: Int, eLabels: IntArrayList): Boolean = {
               currEdgePredicate.test(u, uLabels, v, vLabels, e, eLabels) &&
                  edgeFilterFunc(u, uLabels, v, vLabels, e, eLabels)
            }
         }
      }

      this.copy(edgePredicate = newEdgePredicate)
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

