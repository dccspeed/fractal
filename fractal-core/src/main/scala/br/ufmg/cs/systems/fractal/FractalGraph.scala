package br.ufmg.cs.systems.fractal

import java.util.concurrent.atomic.AtomicInteger

import br.ufmg.cs.systems.fractal.conf.SparkConfiguration
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplications
import br.ufmg.cs.systems.fractal.graph.{EdgeFilteringPredicate, MainGraph, UnlabeledMainGraph, VertexFilteringPredicate}
import br.ufmg.cs.systems.fractal.pattern._
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util._
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import com.koloboke.collect.map.IntIntMap
import com.koloboke.collect.map.hash.{HashIntIntMap, HashIntIntMaps}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.roaringbitmap.RoaringBitmap

//import scala.collection.mutable.Map
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

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
 edgePredicate: EdgeFilteringPredicate,
 vertexPredicate: VertexFilteringPredicate) extends Logging {

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

      if (vertexPredicate != null) {
         _config.set("vertex_filtering_predicate", vertexPredicate)
      }

      _config
   }

   def numPartitions: Int = config.numPartitions

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
      this (path, graphClass, fc, Map.empty, "warn", null, null)
   }

   def this(path: String, graphClass: String,
            fc: FractalContext, logLevel: String) = {
      this (path, graphClass, fc, Map.empty, logLevel, null, null)
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
      //val patternWithPlan = if (pattern.explorationPlan() == null) {
      //   PatternExplorationPlan.apply(pattern).get(0)
      //} else {
      //   pattern
      //}
      val patternWithPlan = if (pattern.explorationPlan() == null) {
         if (graphClass == classOf[UnlabeledMainGraph].getName ||
            !pattern.vertexLabeled()) {
            PatternExplorationPlanOrderingHeuristic.apply(
               pattern, HashIntIntMaps.newUpdatableMap()).getLast
         } else {
            PatternExplorationPlanOrderingHeuristic.apply(
               pattern, vertexLabelFrequencies.value.get()).getLast
         }
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

   def filterVerticesGivenGraph
   (vertexGraphFilterFunc: (Int,MainGraph) => Boolean): FractalGraph = {

      val start = System.nanoTime()

      // determine set of valid vertices
      val keyFunc: VertexInducedSubgraph => Long = s => 0L
      val valueFunc: VertexInducedSubgraph => RoaringBitmap =
         s => RoaringBitmap.bitmapOf(s.getVertices.getLast)
      val reduceFunc: (RoaringBitmap, RoaringBitmap) => Unit =
         (b1,b2) => b1.or(b2)

      val validVertices = vfractoid.expand(1)
         .filter((s,c) => {
            vertexGraphFilterFunc(s.getVertices.getLast, s.getMainGraph)
         })
         .aggregationLongObj [RoaringBitmap] (keyFunc, valueFunc, reduceFunc)
         .values.reduce((b1,b2) => {reduceFunc(b1,b2); b1})

      val elapsed = System.nanoTime() - start

      logApp(s"FilterVerticesGivenGraph took ${elapsed * 1e-6} ms")

      val validVerticesBc = fractalContext.sparkContext.broadcast(validVertices)

      // filter based on the valid vertex set
      filterVertices((u, _) => validVerticesBc.value.contains(u))
   }

   def filterVertices
   (vertexFilterFunc: (Int,IntArrayList) => Boolean): FractalGraph = {
      val currVertexPredicate = vertexPredicate

      val newVertexPredicate = if (currVertexPredicate == null) {
         new VertexFilteringPredicate {
            override def test(u: Int, uLabels: IntArrayList): Boolean = {
               vertexFilterFunc(u, uLabels)
            }
         }
      } else {
         new VertexFilteringPredicate {
            override def test(u: Int, uLabels: IntArrayList): Boolean = {
               currVertexPredicate.test(u, uLabels) &&
                  vertexFilterFunc(u, uLabels)
            }
         }
      }

      this.copy(vertexPredicate = newVertexPredicate)
   }

   def filterEdgesGivenGraph
   (edgeGraphFilterFunc: (Int,Int,Int,MainGraph) => Boolean): FractalGraph = {

      val start = System.nanoTime()

      // determine set of valid vertices
      val keyFunc: EdgeInducedSubgraph => Long = s => 0L
      val valueFunc: EdgeInducedSubgraph => RoaringBitmap =
         s => RoaringBitmap.bitmapOf(s.getEdges.getLast)
      val reduceFunc: (RoaringBitmap, RoaringBitmap) => Unit =
         (b1,b2) => b1.or(b2)

      val validEdges = efractoid.expand(1)
         .filter((s,c) => {
            val g = s.getMainGraph
            val e = s.getEdges.getLast
            val u = g.edgeSrc(e)
            val v = g.edgeDst(e)
            edgeGraphFilterFunc(u,v,e,g)
         })
         .aggregationLongObj [RoaringBitmap] (keyFunc, valueFunc, reduceFunc)
         .values.reduce((b1,b2) => {reduceFunc(b1,b2); b1})

      val elapsed = System.nanoTime() - start

      logApp(s"FilterEdgesGivenGraph took ${elapsed * 1e-6} ms")

      val validEdgesBc = fractalContext.sparkContext.broadcast(validEdges)

      // filter based on the valid vertex set
      filterEdges((_, _, _, _, e, _) => validEdgesBc.value.contains(e))
   }

   def filterEdges
   (edgeFilterFunc: (Int,IntArrayList,Int,IntArrayList,Int,IntArrayList) => Boolean)
   : FractalGraph = {
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

   def getVertexLabelFrequencies(): RDD[(Int,Int)] = {
      val getVertexLabel: VertexInducedSubgraph => Int = s => {
         val u = s.getVertices.getLast
         s.getMainGraph.firstVertexLabel(u)
      }

      val map = vfractoid.expand(1)
         .aggregationIntInt(getVertexLabel, 0, s => 1, _ + _)

      map
   }

   lazy val vertexLabelFrequencies: Broadcast[KolobokeIntIntMapSerializerWrapper] = {
      val map = if (graphClass.contains(classOf[UnlabeledMainGraph].getSimpleName)) {
         HashIntIntMaps.getDefaultFactory.withDefaultValue(-1).newUpdatableMap()
      } else {
         val entries = getVertexLabelFrequencies().collect()
         val (keys, values) = entries.unzip
         HashIntIntMaps.newImmutableMap(keys, values, keys.size)
      }

      val wrappedMap = new KolobokeIntIntMapSerializerWrapper(map)
      fractalContext.sparkContext.broadcast(wrappedMap)
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

