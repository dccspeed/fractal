package br.ufmg.cs.systems.fractal.apps

import java.util
import java.util.Random
import java.util.concurrent.ThreadLocalRandom

import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.computation.{Computation, SubgraphEnumerator, SupernodeEnumerator}
import br.ufmg.cs.systems.fractal.conf.Configuration
import br.ufmg.cs.systems.fractal.graph.MainGraph
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternEdge, PatternUtils, PatternUtilsRDD}
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph
import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.collection.IntArrayListView
import com.koloboke.collect.set.IntSet
import com.koloboke.collect.set.hash.HashIntSets
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits._

object UseCaseLargeSubpatternQueryApp extends Logging {
   def main(args: Array[String]): Unit = {
      // environment setup
      val conf = new SparkConf().setAppName("MyMotifsApp")
      val sc = new SparkContext(conf)
      val fc = new FractalContext(sc, "app")
      val graphPath = args(0) // input graph
      val numVertices = args(1).toInt
      val random = new Random(args(2).toInt)
      val fg = fc.textFile(graphPath,
         graphClass = "br.ufmg.cs.systems.fractal.graph.VELabeledMainGraph")
         .set("ws_external", false)

      val getLabelFromVertex: VertexInducedSubgraph => Long = s => {
         s.getMainGraph.firstVertexLabel(s.getVertices.getLast)
      }

      // sampling pattern
      val pattern = fg.getPatternsFromSamples(1, numVertices, random.nextInt()).first()
      pattern.setInduced(true)
      pattern.setVertexLabeled(true)
      logApp(s"RandomPattern ${pattern}")

      // random target label
      val vlabels = fg.vfractoid.expand(1)
         .aggregationLongLong(getLabelFromVertex, 0L, s => 0L, _ + _)
         .keys
         .collect()
         .toSet
      logApp(s"VertexLabels ${vlabels}")
      val plabels = pattern.getEdges.toArray
         .map(o => o.asInstanceOf[PatternEdge])
         .flatMap(e => List(e.getDestLabel.toLong, e.getSrcLabel.toLong))
         .toSet
      logApp(s"PatternLabels ${plabels}")
      val candidateLabels = vlabels.diff(plabels).toArray
      util.Arrays.sort(candidateLabels)
      logApp(s"CandidateLabels ${candidateLabels.mkString(",")}")
      val targetLabelIdx = random.nextInt(candidateLabels.size)
      logApp(s"TargetLabelIdx ${targetLabelIdx}")
      val targetLabel = candidateLabels(targetLabelIdx).toInt
      logApp(s"TargetLabel ${targetLabel}")

      // all extended patterns by one vertex with target label
      val extendedPatterns = PatternUtilsRDD.extendByVertexRDD(
         sc.parallelize(List(pattern)), targetLabel).collect()
      val numExtendedPatterns = extendedPatterns.size
      logApp(s"NumExtendedPatterns ${numExtendedPatterns}")

      // count each pattern query concurrently
      val start1 = System.currentTimeMillis()
      var futures: List[Future[Long]] = List.empty
      var numExtendedPatternsWithZeroCount = 0L
      val cur = extendedPatterns.iterator
      while (cur.hasNext) {
         val extendedPattern = cur.next()
         extendedPattern.setInduced(true)
         extendedPattern.setVertexLabeled(true)
         val future = Future({
            val c = fg.pfractoid(extendedPattern)
               .expand(extendedPattern.getNumberOfVertices)
               .aggregationCount
            logApp(s"ExtendedPatternPA ${extendedPattern} -> ${c}")
            c
         })

         futures = future :: futures
      }

      var numExtendedSubgraphsPA = 0L
      futures.foreach(future => {
         val numExtendedSubgraphsTmp = Await.result(future, Duration.Inf)

         if (numExtendedSubgraphsTmp == 0) {
            numExtendedPatternsWithZeroCount += 1
         }

         numExtendedSubgraphsPA += numExtendedSubgraphsTmp
      })

      val elapsed1 = System.currentTimeMillis() - start1

      val start2 = System.currentTimeMillis()

      // visit same subgraphs using a hybrid approach: match and explore
      val extendedPatternsHybrid = fg
         .pfractoid(pattern).expand(pattern.getNumberOfVertices)
         .vfractoid
         .expand(1, classOf[NeighborhoodEnumerator])
         .filter((s,c) => {
            val graph = s.getMainGraph
            val lastVertex = s.getVertices.getLast
            graph.firstVertexLabel(lastVertex) == targetLabel
         })

      val numExtendedSubgraphsHybrid1 = extendedPatternsHybrid
         .aggregationCount

      val elapsed2 = System.currentTimeMillis() - start2

      logApp(s"NumExtendedSubgraphsPA" +
         s" numExtendedPatterns=${numExtendedPatterns}" +
         s" numExtendedPatternsWithZeroCount=${numExtendedPatternsWithZeroCount}" +
         s" numSubgraphs=${numExtendedSubgraphsPA}" +
         s" elapsed=${elapsed1}")
      logApp(s"NumExtendedSubgraphsHybrid " +
         s"numSubgraphs=${numExtendedSubgraphsHybrid1} elapsed=${elapsed2}")

      // environment cleaning
      fc.stop()
      sc.stop()
   }
}

class NeighborhoodEnumerator extends SubgraphEnumerator[VertexInducedSubgraph] {
   private var extensionSet: IntSet = _
   private var neighbors: IntArrayListView = _
   private var graph: MainGraph = _

   override def init(config: Configuration,
                     computation: Computation[VertexInducedSubgraph])
   : Unit = {
      extensionSet = HashIntSets.newMutableSet()
      neighbors = new IntArrayListView
   }

   override def computeExtensions_EXTENSION_PRIMITIVE(): Unit = {
      val graph = subgraph.getMainGraph
      val vertices = subgraph.getVertices
      val numVertices = vertices.size()

      // add all vertices in the neighborhood
      extensionSet.clear()
      var i = 0
      while (i < numVertices) {
         val u = vertices.getu(i)
         graph.neighborhoodVertices(u, neighbors)
         var j = 0
         while (j < neighbors.size()) {
            extensionSet.add(neighbors.getu(j))
            j += 1
         }
         i += 1
      }

      // remove vertices already in this subgraph
      i = 0
      while (i < numVertices) {
         extensionSet.removeInt(vertices.getu(i))
         i += 1
      }

      // add extensions as valid
      extensions.setFrom(extensionSet)
      newExtensions(extensions)
   }
}
