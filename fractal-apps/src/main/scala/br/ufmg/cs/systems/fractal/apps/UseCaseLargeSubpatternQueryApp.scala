package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.computation.{Computation, SubgraphEnumerator, SupernodeEnumerator}
import br.ufmg.cs.systems.fractal.conf.Configuration
import br.ufmg.cs.systems.fractal.graph.MainGraph
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternUtils, PatternUtilsRDD}
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
      val fg = fc.textFile(graphPath,
         graphClass = "br.ufmg.cs.systems.fractal.graph.VELabeledMainGraph")
         .set("ws_external", false)

      // sampled pattern not containing label 14
      val targetLabel = 14
      val pattern = fg.getPatternsFromSamples(1, 8).first()
      pattern.setInduced(true)
      pattern.setVertexLabeled(true)

      // all extended patterns by one vertex with target label
      val extendedPatterns = PatternUtilsRDD.extendByVertexRDD(
         sc.parallelize(List(pattern)), targetLabel).collect()
      val numExtendedPatterns = extendedPatterns.size
      logApp(s"NumExtendedPatterns ${numExtendedPatterns}")

      // count each pattern query concurrently
      val start1 = System.currentTimeMillis()
      var futures: List[Future[(Pattern,Long)]] = List.empty
      var numExtendedSubgraphsPA = 0L
      var numExtendedPatternsWithZeroCount = 0L
      val cur = extendedPatterns.iterator
      while (cur.hasNext) {
         val extendedPattern = cur.next()
         extendedPattern.setInduced(true)
         extendedPattern.setVertexLabeled(true)
         val future = Future((extendedPattern, fg
            .pfractoid(extendedPattern)
            .expand(extendedPattern.getNumberOfVertices)
            .aggregationCount))

         futures = future :: futures
      }

      futures.foreach(future => {
         val (extendedPattern, numExtendedSubgraphsTmp) =
            Await.result(future, Duration.Inf)

         if (numExtendedSubgraphsTmp == 0) {
            numExtendedPatternsWithZeroCount += 1
         }

         logApp(s"ExtendedPatternPA ${extendedPattern} -> " +
            s"${numExtendedSubgraphsTmp}")
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
