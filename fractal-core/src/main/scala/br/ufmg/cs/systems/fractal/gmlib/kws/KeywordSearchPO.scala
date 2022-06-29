package br.ufmg.cs.systems.fractal.gmlib.kws

import java.io.Serializable
import java.util.function.IntConsumer

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import br.ufmg.cs.systems.fractal.{FractalGraph, Fractoid}
import com.koloboke.collect.set.IntSet
import com.koloboke.collect.set.hash.HashIntSets

/**
 */
class KeywordSearchPO(keywords: Set[Int], numEdges: Int)
   extends BuiltInApplication[Fractoid[EdgeInducedSubgraph]] {

   private val numKeywords: Int = keywords.size

   private lazy val subgraphLabels: IntSet =
      HashIntSets.newUpdatableSet(numKeywords)

   private lazy val lastEdgeLabels: IntSet =
      HashIntSets.newUpdatableSet(numKeywords)

   private lazy val keywordsSet: IntSet =
      HashIntSets.newImmutableSet(keywords.toArray)

   private def getSubgraphLabels(subgraph: EdgeInducedSubgraph): IntSet = {
      val graph = subgraph.getMainGraph
      val vertices = subgraph.getVertices
      val numEdges = subgraph.getNumEdges
      subgraphLabels.clear()
      var i = 0
      var j = 0
      while (i < numEdges - 1) {
         val numVerticesAdded = subgraph.numVerticesAdded(i)
         val n = j + numVerticesAdded
         while (j < n) {
            val l = graph.firstVertexLabel(vertices.getu(j))
            if (keywordsSet.contains(l)) {
               subgraphLabels.add(l)
            }
            j += 1
         }
         i += 1
      }
      subgraphLabels
   }

   private def getLastEdgeLabels(subgraph: EdgeInducedSubgraph): IntSet = {
      val graph = subgraph.getMainGraph
      val vertices = subgraph.getVertices
      val numVertices = vertices.size()
      val numEdges = subgraph.getNumEdges
      lastEdgeLabels.clear()

      val numVerticesAdded = subgraph.numVerticesAdded(numEdges - 1)
      var from = numVertices - numVerticesAdded

      while (from < numVertices) {
         val l = graph.firstVertexLabel(vertices.getu(from))
         if (keywordsSet.contains(l)) {
            lastEdgeLabels.add(l)
         }
         from += 1
      }

      lastEdgeLabels
   }

   private def keywordSearchFilter
   (s: EdgeInducedSubgraph, c: Computation[EdgeInducedSubgraph])
   : Boolean = {
      val lastEdgeLabels = getLastEdgeLabels(s)
      val subgraphLabels = getSubgraphLabels(s)

      !lastEdgeLabels.containsAll(subgraphLabels) &&
         !subgraphLabels.containsAll(lastEdgeLabels)
   }

   private def hasKeywordsFilter
   (s: EdgeInducedSubgraph, c: Computation[EdgeInducedSubgraph])
   : Boolean = {
      val graph = s.getMainGraph
      val vertices = s.getVertices
      val numVertices = vertices.size()
      val numEdges = s.getNumEdges

      val numVerticesAdded = s.numVerticesAdded(numEdges - 1)
      var from = numVertices - numVerticesAdded

      while (from < numVertices) {
         val l = graph.firstVertexLabel(vertices.getu(from))
         if (keywords.contains(l)) {
            return true
         }
         from += 1
      }

      false
   }

   override def apply(fgraph: FractalGraph): Fractoid[EdgeInducedSubgraph] = {
      var frac = fgraph.efractoid.expand(1).filter(hasKeywordsFilter _)
      var i = 1
      while (i < numKeywords) {
         frac = frac.expand(1)
            .filter(hasKeywordsFilter _)
            .filter(keywordSearchFilter _)
         i += 1
      }

      frac
   }
}
