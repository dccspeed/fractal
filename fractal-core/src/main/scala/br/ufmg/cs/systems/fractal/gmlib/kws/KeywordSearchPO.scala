package br.ufmg.cs.systems.fractal.gmlib.kws

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.subgraph.EdgeInducedSubgraph
import br.ufmg.cs.systems.fractal.{FractalGraph, Fractoid}
import com.koloboke.collect.set.IntSet
import com.koloboke.collect.set.hash.HashIntSets

/**
 */
class KeywordSearchPO(keywords: Set[Int], numEdges: Int)
   extends BuiltInApplication[Fractoid[EdgeInducedSubgraph]] {

   private lazy val subgraphLabels: IntSet =
      HashIntSets.newUpdatableSet(keywords.size)

   private lazy val lastEdgeLabels: IntSet =
      HashIntSets.newUpdatableSet(keywords.size)

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

   private def lastEdgeCoversNewWord
   (s: EdgeInducedSubgraph, c: Computation[EdgeInducedSubgraph])
   : Boolean = {
      val lastEdgeLabels = getLastEdgeLabels(s)

      // last edge does not cover any keyword
      if (lastEdgeLabels.isEmpty) return false

      // last edge is the first to cover new keywords
      if (s.getNumEdges == 1) return true

      val subgraphLabels = getSubgraphLabels(s)

      // each edge covers keywords not covered by any other edge
      !lastEdgeLabels.containsAll(subgraphLabels) &&
         !subgraphLabels.containsAll(lastEdgeLabels)
   }

   private def coversAllKeywords
   (s: EdgeInducedSubgraph, c: Computation[EdgeInducedSubgraph]): Boolean = {
      val subgraphLabels = getLastEdgeLabels(s)
      subgraphLabels.addAll(getSubgraphLabels(s))

      subgraphLabels.equals(keywordsSet)
   }

   override def apply(fgraph: FractalGraph): Fractoid[EdgeInducedSubgraph] = {
      fgraph.efractoid
         .extend(1)
         .filter(lastEdgeCoversNewWord _)
         .explore(numEdges - 1)
         .filter(coversAllKeywords _)
   }
}
