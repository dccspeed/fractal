package br.ufmg.cs.systems.fractal.gmlib.kws

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import br.ufmg.cs.systems.fractal.{FractalGraph, Fractoid}
import com.koloboke.collect.map.IntIntMap
import com.koloboke.collect.map.hash.HashIntIntMaps
import com.koloboke.collect.set.IntSet
import com.koloboke.collect.set.hash.HashIntSets

/**
 * Minimal subgraph version of keyword search: in this setting each keyword
 * (label) must be covered by some vertex and the resulting subgraph should
 * not contain unnecessary vertices (minimal).
 */
class MinimalKeywordSearchPO(keywords: Set[Int], numVertices: Int)
   extends BuiltInApplication[Fractoid[VertexInducedSubgraph]] {

   private val numKeywords: Int = keywords.size

   private val keywordsArray: IntArrayList =
      new IntArrayList(keywords.toArray)

   private lazy val labelsCount: IntIntMap = HashIntIntMaps.getDefaultFactory()
      .withDefaultValue(0).newUpdatableMap(numKeywords)

   private val visited: IntArrayList = new IntArrayList(numKeywords)

   private val depth: IntArrayList = new IntArrayList(numKeywords)

   private val low: IntArrayList = new IntArrayList(numKeywords)

   private lazy val articulationPoints: IntSet =
      HashIntSets.newUpdatableSet(numKeywords)

   private def reset(): Unit = {
      visited.clear()
      depth.clear()
      low.clear()
      articulationPoints.clear()
      var i = 0
      while (i < numVertices) {
         visited.add(0)
         depth.add(0)
         low.add(0)
         i += 1
      }
   }

   private def checkAndFillLabelCounts
   (s: VertexInducedSubgraph, c: Computation[VertexInducedSubgraph]): Boolean = {
      val graph = s.getMainGraph
      val vertices = s.getVertices
      val numVertices = vertices.size()
      labelsCount.clear()

      var i = 0
      while (i < numVertices) {
         val u = vertices.getu(i)
         val uLabel = graph.firstVertexLabel(u)
         if (keywords.contains(uLabel)) {
            val n = labelsCount.addValue(uLabel, 1)
            if (n > 1) {
               return false
            }
         }
         i += 1
      }

      true
   }

   private def fillArticulationPoints(pattern: Pattern, u: Int,
                                      parent: Int, d: Int): Unit = {
      visited.set(u, 1)
      depth.set(u, d)
      low.set(u, d)

      val adjList = pattern.getVertexPosToEdgeIndices.get(u)
      val edges = pattern.getEdges

      var numChildren = 0
      var i = 0
      while (i < adjList.size()) {
         val pedge = edges.getu(adjList.getu(i))
         val v = if (pedge.getSrcPos == u) pedge.getDestPos else pedge.getSrcPos
         if (visited.getu(v) == 0) {
            numChildren += 1
            fillArticulationPoints(pattern, v, u, d + 1)

            // back edge from v subtree to some ancestor of u
            low.set(u, Math.min(low.getu(u), low.getu(v)))

            // if v subtree do not connect to one of u ancestors, it means
            // that there is no way to reach those vertices without passing by u
            // thus, u is an articulation point
            if (parent != -1 && low.get(v) >= depth.getu(u)) {
               articulationPoints.add(u)
            }
         } else if (v != parent) {
            low.set(u, Math.min(low.getu(u), depth.getu(v)))
         }

         i += 1
      }

      // the root of the tree is an articulation point if it has more than
      // one child
      if (parent == -1 && numChildren > 1) {
         articulationPoints.add(u)
      }
   }

   private def minimalKeywordSearchFilter
   (s: VertexInducedSubgraph, c: Computation[VertexInducedSubgraph]): Boolean = {
      if (!checkAndFillLabelCounts(s, c)) return false

      // check whether all labels are covered
      var i = 0
      while (i < numKeywords) {
         val l = keywordsArray.getu(i)
         if (labelsCount.get(l) != 1) {
            return false
         }
         i += 1
      }

      // fill articulation points and check whether each vertex is essential
      // to cover this many labels
      val graph = s.getMainGraph
      val pattern = s.quickPattern()
      val vertices = s.getVertices
      val numVertices = pattern.getNumberOfVertices
      reset()
      fillArticulationPoints(pattern, 0, -1, 0)

      i = 0
      while (i < numVertices) {
         val u = vertices.getu(i)
         val uLabel = graph.firstVertexLabel(u)
         if (!keywords.contains(uLabel) && !articulationPoints.contains(i)) {
            return false
         }
         i += 1
      }

      true
   }

   override def apply(fgraph: FractalGraph): Fractoid[VertexInducedSubgraph] = {
      fgraph.vfractoid
         .extend(1)
         .filter(checkAndFillLabelCounts _)
         .explore(numVertices - 1)
         .filter(minimalKeywordSearchFilter _)
   }
}
