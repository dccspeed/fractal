package br.ufmg.cs.systems.fractal.gmlib.quasicliques

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.graph.MainGraph
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph
import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.collection.{IntArrayList, IntArrayListView}
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListViewPool
import br.ufmg.cs.systems.fractal.{FractalGraph, Fractoid}
import com.koloboke.collect.set.IntSet
import com.koloboke.collect.set.hash.HashIntSets

/**
 * Induced quasi cliques with maxNumVertices vertices and each vertex
 * having minDensity * (|V| - 1) internal edges.
 * @param maxNumVertices
 * @param minDensity
 */
class QuasiCliquesSF(maxNumVertices: Int, minDensity: Double)
   extends BuiltInApplication[Fractoid[VertexInducedSubgraph]] {

   // reusable data structures
   private val vertexDegrees = new IntArrayList(maxNumVertices)
   private val targetDegree = Math.ceil(minDensity * (maxNumVertices - 1))

   /**
    * Check if current subgraph is quasi-clique or can become a quasi-clique
    * @param subgraph subgraph
    * @param computation computation
    * @return true - valid; false - invalid
    */
   private def isQuasiClique(subgraph: VertexInducedSubgraph,
                                 computation: Computation[VertexInducedSubgraph])
   : Boolean = {
      val vertices = subgraph.getVertices
      val numVertices = vertices.size()

      // array to store vertices degrees
      var i = 0
      vertexDegrees.clear()
      while (i < numVertices) {
         vertexDegrees.add(0)
         i += 1
      }

      // compute vertices degrees
      val edges = subgraph.quickPattern().getEdges
      val numEdges = edges.size()
      i = 0
      while (i < numEdges) {
         val pe = edges.getu(i)
         val src = pe.getSrcPos
         val dst = pe.getDestPos
         vertexDegrees.setu(src, vertexDegrees.getu(src) + 1)
         vertexDegrees.setu(dst, vertexDegrees.getu(dst) + 1)
         i += 1
      }

      // check if each vertex has sufficient degree
      i = 0
      while (i < numVertices) {
         val d = vertexDegrees.getu(i)
         if (d + (maxNumVertices - numVertices) < targetDegree) return false
         i += 1
      }

      val valid = diameterFilter(subgraph, computation)

      valid
   }

   private lazy val vertexSet: IntSet = HashIntSets.newMutableSet()

   private def distanceNeighbors(g: MainGraph, u: Int, d: Int): Unit = {
      if (d == 0) return
      val neighbors = g.neighborhoodVertices(u)
      var i = 0
      while (i < neighbors.size()) {
         val v = neighbors.getu(i)
         vertexSet.removeInt(v)
         if (vertexSet.isEmpty) {
            i = neighbors.size()
         } else {
            distanceNeighbors(g, v, d - 1)
         }
         i += 1
      }
      IntArrayListViewPool.instance().reclaimObject(neighbors)
   }

   private def diameterFilter(subgraph: VertexInducedSubgraph,
                              computation: Computation[VertexInducedSubgraph])
   : Boolean = {
      if (minDensity >= 0.5) {
         val vertices = subgraph.getVertices
         val lastVertex = vertices.getLast
         vertexSet.clear()
         vertexSet.addAll(vertices)
         distanceNeighbors(subgraph.getMainGraph, lastVertex, 2)
         vertexSet.isEmpty
      } else {
         true
      }
   }

   override def apply(fg: FractalGraph): Fractoid[VertexInducedSubgraph] = {
      val numSteps = maxNumVertices - 1
      fg.vfractoid.expand(1)
         .filter(isQuasiClique)
         .explore(numSteps)
   }
}
