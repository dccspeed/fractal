package br.ufmg.cs.systems.fractal.gmlib.quasicliques

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import br.ufmg.cs.systems.fractal.{FractalGraph, Fractoid}

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
   private def quasiCliqueFilter(subgraph: VertexInducedSubgraph,
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

      true
   }

   override def apply(fg: FractalGraph): Fractoid[VertexInducedSubgraph] = {
      val numSteps = maxNumVertices - 1
      fg.vfractoid.expand(1).filter(quasiCliqueFilter).explore(numSteps)
   }
}
