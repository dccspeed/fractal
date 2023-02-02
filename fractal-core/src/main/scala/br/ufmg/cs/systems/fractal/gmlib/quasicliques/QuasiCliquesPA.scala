package br.ufmg.cs.systems.fractal.gmlib.quasicliques

import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternUtilsRDD}
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import br.ufmg.cs.systems.fractal.{FractalGraph, Fractoid}

/**
 * Induced quasi cliques with maxNumVertices vertices and each vertex
 * having minDensity * (|V| - 1) internal edges.
 * @param maxNumVertices
 * @param minDensity
 */
class QuasiCliquesPA(maxNumVertices: Int, minDensity: Double)
   extends BuiltInApplication[(Int,Iterator[Fractoid[PatternInducedSubgraph]])] {

   // reusable data structures
   private val vertexDegrees = new IntArrayList(maxNumVertices)

   def quasiCliquePatternFilter(p: Pattern, d: Int): Boolean = {
      val numVertices = p.getNumberOfVertices
      val edges = p.getEdges
      vertexDegrees.clear()
      var i = 0
      while (i < numVertices) {
         vertexDegrees.add(0)
         i += 1
      }

      i = 0
      while (i < edges.size()) {
         val e = edges.getu(i)
         val src = e.getSrcPos
         val dst = e.getDestPos
         vertexDegrees.setu(src, vertexDegrees.getu(src) + 1)
         vertexDegrees.setu(dst, vertexDegrees.getu(dst) + 1)
         i += 1
      }

      var valid = true
      i = 0
      while (i < numVertices && valid) {
         if (vertexDegrees.getu(i) < d) valid = false
         i += 1
      }

      valid
   }

   override def apply(fg: FractalGraph)
   : (Int, Iterator[Fractoid[PatternInducedSubgraph]]) = {
      val fc = fg.fractalContext
      val sc = fc.sparkContext
      val minDegree = Math.ceil(minDensity * (maxNumVertices - 1)).toInt
      val patternCandidates = PatternUtilsRDD
         .getOrGenerateVertexPatternsRDD(sc, maxNumVertices)
      val quasiCliquePatterns = patternCandidates
         .filter(p => quasiCliquePatternFilter(p, minDegree))
         .map(p => {
            p.setVertexLabeled(false)
            p.setInduced(true)
            p
         })

      val numSteps = quasiCliquePatterns.count().toInt
      val sortedPatterns = quasiCliquePatterns.sortBy(p => -p.getNumberOfEdges)
      val fractoidsIter = sortedPatterns.toLocalIterator
         .map(p => fg.pfractoid(p).expand(p.getNumberOfVertices))

      (numSteps, fractoidsIter)
   }
}
