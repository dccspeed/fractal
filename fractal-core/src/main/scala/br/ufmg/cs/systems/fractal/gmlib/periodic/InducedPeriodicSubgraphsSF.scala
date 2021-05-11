package br.ufmg.cs.systems.fractal.gmlib.periodic

import java.io.Serializable
import java.util.function.IntConsumer

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.graph.MainGraph
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import br.ufmg.cs.systems.fractal.{FractalGraph, Fractoid}

/**
 * Periodic induced subgraphs assumes each each has a sequence of ordered
 * timestamps as labels representing periods where that particular edge is
 * active in the input graph. This implementation strategy is naive and just
 * filters subgraphs having the periodic property:
 * - must exist a (sub)sequence in this common timestamps with size at least
 * *periodicThreshold* and also the time difference between consecutive
 * timestamps must be the same for the whole (sub)sequence
 */
class InducedPeriodicSubgraphsSF(periodicThreshold: Int)
   extends BuiltInApplication[Fractoid[VertexInducedSubgraph]] {

   private val periodicTime = new IntArrayList()
   private val consumer = new PeriodicConsumer
   private val stack = new IntArrayList()
   private val indices = new IntArrayList()

   /**
    * Consumer used to process common timestamps of a set of edges
    */
   class PeriodicConsumer extends IntConsumer with Serializable {
      override def accept(timestamp: Int): Unit = periodicTime.add(timestamp)
   }

   /**
    * Verifies whether a set of timestamps complies with the periodic property
    * @param timestamps
    * @param periodicThreshold
    * @return boolean indicating whether these timestamps are periodic
    */
   private def isPeriodic(timestamps: IntArrayList, periodicThreshold: Int)
   : Boolean = {
      val numTimestamps = timestamps.size()

      if (numTimestamps < periodicThreshold) return false

      stack.clear()
      indices.clear()
      var step = -1

      // initialize candidates stack
      stack.add(-1)
      var i = numTimestamps - 1
      while (i >= 0) {
         stack.add(i)
         i -= 1
      }

      var continue = !stack.isEmpty
      var foundPeriodicTimestamps = false

      while (continue) {
         val idx = stack.remove(stack.size() - 1) // next timestamp index

         if (idx == -1) { // depth limit reached, backtrack
            indices.removeLast()

         } else { // consume next timestamp index
            var valid = true

            indices.add(idx)
            val indicesSize = indices.size()

            if (indicesSize == 2) { // new period step
               step = timestamps.getu(indices.getu(1)) -
                  timestamps.getu(indices.getu(0))
            } else if (indicesSize > 1
                  && timestamps.getu(indices.getu(indicesSize - 1)) -
                  timestamps.getu(indices.getu(indicesSize - 2)) != step
            ) { // period step exists but next interval is not periodic
               indices.removeLast()
               valid = false
            }

            if (valid) { // current sequence is valid
               if (indicesSize == periodicThreshold) {
                  /**
                   * periodic condition holds, finish searching
                   */
                  continue = false
                  foundPeriodicTimestamps = true
               } else { // valid sequence has not reached min size yet
                  /**
                   * add new index candidates for next level
                   */
                  stack.add(-1)
                  i = idx + 1
                  while (i < numTimestamps) {
                     stack.add(i)
                     i += 1
                  }
               }
            }
         }

         continue = continue && !stack.isEmpty
      }

      foundPeriodicTimestamps
   }

   /**
    * Periodic filter function to be used on each subgraph
    * @param s subgraph
    * @param c computation
    * @return
    */
   private def periodicFilter
   (s: VertexInducedSubgraph, c: Computation[VertexInducedSubgraph])
   : Boolean = {
      if (s.getNumVertices == 1) return true
      val graph = c.getConfig.getMainGraph
      periodicTime.clear()
      graph.forEachCommonEdgeLabels(s.getEdges, consumer)
      isPeriodic(periodicTime, periodicThreshold)
   }

   /**
    * Main entry for this built-in application
    * @param fgraph
    * @return fractoid that can be explored an arbitrary number of times
    */
   override def apply(fgraph: FractalGraph): Fractoid[VertexInducedSubgraph] = {
      fgraph.vfractoid.expand(1).filter(
         (s,c) => periodicFilter(s, c)
      )
   }
}
