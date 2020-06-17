package br.ufmg.cs.systems.fractal.gmlib.periodic

import java.io.Serializable
import java.util.function.IntConsumer

import br.ufmg.cs.systems.fractal.{FractalGraph, Fractoid}
import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.keywordsearch.BuiltInApplication
import br.ufmg.cs.systems.fractal.graph.MainGraph
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph
import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListViewPool
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool

/**
 * Periodic induced subgraphs assumes each each has a sequence of ordered
 * timestamps as labels representing periods where that particular edge is
 * active in the input graph. This implementation strategy is naive and just
 * filters subgraphs having the periodic property:
 * - must exist a (sub)sequence in this common timestamps with size at least
 * *periodicThreshold* and also the time difference between consecutive
 * timestamps must be the same for the whole (sub)sequence
 */
object InducedPeriodicSubgraphs extends Logging {

   /**
    * Consumer used to process common timestamps of a set of edges
    */
   class PeriodicConsumer extends IntConsumer with Serializable {
      val periodicTime: IntArrayList = new IntArrayList()
      def clear(): Unit = periodicTime.clear()
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

      val stack = IntArrayListPool.instance().createObject()
      val indices = IntArrayListPool.instance().createObject()
      var step = -1

      stack.add(-1)
      var i = numTimestamps - 1
      while (i >= 0) {
         stack.add(i)
         i -= 1
      }

      var continue = stack.size() > 0
      var foundPeriodicTimestamps = false

      while (continue) {
         val idx = stack.remove(stack.size() - 1)
         if (idx == -1) {
            indices.removeLast()
         } else {
            var valid = true

            indices.add(idx)
            val indicesSize = indices.size()
            if (indicesSize > 1) {
               if (indicesSize == 2) {
                  step = timestamps.getu(indices.getu(1)) -
                     timestamps.getu(indices.getu(0))
               } else if (timestamps.getu(indices.getu(indicesSize - 1)) -
                  timestamps.getu(indices.getu(indicesSize - 2)) != step) {
                  indices.removeLast()
                  valid = false
               }
            }

            if (valid) {
               if (indicesSize == periodicThreshold) {
                  // periodic condition holds
                  continue = false
                  foundPeriodicTimestamps = true
               } else {
                  indices.add(-1)
                  i = idx + 1
                  while (i < numTimestamps) {
                     stack.add(i)
                     i += 1
                  }
               }
            }
         }
      }

      IntArrayListPool.instance().reclaimObject(stack)
      IntArrayListPool.instance().reclaimObject(indices)

      logInfo(s"PeriodicTimestamp ${indices} ${foundPeriodicTimestamps}")

      foundPeriodicTimestamps
   }

   /**
    * Periodic filter function to be used on each subgraph
    * @param s subgraph
    * @param c computation
    * @param periodicThreshold subgraph must be periodic at this rate
    * @param consumer to consumer common edge labels in the input graph
    * @return
    */
   private def periodicFilter
   (s: VertexInducedSubgraph, c: Computation[VertexInducedSubgraph],
    periodicThreshold: Int, consumer: PeriodicConsumer): Boolean = {
      if (s.getNumVertices == 1) return true
      val graph = c.getConfig.getMainGraph[MainGraph[_,_]]
      consumer.clear()
      graph.forEachCommonEdgeLabels(s.getEdges, consumer)
      isPeriodic(consumer.periodicTime, periodicThreshold)
   }

   /**
    * Main entry for this built-in application
    * @param fgraph
    * @param periodicThreshold
    * @return fractoid that can be explored an arbitrary number of times
    */
   def apply(fgraph: FractalGraph, periodicThreshold: Int)
   : Fractoid[VertexInducedSubgraph] = {
      val consumer = new PeriodicConsumer
      fgraph.vfractoid.expand(1).filter(
         (s,c) => periodicFilter(s, c, periodicThreshold, consumer)
      )
   }
}
