package br.ufmg.cs.systems.fractal.aggregation

import br.ufmg.cs.systems.fractal.subgraph.Subgraph
import com.koloboke.collect.map.ObjLongCursor

class ObjLongIteratorConsumer[S <: Subgraph, K <: java.io.Serializable]
(val agg: ObjLongSubgraphAggregation[S, K], finishCallback: () => Unit)
   extends Iterator[(K, Long)] {

   private var cursor: ObjLongCursor[K] = _
   private var readyToFinish: Boolean = false
   private var finished: Boolean = false
   private var nextConsumed: Boolean = true

   def finishIterator: Unit = synchronized {
      readyToFinish = true
      while (!finished) {
         agg.notifyWorkProduced()
         wait(10)
      }
   }

   override def hasNext: Boolean = {
      if (!nextConsumed) return true

      while (!readyToFinish) {
         if (cursor == null) {
            agg.waitWorkProduced()
            cursor = agg.getKeyValueMap().cursor()
         }
         if (cursor.moveNext()) {
            nextConsumed = false
            return true
         }
         cursor = null
         agg.notifyWorkConsumed()
      }

      // if execution reached this point, this iteration is finished
      if (cursor == null) {
         agg.waitWorkProduced()
         cursor = agg.getKeyValueMap.cursor()
      }

      // make sure we DO NOT make cursor null, to prevent an additional
      // *waitWorkProduced* call
      if (cursor.moveNext()) {
         nextConsumed = false
         true
      } else {
         finished = true
         finishCallback()
         false
      }
   }

   override def next(): (K, Long) = {
      nextConsumed = true
      (cursor.key(), cursor.value())
   }
}
