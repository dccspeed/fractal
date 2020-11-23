package br.ufmg.cs.systems.fractal.aggregation

import br.ufmg.cs.systems.fractal.subgraph.Subgraph
import com.koloboke.collect.map.ObjObjCursor

class ObjObjIteratorConsumer
[S <: Subgraph, K <: java.io.Serializable, V <: java.io.Serializable]
(val agg: ObjObjSubgraphAggregation[S, K, V]) extends Iterator[(K, V)] {

   private var cursor: ObjObjCursor[K, V] = _
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
            cursor = agg.getKeyValueMap.cursor()
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
         false
      }
   }

   override def next(): (K, V) = {
      nextConsumed = true
      (cursor.key(), cursor.value())
   }
}
