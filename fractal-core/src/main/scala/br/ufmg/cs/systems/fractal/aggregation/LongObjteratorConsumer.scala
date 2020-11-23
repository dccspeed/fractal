package br.ufmg.cs.systems.fractal.aggregation

import br.ufmg.cs.systems.fractal.subgraph.Subgraph
import com.koloboke.collect.map.{LongLongCursor, LongObjCursor}

class LongObjteratorConsumer[S <: Subgraph, V <: java.io.Serializable]
(val agg: LongObjSubgraphAggregation[S,V]) extends Iterator[(Long, V)] {

   private var cursor: LongObjCursor[V] = _
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
         false
      }
   }

   override def next(): (Long, V) = {
      nextConsumed = true
      (cursor.key(), cursor.value())
   }
}
