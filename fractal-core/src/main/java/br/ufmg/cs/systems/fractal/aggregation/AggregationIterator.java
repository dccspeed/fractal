package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import com.koloboke.collect.map.ObjObjCursor;
import com.koloboke.collect.map.hash.HashObjObjMap;
import org.apache.hadoop.io.Writable;
import scala.Tuple2;

import java.util.Iterator;

public class AggregationIterator<K extends Writable, V extends Writable> implements Iterator<Tuple2<K,V>> {
   private final ObjArrayList<ObjObjCursor<K,V>> cursors;
   private ObjObjCursor<K,V> currCursor;
   private boolean mapAddingFinished;

   public AggregationIterator() {
      cursors = new ObjArrayList<>();
      mapAddingFinished = false;
   }

   public void addMap(HashObjObjMap<K,V> map) {
      ObjObjCursor<K,V> cursor = map.cursor();
      synchronized (cursors) {
         cursors.add(cursor);
      }
   }

   public void setMapAddingFinished(boolean mapAddingFinished) {
      this.mapAddingFinished = mapAddingFinished;
   }

   private void trySetCurrentCursor() {
      if (currCursor == null) {
         synchronized (cursors) {
            if (!cursors.isEmpty()) {
               int lastIdx = cursors.size() - 1;
               currCursor = cursors.remove(lastIdx);
               cursors.setu(lastIdx, null);
            }
         }
      }
   }

   @Override
   public boolean hasNext() {
      while (true) {
         trySetCurrentCursor();

         if (currCursor != null) {
            if (currCursor.moveNext()) return true;
            else {
               currCursor = null;
               continue;
            }
         } else {
            if (mapAddingFinished) {
               trySetCurrentCursor();
               if (currCursor != null) continue;
               else break;
            }
            else {
               // wait
               synchronized (this) {
                  try {
                     this.wait();
                  } catch (InterruptedException e) {
                     e.printStackTrace();
                  }
               }
            }
         }
      }

      return false;
   }

   @Override
   public Tuple2<K, V> next() {
      return Tuple2.apply(currCursor.key(), currCursor.value());
   }
}
