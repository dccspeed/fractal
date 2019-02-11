package io.arabesque.extender;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.function.IntConsumer;
import com.koloboke.function.IntIntPredicate;
import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.set.hash.HashIntSet;

import io.arabesque.computation.Computation;
import io.arabesque.embedding.Embedding;
import io.arabesque.graph.MainGraph;
import io.arabesque.graph.VertexNeighbourhood;

import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.ObjArrayList;
import io.arabesque.utils.pool.IntArrayListPool;
import io.arabesque.utils.pool.IntIntMapPool;
import io.arabesque.utils.pool.HashIntSetPool;
import io.arabesque.utils.Utils;

import java.io.*;

public class GtrieExtender3 extends GtrieExtender {

   private static final IntArrayList patterns = new IntArrayList() {
      {
         add(0b00000000000000000000000000000000);
            add(0b00000000000000000000000000000010);
               add(0b00000000000000000000000000001110);
               add(0b00000000000000000000000000001010);
      }
   };

   // <pattern> <id> <depth> <mask> <parentpos> <nchildren> <childrenpos ...>
   private static final IntArrayList gtrie3 = new IntArrayList() {
      {
         add(patterns.get(0)); add(0); add(0); add(0); add(-1); add(1); add(7);
            add(patterns.get(1)); add(1); add(1); add(1); add(0); add(2); add(15); add(21);
               add(patterns.get(2)); add(2); add(2); add(2); add(7); add(0);
               add(patterns.get(3)); add(3); add(2); add(2); add(7); add(0);
      }
   };
   
   public GtrieExtender3() {
      super();
      this.gtrie = gtrie3;
   }

   @Override
   public boolean testSb(Embedding e, int pattern, int v) {
      if (e.getNumVertices() + 1 < 3) {
         return true;
      }

      IntArrayList vertices = e.getVertices();
      if (pattern == patterns.get(2)) {
         return vertices.get(0) < vertices.get(1) && vertices.get(1) < v;
      } else if (pattern == patterns.get(3)) {
         return vertices.get(0) < v;
      } else {
         throw new RuntimeException("Not allowed");
      }
   }
}
