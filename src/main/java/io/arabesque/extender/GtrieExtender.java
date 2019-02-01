package io.arabesque.extender;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.function.IntConsumer;

import io.arabesque.computation.Computation;
import io.arabesque.embedding.Embedding;
import io.arabesque.graph.VertexNeighbourhood;

import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.ObjArrayList;
import io.arabesque.utils.pool.IntArrayListPool;
import io.arabesque.utils.pool.IntIntMapPool;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

public class GtrieExtender extends Extender {

   public static final IntArrayList gtrie3 = new IntArrayList() {
      {
         add(0b00000000000000000000000000000000); add(1); add(1);
            add(0b00000000000000000000000000000010); add(2); add(2);
               add(0b00000000000000000000000000001010); add(0); add(4);
               add(0b00000000000000000000000000001110); add(0); add(4);
      }
   };

   // current position in gtrie
   protected int gtriePos;

   // cummulative pattern
   protected IntArrayList cummulativePattern;

   // mapping from vertex id to pattern, per enumeration depth
   protected ObjArrayList<IntIntMap> vertexToPatternPerDepth;

   private IntArrayList includes;
   private IntArrayList excludes;

   private IncludeConsumer includeConsumer = new IncludeConsumer();
   private ExcludeConsumer excludeConsumer = new ExcludeConsumer();

   public GtrieExtender() {
      this.gtriePos = 0;
      this.cummulativePattern = new IntArrayList();
      this.vertexToPatternPerDepth = new ObjArrayList<IntIntMap>();
      this.includes = new IntArrayList();
      this.excludes = new IntArrayList();
   }

   public IntIntMap getAndSet(int depth) {
      IntIntMap vertexToPattern;
      if (depth < vertexToPatternPerDepth.size()) {
         vertexToPattern = vertexToPatternPerDepth.getUnchecked(depth);
      } else if (depth == vertexToPatternPerDepth.size()) {
         vertexToPattern = IntIntMapPool.instance().createObject();
         vertexToPatternPerDepth.add(vertexToPattern);
      } else {
         throw new RuntimeException("Not allowed");
      }
      return vertexToPattern;
   }

   @Override
   protected IntCollection extend(Computation c, Embedding e) {
      IntArrayList vertices = e.getVertices();
      int numVertices = vertices.size();
      int pattern = gtrie3.getUnchecked(gtriePos);
      int numChildren = gtrie3.getUnchecked(gtriePos + 1);
      int mask = gtrie3.getUnchecked(gtriePos + 3);
      IntIntMap vertexToPattern = getAndSet(numVertices - 1);
      for (int i = 0; i < numChildren; ++i) {
         int cpattern = gtrie3.getUnchecked(gtriePos + 4 + i);
         int connectionPattern = cpattern >> mask;
         
         includes.clear();
         excludes.clear();

         for (int j = 0; j < numVertices; ++j) {
            if (checkBit(connectionPattern, j)) {
               includes.add(vertices.getUnchecked(j));
            } else {
               excludes.add(vertices.getUnchecked(j));
            }
         }

         includeConsumer.set(vertexToPattern, cpattern);

         // include
         for (int j = 0; j < includes.size(); ++j) {
            IntArrayList neighbors = getNeighbors(c, includes.getUnchecked(j));
            if (neighbors != null) {
               neighbors.forEach(includeConsumer);
            }
         }

         // exclude
         for (int j = 0; j < excludes.size(); ++j) {
            IntArrayList neighbors = getNeighbors(c, excludes.getUnchecked(j));
            if (neighbors != null) {
               neighbors.forEach(excludeConsumer);
            }
         }
      }

      return vertexToPattern.keySet();
   }

   private boolean checkBit(int word, int pos) {
      return ((word >> pos) & 0x00000001) == 0x00000001;
   }

   private IntArrayList getNeighbors(Computation c, int v) {
      VertexNeighbourhood neighbourhood = c.getConfig().getMainGraph().
         getVertexNeighbourhood(v);
      if (neighbourhood == null) {
         return null;
      } else {
         return neighbourhood.getOrderedVertices();
      }
   }

   private class IncludeConsumer implements IntConsumer {
      private IntIntMap extensions;
      private int pattern;

      public IncludeConsumer set(IntIntMap extensions, int pattern) {
         this.extensions = extensions;
         this.pattern = pattern;
         return this;
      }

      @Override
      public void accept(int v) {
         extensions.put(v, pattern);
      }
   }

   private class ExcludeConsumer implements IntConsumer {
      private IntIntMap extensions;
      private int pattern;

      public ExcludeConsumer set(IntIntMap extensions, int pattern) {
         this.extensions = extensions;
         this.pattern = pattern;
         return this;
      }

      @Override
      public void accept(int v) {
         extensions.remove(v);
      }
   }
   
}
