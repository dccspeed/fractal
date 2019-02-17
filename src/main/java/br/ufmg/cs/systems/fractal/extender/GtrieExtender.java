package br.ufmg.cs.systems.fractal.extender;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.graph.VertexNeighbourhood;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntIntMapPool;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.function.IntConsumer;
import com.koloboke.function.IntIntPredicate;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public abstract class GtrieExtender extends Extender {

   // <pattern> <id> <depth> <mask> <parentpos> <nchildren> <childrenpos ...>
   protected IntArrayList gtrie;
   
   public abstract boolean testSb(Subgraph e, int pattern, int v);

   // current position in gtrie
   protected int gtriePos;

   // mapping from vertex id to pattern, per enumeration depth
   protected ObjArrayList<IntIntMap> vertexToPatternPerDepth;

   protected IntArrayList includes;
   protected IntArrayList excludes;

   protected transient IncludeConsumer includeConsumer;
   protected transient IntersectPredicate intersectPredicate;
   protected transient ExcludeConsumer excludeConsumer;
   protected transient NeighborsConsumer neighborsConsumer;
   protected transient NeighborsPredicate neighborsPredicate;

   public static GtrieExtender create(int size) {
      if (size == 3) {
         return new GtrieExtender3();
      } else if (size == 4) {
         return new GtrieExtender4();
      } else {
         throw new RuntimeException("Gtrie with size " + size +
               " is not supported");
      }
   }

   public int id(Subgraph e) {
      return id(e, gtriePos);
   }
   
   public int id(Subgraph e, int pos) {
      return gtrie.get(pos + 1);
   }

   public int depth(Subgraph e) {
      return gtrie.get(gtriePos + 2);
   }

   public int mask(Subgraph e) {
      return mask(e, gtriePos);
   }

   public int mask(Subgraph e, int pos) {
      return gtrie.get(pos + 3);
   }
   
   public int parentPos(Subgraph e) {
      return gtrie.get(gtriePos + 4);
   }

   public int parent(Subgraph e) {
      return gtrie.get(parentPos(e));
   }

   public int nchildren(Subgraph e) {
      return gtrie.get(gtriePos + 5);
   }
   
   public int childPos(Subgraph e, int i) {
      if (i >= nchildren(e)) {
         throw new RuntimeException("Child out of bounds");
      }
      return gtrie.get(gtriePos + 6 + i);
   }

   public int child(Subgraph e, int i) {
      return gtrie.get(childPos(e, i));
   }

   public void bootstrap(Subgraph e) {
      if (e.getNumVertices() == 0) return;

      MainGraph graph = e.getConfig().getMainGraph();
      IntArrayList vertices = e.getVertices();
      int numVertices = vertices.size();

      int pattern = 0;
      int mask = 1;
      gtriePos = 0;

      for (int size = 1; size < numVertices; ++size) {
         int v = vertices.get(size);
         for (int i = 0; i < size; ++i) {
            int u = vertices.get(i);
            if (graph.isNeighborVertex(v, u)) {
               pattern = pattern | (0x00000001 << mask);
            }
            mask += 1;
         }
         
         int nchildren = nchildren(e);
         for (int i = 0; i < nchildren; ++i) {
            if (child(e, i) == pattern) {
               gtriePos = childPos(e, i);
               break;
            }
         }

      }
   }
    
   @Override
   public void writeExternal(ObjectOutput objOutput) throws IOException {
      objOutput.writeInt(gtriePos);
      objOutput.writeInt(vertexToPatternPerDepth.size());
      for (int i = 0; i < vertexToPatternPerDepth.size(); ++i) {
         IntIntMap mapping = vertexToPatternPerDepth.get(i);
         objOutput.writeInt(mapping.size());
         IntIntCursor cur = mapping.cursor();
         while (cur.moveNext()) {
            objOutput.writeInt(cur.key());
            objOutput.writeInt(cur.value());
         }
      }
   }
    
   @Override
   public void readExternal(ObjectInput objInput) throws IOException, ClassNotFoundException {
      gtriePos = objInput.readInt();
      int size = objInput.readInt();
      vertexToPatternPerDepth.ensureCapacity(size);
      for (int i = 0; i < size; ++i) {
         IntIntMap mapping = IntIntMapPool.instance().createObject();
         vertexToPatternPerDepth.add(mapping);
         int msize = objInput.readInt();
         mapping.ensureCapacity(msize);
         for (int j = 0; j < msize; ++j) {
            mapping.put(objInput.readInt(), objInput.readInt());
         }
      }
   }

   public GtrieExtender() {
      this.gtriePos = 0;
      this.vertexToPatternPerDepth = new ObjArrayList<IntIntMap>();
      this.includes = new IntArrayList();
      this.excludes = new IntArrayList();
      this.includeConsumer = new IncludeConsumer();
      this.intersectPredicate = new IntersectPredicate();
      this.excludeConsumer = new ExcludeConsumer();
      this.neighborsConsumer = new NeighborsConsumer();
      this.neighborsPredicate = new NeighborsPredicate();
   }

   public boolean sanityCheck(MainGraph graph, Subgraph e, int pattern) {
      if (e.getNumVertices() == 0) {
         throw new RuntimeException("Not allowed");
      }

      if (e.getNumVertices() == 1) return pattern == 0;
      
      if (e.getNumVertices() == 2) return pattern == 2;

      int _pattern = pattern >> 2;

      IntArrayList vertices = e.getVertices();
      int numVertices = vertices.size();


      for (int size = 2; size < numVertices; ++size) {
         int u = vertices.get(size);
         for (int i = 0; i < size; ++i) {
            int v = vertices.get(i);
            boolean connected = (_pattern & 0x00000001) == 0x00000001;
            boolean areNeighbors = graph.isNeighborVertex(v, u);
            if (connected != areNeighbors) {
               return false;
            }
            _pattern = _pattern >> 1;
         }
      }

      return true;
   }

   public int pattern(Subgraph e) {
      ensurePattern(e);
     //bootstrap(e);
      int pattern = gtrie.get(gtriePos);
      //if (!sanityCheck(e.getConfig().getMainGraph(), e, pattern)) {
      //   throw new RuntimeException("WrongPattern " + e +
      //         " pattern=" + Integer.toBinaryString(pattern) +
      //         " realPattern=" + e.getPattern());
      //}
      return pattern;
   }

   public IntIntMap getAndSet(int depth) {
      IntIntMap vertexToPattern = null;
      if (depth < vertexToPatternPerDepth.size()) {
         vertexToPattern = vertexToPatternPerDepth.get(depth);
         vertexToPattern.clear();
      } else {
         while (vertexToPatternPerDepth.size() <= depth) {
            vertexToPattern = IntIntMapPool.instance().createObject();
            vertexToPatternPerDepth.add(vertexToPattern);
         }
      }
      return vertexToPattern;
   }

   private void ensurePattern(Subgraph e) {
      IntArrayList vertices = e.getVertices();
      int numVertices = vertices.size();
      int depth = depth(e);

      // pattern is longer than subgraph, traverse up
      while (depth + 1 >= numVertices && depth > 0) {
         //System.out.print("up " + e + " gtriePosFrom=" + gtriePos + "");
         gtriePos = parentPos(e);
         //System.out.println(" gtriePosTo=" + gtriePos);
         depth = depth(e);
      }

      // pattern is shorter than subgraph, traverse down
      while (depth + 1 < numVertices) {
         int v = vertices.get(depth + 1);
         IntIntMap vertexToPattern = vertexToPatternPerDepth.get(depth);
         if (vertexToPattern == null) {
            bootstrap(e);
            return;
         }
         int cpattern = vertexToPattern.get(v);
         if (cpattern == -1) {
            bootstrap(e);
            return;
         }
         int nchildren = nchildren(e);
         boolean switched = false;
         for (int i = 0; i < nchildren; ++i) {
            if (child(e, i) == cpattern) {
               //System.out.print("down " + e + " gtriePosFrom=" + gtriePos);
               gtriePos = childPos(e, i);
               switched = true;
               //System.out.println(" gtriePosTo=" + gtriePos);
               break;
            }
         }
         if (!switched) {
            throw new RuntimeException("PatternNotFound " + e +
                  " vertex=" + v +
                  " depth=" + depth +
                  " gtriePos=" + gtriePos +
                  " cpattern=" + Integer.toBinaryString(cpattern) +
                  " nchildren=" + nchildren +
                  " vertexToPattern=" + vertexToPatternPerDepth.get(depth));
         }
         depth = depth(e);
      }

      // remove extra extension mappings
      while (vertexToPatternPerDepth.size() > numVertices - 1) {
         IntIntMap removed = vertexToPatternPerDepth.remove(
               vertexToPatternPerDepth.size() - 1);
         IntIntMapPool.instance().reclaimObject(removed);
      }
   }
   
   @Override
   public IntCollection extend(Subgraph e, Computation c) {
      IntArrayList vertices = e.getVertices();
      int numVertices = vertices.size();

      if (numVertices == 0) {
         return e.getExtensibleWordIds(c);
      }

      MainGraph graph = c.getConfig().getMainGraph();

      // extract gtrie info
      int pattern = pattern(e);
      int depth = depth(e);
      int numChildren = nchildren(e);
      int mask = mask(e);

      // final extension mapping (vertex -> pattern)
      IntIntMap vertexToPattern = getAndSet(numVertices - 1);

      //HashIntSet cpatterns = HashIntSetPool.instance().createObject();

      //for (int i = 0; i < numChildren; ++i) {
      //   cpatterns.add(child(e, i));
      //}

      //int cmask = mask(e, childPos(e, 0));

      //for (int i = 0; i < numVertices; ++i) {
      //   int v = vertices.get(i);
      //   IntArrayList neighbors = getNeighbors(c, v);
      //   if (neighbors != null) {
      //      neighborsConsumer.set(vertexToPattern,
      //            (0x00000001 << (i + cmask)) | pattern);
      //      neighbors.forEach(neighborsConsumer);
      //   }
      //}
      //
      //for (int i = 0; i < numVertices; ++i) {
      //   vertexToPattern.remove(vertices.get(i));
      //}


      //neighborsPredicate.set(cpatterns, e);
      //vertexToPattern.removeIf(neighborsPredicate);

      //IntIntMap tmp = IntIntMapPool.instance().createObject();
      for (int i = 0; i < numChildren; ++i) {
         int cpattern = child(e, i);
         int cpos = childPos(e, i);
         int cmask = mask(e, cpos);
         int connectionPattern = cpattern >> cmask;
         
         includes.clear();
         excludes.clear();

         for (int j = 0; j < numVertices; ++j) {
            if (checkBit(connectionPattern, j)) {
               includes.add(vertices.get(j));
            } else {
               excludes.add(vertices.get(j));
            }
         }

         //System.out.printf("Extend %s mask=%d cpattern=%s connectionPattern=%s" +
         //      " includes=%s excludes=%s\n",
         //      e.toString(),
         //      mask,
         //      Integer.toBinaryString(cpattern),
         //      Integer.toBinaryString(connectionPattern),
         //      includes.toString(), excludes.toString());

         // include first neighborhood
         includeConsumer.set(vertexToPattern, cpattern, id(e, cpos), e);
         IntArrayList neighbors = getNeighbors(c, includes.get(0));
         if (neighbors != null) {
            neighbors.forEach(includeConsumer);
         }

         excludeConsumer.set(vertexToPattern, cpattern);
         // exclude neighborhoods
         for (int j = 0; j < excludes.size(); ++j) {
            neighbors = getNeighbors(c, excludes.get(j));
            if (neighbors != null) {
               neighbors.forEach(excludeConsumer);
            }
         }

         // intersect neighborhoods
         for (int j = 1; j < includes.size(); ++j) {
            int vertex = includes.get(j);
            intersectPredicate.set(graph, vertex, cpattern);
            vertexToPattern.removeIf(intersectPredicate);
         }

         //vertexToPattern.putAll(tmp);
         //tmp.clear();
      }

      for (int i = 0; i < numVertices; ++i) {
         vertexToPattern.remove(vertices.get(i));
      }

      //IntIntMapPool.instance().reclaimObject(tmp);

      //System.out.println("Extensions " + e + " " + vertexToPattern);

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
      private int pos;
      private int id;
      private Subgraph subgraph;
   
      public IncludeConsumer set(IntIntMap extensions, int pattern, int id, Subgraph subgraph) {
         this.extensions = extensions;
         this.pattern = pattern;
         this.id = id;
         this.subgraph = subgraph;
         return this;
      }

      @Override
      public void accept(int v) {
         if (!extensions.containsKey(v) && testSb(subgraph, pattern, v)) {
            extensions.put(v, pattern);
         }
      }
   }

   private class IntersectPredicate implements IntIntPredicate {
      private MainGraph graph;
      private int vertex;
      private int pattern;

      public IntersectPredicate set(MainGraph graph, int vertex, int pattern) {
         this.graph = graph;
         this.vertex = vertex;
         this.pattern = pattern;
         return this;
      }

      @Override
      public boolean test(int v, int p) {
         return p == pattern && !graph.isNeighborVertex(vertex, v);
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
         if (extensions.get(v) == pattern) {
            extensions.remove(v);
         }
      }
   }

   private class NeighborsConsumer implements IntConsumer {
      private IntIntMap extensions;
      private int pattern;

      public NeighborsConsumer set(IntIntMap extensions, int pattern) {
         this.extensions = extensions;
         this.pattern = pattern;
         return this;
      }

      @Override
      public void accept(int v) {
         int cp = extensions.get(v);
         if (cp == -1) {
            extensions.put(v, pattern);
         } else {
            extensions.put(v, pattern | cp);
         }
      }
   }

   private class NeighborsPredicate implements IntIntPredicate {
      private HashIntSet patterns;
      private Subgraph subgraph;

      public NeighborsPredicate set(HashIntSet patterns, Subgraph subgraph) {
         this.patterns = patterns;
         this.subgraph = subgraph;
         return this;
      }

      @Override
      public boolean test(int v, int p) {
         return !patterns.contains(p) || !GtrieExtender.this.testSb(subgraph, p, v);
      }
   }
}
