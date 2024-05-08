package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayListView;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import org.apache.log4j.Logger;

import java.util.function.IntConsumer;

/*
 * Get k-hop vertex induced egonets
 */
public class EgoNetEnumeratorVertexInduced extends SubgraphEnumerator<VertexInducedSubgraph> {
   protected static final Logger LOG =
           Logger.getLogger(EgoNetEnumeratorVertexInduced.class);

   private static final int MAX_HOP = 10;
   private IntArrayListView neighbors;
   private IntSet hopExtensions;
   private IntArrayList hopExtensionsSizes;
   private final VertexAdderConsumer vertexAdderConsumer = new VertexAdderConsumer();

   @Override
   public void init(Configuration config, Computation<VertexInducedSubgraph> computation) {
      neighbors = new IntArrayListView();
      hopExtensions = HashIntSets.newMutableSet();
   }

   private void ensureState() {
      if (hopExtensionsSizes != null) return;
      IntArrayList hopExtensionsSizes = new IntArrayList();
      hopExtensionsSizes.add(1);
      this.hopExtensionsSizes = hopExtensionsSizes;
      SubgraphEnumerator senum = this;
      while (senum.nextEnumerator() != null) {
         senum = senum.nextEnumerator();
         if (senum instanceof EgoNetEnumeratorVertexInduced) {
            EgoNetEnumeratorVertexInduced enenum = (EgoNetEnumeratorVertexInduced) senum;
            if (enenum.hopExtensionsSizes == null) {
               enenum.hopExtensionsSizes = hopExtensionsSizes;
            }
         }
      }
   }

   @Override
   public boolean extend_EXTENSION_PRIMITIVE() {
      if (prefixSize == 0) return super.extend_EXTENSION_PRIMITIVE();

      ensureState();

      int eidx = extensionsIdx.getAndIncrement();
      if (eidx < extensionsSize) {
         int hopExtensionsSize = extensions.get(eidx);
         if (hopExtensionsSize != hopExtensions.size()) {
            throw new RuntimeException("invalid extension");
         }

         hopExtensionsSizes.add(hopExtensionsSizes.getLast() + hopExtensionsSize);


         hopExtensions.forEach(vertexAdderConsumer);

         return true;
      }

      hopExtensionsSizes.removeLast();
      for (int i = 0; i < hopExtensions.size(); ++i) subgraph.removeLastWord();

      return false;
   }

   @Override
   public synchronized void computeExtensions_EXTENSION_PRIMITIVE() {
      ensureState();
      MainGraph graph = subgraph.getMainGraph();
      IntArrayList vertices = subgraph.getVertices();
      int numVertices = vertices.size();

      int from = hopExtensionsSizes.size() == 1 ? 0 : hopExtensionsSizes.get(hopExtensionsSizes.size() - 2);
      int to = hopExtensionsSizes.getLast();

      // compute hop extensions
      hopExtensions.clear();
      for (int i = from; i < to; ++i) {
         int u = vertices.getu(i);
         graph.neighborhoodVertices(u, neighbors);
         for (int j = 0; j < neighbors.size(); ++j) {
            int v = neighbors.getu(j);
            hopExtensions.add(v);
         }
      }

      // remove extensions already in subgraph
      for (int i = 0; i < numVertices; ++i) {
         hopExtensions.removeInt(vertices.getu(i));
      }

      // make hopExtensions idx the only placeholder extension
      extensions.clear();
      extensions.add(hopExtensions.size());
      newExtensions(extensions);
   }

   private class VertexAdderConsumer implements IntConsumer {

      @Override
      public void accept(int u) {
         EgoNetEnumeratorVertexInduced.this.subgraph.addWord(u);
      }
   }
}
