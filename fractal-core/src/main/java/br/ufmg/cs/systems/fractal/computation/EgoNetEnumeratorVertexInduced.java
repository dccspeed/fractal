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
   private ObjArrayList<IntSet> extensionsByHop;
   private final VertexAdderConsumer vertexAdderConsumer = new VertexAdderConsumer();

   @Override
   public void init(Configuration config, Computation<VertexInducedSubgraph> computation) {
      neighbors = new IntArrayListView();
      extensionsByHop = new ObjArrayList<>(MAX_HOP);
   }

   @Override
   public boolean extend_EXTENSION_PRIMITIVE() {
      LOG.error("extend " + subgraph + " " + this);
      if (prefixSize == 0) return super.extend_EXTENSION_PRIMITIVE();

      int eidx = extensionsIdx.getAndIncrement();
      if (eidx < extensionsSize) {
         int hopExtensionsIdx = extensions.get(eidx);

         while (extensionsByHop.size() - 1 > hopExtensionsIdx) {
            int numExtensionsToRemove = extensionsByHop.getLast().size();
            extensionsByHop.removeLast();
            for (int i = 0; i < numExtensionsToRemove; ++i) subgraph.removeLastWord();
         }

         IntSet hopExtensions = extensionsByHop.getLast();
         hopExtensions.forEach(vertexAdderConsumer);

         LOG.error("extendAfter " + subgraph);

         return true;
      }
      return false;
   }

   @Override
   public synchronized void computeExtensions_EXTENSION_PRIMITIVE() {
      MainGraph graph = subgraph.getMainGraph();
      IntArrayList vertices = subgraph.getVertices();
      int numVertices = vertices.size();

      int hopExtensionsIdx = extensionsByHop.size();

      IntSet extensionSet = extensionsByHop.getu(hopExtensionsIdx);
      if (extensionSet == null) extensionSet = HashIntSets.newMutableSet();

      // compute hop extensions
      extensionSet.clear();
      for (int i = 0; i < numVertices; ++i) {
         int u = vertices.getu(i);
         graph.neighborhoodVertices(u, neighbors);
         LOG.error(u + " " + neighbors);
         for (int j = 0; j < neighbors.size(); ++j) {
            int v = neighbors.getu(j);
            extensionSet.add(v);
         }
      }

      // remove extensions already in subgraph
      for (int i = 0; i < numVertices; ++i) {
         extensionSet.removeInt(vertices.getu(i));
      }

      // add hop extensions to the pool of extensions
      extensionsByHop.add(extensionSet);

      // make hopExtensions idx the only placeholder extension
      extensions.clear();
      extensions.add(hopExtensionsIdx);
      newExtensions(extensions);

      LOG.error("compute " + subgraph + " extensions " + extensions + " hopExtensions " + extensionSet);
   }

   private class VertexAdderConsumer implements IntConsumer {

      @Override
      public void accept(int u) {
         EgoNetEnumeratorVertexInduced.this.subgraph.addWord(u);
      }
   }
}
