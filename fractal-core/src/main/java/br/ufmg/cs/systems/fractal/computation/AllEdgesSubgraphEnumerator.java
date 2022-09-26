package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.subgraph.EdgeInducedSubgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayListView;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import org.apache.log4j.Logger;

public class AllEdgesSubgraphEnumerator extends SubgraphEnumerator<EdgeInducedSubgraph> {
   protected static final Logger LOG =
           Logger.getLogger(AllEdgesSubgraphEnumerator.class);

   private IntArrayListView neighbors;
   private IntSet extensionSet;

   @Override
   public void init(Configuration config,
                    Computation<EdgeInducedSubgraph> computation) {
      neighbors = new IntArrayListView();
      extensionSet = HashIntSets.newMutableSet();
   }

   @Override
   public void computeExtensions_EXTENSION_PRIMITIVE() {
      MainGraph graph = subgraph.getMainGraph();
      IntArrayList vertices = subgraph.getVertices();
      IntArrayList edges = subgraph.getEdges();
      int numVertices = vertices.size();
      int numEdges = edges.size();

      extensionSet.clear();
      for (int i = 0; i < numVertices; ++i) {
         int u = vertices.getu(i);
         graph.neighborhoodEdges(u, neighbors);
         for (int j = 0; j < neighbors.size(); ++j) {
            extensionSet.add(neighbors.getu(j));
         }
      }

      for (int i = 0; i < numEdges; ++i) {
         extensionSet.removeInt(edges.getu(i));
      }

      extensions.setFrom(extensionSet);
      newExtensions(extensions);
   }
}
