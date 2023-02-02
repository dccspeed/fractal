package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayListView;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import org.apache.log4j.Logger;

import java.util.Random;

/*
 * Visits a fraction of subgraphs uniformly at random.
 */
public class SupernodeEnumerator<S extends Subgraph> extends SubgraphEnumerator<S> {
   protected static final Logger LOG =
           Logger.getLogger(SupernodeEnumerator.class);

   private IntArrayListView neighbors;
   private IntSet extensionSet;
   private int supernodeSize = 3;

   @Override
   public void init(Configuration config, Computation<S> computation) {
      neighbors = new IntArrayListView();
      extensionSet = HashIntSets.newMutableSet();
   }

   @Override
   public void computeExtensions_EXTENSION_PRIMITIVE() {
      MainGraph graph = subgraph.getMainGraph();
      IntArrayList vertices = subgraph.getVertices();
      int numVertices = vertices.size();

      int lowerBound = Integer.MIN_VALUE;

      extensionSet.clear();
      for (int i = numVertices - 1; i >= 0; --i) {
         int u = vertices.getu(i);
         graph.neighborhoodVertices(u, neighbors);
         for (int j = 0; j < neighbors.size(); ++j) {
            int v = neighbors.getu(j);
            if (v > lowerBound) extensionSet.add(v);
            else extensionSet.removeInt(v);
         }

         if (i >= supernodeSize && u > lowerBound) {
            lowerBound = u;
         }
      }

      //for (int i = 0; i < numVertices; ++i) {
      //   int u = vertices.getu(i);
      //   graph.neighborhoodVertices(u, neighbors);
      //   for (int j = 0; j < neighbors.size(); ++j) {
      //      extensionSet.add(neighbors.getu(j));
      //   }
      //}

      for (int i = 0; i < numVertices; ++i) {
         extensionSet.removeInt(vertices.getu(i));
      }

      extensions.setFrom(extensionSet);
      newExtensions(extensions);
   }
}
