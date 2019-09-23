package br.ufmg.cs.systems.fractal.optimization;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.graph.VertexNeighbourhood;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;

public class CliqueVertexInducedSubgraph extends VertexInducedSubgraph {
   @Override
   protected void updateExtensions(Computation computation) {
      if (dirtyExtensionWordIds) {
         extensionWordIds().clear();

         int numVertices = getNumVertices();
         IntArrayList vertices = getVertices();
         int lastVertex = vertices.getLast();

         VertexNeighbourhood neighbourhood = configuration.getMainGraph().
            getVertexNeighbourhood(lastVertex);

         if (neighbourhood != null) {
            IntArrayList orderedVertices = neighbourhood.getOrderedVertices();
            int numOrderedVertices = orderedVertices.size();
            for (int j = numOrderedVertices - 1; j >= 0; --j) {
               int v = orderedVertices.getUnchecked(j);
               if (v > lastVertex) {
                  extensionWordIds().add(v);
               } else {
                  break;
               }
            }
         }
      }
   }
}
