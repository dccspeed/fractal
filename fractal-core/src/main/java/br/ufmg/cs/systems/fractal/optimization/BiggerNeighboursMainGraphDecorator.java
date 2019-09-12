package br.ufmg.cs.systems.fractal.optimization;

import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.graph.VertexNeighbourhood;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;

public class BiggerNeighboursMainGraphDecorator extends OrderedNeighboursMainGraphDecorator {

    public BiggerNeighboursMainGraphDecorator(MainGraph underlyingMainGraph) {
        super(underlyingMainGraph);

        int removed = 0;

        for (int v  = 0; v < underlyingMainGraph.getNumberVertices(); ++v) {
           VertexNeighbourhood neighborhood = getVertexNeighbourhood(v);
           if (neighborhood == null) continue;

           IntArrayList neighbors = neighborhood.getOrderedVertices();

           for (int i = 0; i < neighbors.size(); ++i) {
              int u = neighbors.getUnchecked(i);
              if (u < v) {
                 neighborhood.removeVertex(u);
                 ++removed;
              } else {
                 break;
              }
           }

           neighborhood.buildSortedNeighborhood();
        }

        System.out.println("RemovedVertices " + removed);
    }
}
