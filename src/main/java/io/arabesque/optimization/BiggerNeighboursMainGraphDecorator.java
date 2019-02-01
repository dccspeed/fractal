package io.arabesque.optimization;

import com.koloboke.collect.IntCursor;

import io.arabesque.graph.*;
import io.arabesque.utils.collection.IntArrayList;

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
