package io.arabesque.optimization;

import com.koloboke.collect.IntCollection;

import io.arabesque.computation.Computation;
import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.graph.VertexNeighbourhood;
import io.arabesque.utils.collection.IntArrayList;

import java.util.Arrays;

public class CliqueVertexInducedEmbedding extends VertexInducedEmbedding {
    @Override
    protected void updateExtensibleWordIdsSimple(Computation computation) {
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
               int fromIdx = orderedVertices.binarySearch(lastVertex);
               fromIdx = (fromIdx < 0) ? (-fromIdx - 1) : fromIdx;
               for (int j = fromIdx; j < numOrderedVertices; ++j) {
                  extensionWordIds().add(orderedVertices.getUnchecked(j));
               }
            }
        }
    }
}
