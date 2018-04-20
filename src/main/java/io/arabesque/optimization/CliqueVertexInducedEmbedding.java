package io.arabesque.optimization;

import com.koloboke.collect.IntCollection;

import io.arabesque.computation.Computation;
import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.graph.VertexNeighbourhood;
import io.arabesque.utils.collection.IntArrayList;

import java.util.Arrays;

public class CliqueVertexInducedEmbedding extends VertexInducedEmbedding {
    @Override
    public IntCollection getExtensibleWordIds(Computation computation) {
        if (dirtyExtensionWordIds) {
            extensionWordIds().clear();

            int numVertices = getNumVertices();
            IntArrayList vertices = getVertices();
            int lastVertex = vertices.getLast();

            VertexNeighbourhood neighbourhood = configuration.getMainGraph().
               getVertexNeighbourhood(lastVertex);

            if (neighbourhood != null) {
               int[] orderedVertices = neighbourhood.getOrderedVertices();
               int fromIdx = Arrays.binarySearch(orderedVertices, lastVertex);
               fromIdx = (fromIdx < 0) ? (-fromIdx - 1) : fromIdx;
               for (int j = fromIdx; j < orderedVertices.length; ++j) {
                  extensionWordIds.add(orderedVertices[j]);
               }
            }

        }

        return extensionWordIds();
    }
}
