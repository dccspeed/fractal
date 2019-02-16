package io.arabesque.optimization;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.IntCursor;

import io.arabesque.computation.Computation;
import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.graph.VertexNeighbourhood;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.pool.IntArrayListPool;
import io.arabesque.utils.Utils;

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
