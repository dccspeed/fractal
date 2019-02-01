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
   public IntCollection getExtensibleWordIds(Computation computation) {
      IntCollection extensions = null;
      if (dirtyExtensionWordIds) {
         if (getNumWords() > 0) {
            extensions = cliqueExtensions();
         } else {
            updateInitExtensibleWordIds(computation);
            extensions = extensionWordIds();
         }
      }

      return extensions;
   }

   private IntArrayList cliqueExtensions() {
      int numVertices = vertices.size();
      int lastVertex = vertices.getUnchecked(numVertices - 1);

      IntArrayList extensions = IntArrayListPool.instance().createObject();

      VertexNeighbourhood neighbourhood = configuration.getMainGraph().
         getVertexNeighbourhood(lastVertex);

      if (neighbourhood == null) {
         return extensions;
      }

      IntArrayList orderedVertices = neighbourhood.getOrderedVertices();
      int numOrderedVertices = orderedVertices.size();

      int i;
      for (i = 0; i < numOrderedVertices &&
            orderedVertices.getUnchecked(i) < lastVertex; ++i);

      if (numVertices == 1) {
         for (int j = i; j < numOrderedVertices; ++j) {
            extensions.add(orderedVertices.getUnchecked(j));
         }
      } else {
         IntArrayList prevExtensions = extensionArrays.
            getUnchecked(numVertices - 2);

         Utils.sintersect(orderedVertices, prevExtensions,
               i, numOrderedVertices, 0, prevExtensions.size(),
               extensions);
      }

      if (extensionArrays.size() >= numVertices) {
         IntArrayList old = extensionArrays.
            setUnchecked(numVertices - 1, extensions);
         IntArrayListPool.instance().reclaimObject(old);
      } else {
         extensionArrays.add(extensions);
      }

      return extensions;
   }

   //@Override
   //protected void updateExtensibleWordIdsSimple(Computation computation) {
   //   if (dirtyExtensionWordIds) {
   //      extensionWordIds().clear();

   //      int numVertices = getNumVertices();
   //      IntArrayList vertices = getVertices();
   //      int lastVertex = vertices.getLast();

   //      VertexNeighbourhood neighbourhood = configuration.getMainGraph().
   //         getVertexNeighbourhood(lastVertex);

   //      if (neighbourhood != null) {
   //         IntArrayList orderedVertices = neighbourhood.getOrderedVertices();
   //         int numOrderedVertices = orderedVertices.size();
   //         for (int j = numOrderedVertices - 1; j >= 0; --j) {
   //            int v = orderedVertices.getUnchecked(j);
   //            if (v > lastVertex) {
   //               extensionWordIds().add(v);
   //            } else {
   //               break;
   //            }
   //         }
   //      }
   //   }
   //}
}
