package io.arabesque.graph;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.function.IntConsumer;
import com.koloboke.function.IntIntConsumer;
import io.arabesque.utils.collection.AtomicBitSetArray;
import io.arabesque.utils.collection.ReclaimableIntCollection;
import io.arabesque.utils.pool.IntSingletonPool;
import io.arabesque.utils.collection.IntArrayList;

import java.util.Arrays;

public class BasicVertexNeighbourhood implements VertexNeighbourhood, java.io.Serializable {
   // Key = neighbour vertex id, Value = edge id that connects owner of neighbourhood with Key
   protected IntIntMap neighbourhoodMap;
   private IntIntMap removedNeighbourhoodMap;
   private IntArrayList orderedVertices;
   private IntArrayList orderedEdges;

   public BasicVertexNeighbourhood() {
      this.neighbourhoodMap = HashIntIntMaps.getDefaultFactory().withDefaultValue(-1).newMutableMap();
      this.removedNeighbourhoodMap = HashIntIntMaps.getDefaultFactory().withDefaultValue(-1).newMutableMap();
   }

   @Override
   public void buildSortedNeighborhood() {
      orderedVertices = new IntArrayList(neighbourhoodMap.keySet().toIntArray());
      orderedVertices.sort();
      orderedEdges = new IntArrayList(neighbourhoodMap.values().toIntArray());
      orderedEdges.sort();
   }

   @Override
   public IntArrayList getOrderedVertices() {
      return orderedVertices;
   }

   @Override
   public IntArrayList getOrderedEdges() {
      return orderedEdges;
   }

   @Override
   public void reset() {
      IntIntCursor cur = removedNeighbourhoodMap.cursor();
      while (cur.moveNext()) {
         neighbourhoodMap.put(cur.key(), cur.value());
         cur.remove();
      }
   }

   @Override
   public void removeVertex(int vertexId) {
      int edgeId = neighbourhoodMap.remove(vertexId);
      if (edgeId != neighbourhoodMap.defaultValue()) {
         removedNeighbourhoodMap.put(vertexId, edgeId);
      }
   }
   
   @Override
   public int applyTag(AtomicBitSetArray vtag, AtomicBitSetArray etag) {
      IntIntCursor cur = neighbourhoodMap.cursor();
      int numVertices = neighbourhoodMap.size();
      int removedEdges = 0;
      while (cur.moveNext()) {
         if (!vtag.contains(cur.key()) || !etag.contains(cur.value())) {
            removedNeighbourhoodMap.put(cur.key(), cur.value());
            //verticesBitmap.remove(cur.key());
            //edgesBitmap.remove(cur.value());
            cur.remove();
            --numVertices;
            ++removedEdges;
         }
      }

      if (numVertices != neighbourhoodMap.size()) {
         throw new RuntimeException("Tagging error. Expected: " +
                 numVertices + " Got: " + neighbourhoodMap.size());
      }

      buildSortedNeighborhood();

      return removedEdges;

   }

   @Override
   public int applyTagVertexes(AtomicBitSetArray tag) {
      IntIntCursor cur = neighbourhoodMap.cursor();
      int numVertices = neighbourhoodMap.size();
      int removedEdges = 0;
      while (cur.moveNext()) {
         if (!tag.contains(cur.key())) {
            removedNeighbourhoodMap.put(cur.key(), cur.value());
            cur.remove();
            --numVertices;
            ++removedEdges;
         }
      }

      if (numVertices != neighbourhoodMap.size()) {
         throw new RuntimeException("Tagging error. Expected: " +
                 numVertices + " Got: " + neighbourhoodMap.size());
      }

      buildSortedNeighborhood();

      return removedEdges;
   }

   @Override
   public int applyTagEdges(AtomicBitSetArray tag) {
      IntIntCursor cur = neighbourhoodMap.cursor();
      int numVertices = neighbourhoodMap.size();
      int removedWords = 0;
      while (cur.moveNext()) {
         if (!tag.contains(cur.value())) {
            removedNeighbourhoodMap.put(cur.key(), cur.value());
            cur.remove();
            --numVertices;
            ++removedWords;
         }
      }

      if (numVertices != neighbourhoodMap.size()) {
         throw new RuntimeException("Tagging error. Expected: " +
                 numVertices + " Got: " + neighbourhoodMap.size());
      }

      return neighbourhoodMap.size();
   }

   @Override
   public IntCollection getNeighborVertices() {
      return neighbourhoodMap.keySet();
   }

   @Override
   public IntCollection getNeighborEdges() {
      return neighbourhoodMap.values();
   }

   @Override
   public ReclaimableIntCollection getEdgesWithNeighbourVertex(int neighbourVertexId) {
      int edgeId = neighbourhoodMap.get(neighbourVertexId);

      if (edgeId >= 0) {
         return IntSingletonPool.instance().createObject(edgeId);
      } else {
         return null;
      }
   }

   @Override
   public void forEachEdgeId(int nId, IntConsumer intConsumer) {
      int edgeId = neighbourhoodMap.get(nId);

      if (edgeId >= 0) {
         intConsumer.accept(edgeId);
      }
   }

   @Override
   public int forEachVertexEdgeLowerBound(
         IntIntConsumer consumer, int lowerBound) {
      int numVertices = orderedVertices.size();
      int fromIdx = orderedVertices.binarySearch(lowerBound);
      fromIdx = (fromIdx < 0) ? (-fromIdx - 1) : fromIdx;
      for (int i = fromIdx; i < numVertices; ++i) {
         int vIdx = orderedVertices.getUnchecked(i);
         consumer.accept(vIdx, neighbourhoodMap.get(vIdx));
      }

      return (numVertices - fromIdx);
   }
   
   @Override
   public void forEachVertexEdge(IntIntConsumer consumer) {
      neighbourhoodMap.forEach(consumer);
   }

   @Override
   public boolean isNeighbourVertex(int vertexId) {
      return neighbourhoodMap.containsKey(vertexId);
   }

   @Override
   public void addEdge(int neighbourVertexId, int edgeId) {
      //if (neighbourhoodMap.containsKey(neighbourVertexId)) {
      //   throw new RuntimeException(
      //         "This edge already exists and this is not a multi-vertex neighbourhood.");
      //}
      neighbourhoodMap.put(neighbourVertexId, edgeId);
   }

   @Override
   public String toString() {
      return "BasicVertexNeighbourhood{" +
              "neighbourhoodMap=" + neighbourhoodMap +
              '}';
   }
}
