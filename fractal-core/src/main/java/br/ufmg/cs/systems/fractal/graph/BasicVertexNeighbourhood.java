package br.ufmg.cs.systems.fractal.graph;

import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ReclaimableIntCollection;
import br.ufmg.cs.systems.fractal.util.pool.IntSingletonPool;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import java.util.function.IntConsumer;
import java.util.function.Predicate;

import com.koloboke.function.IntIntConsumer;

public class BasicVertexNeighbourhood implements VertexNeighbourhood, java.io.Serializable {
   // Key = neighbour vertex id, Value = edge id that connects owner of neighbourhood with Key
   protected IntIntMap neighbourhoodMap;
   protected IntIntMap removedNeighbourhoodMap;
   protected IntArrayList orderedVertices;
   protected IntArrayList orderedEdges;

   protected MainGraph graph;

   public BasicVertexNeighbourhood() {
      this.neighbourhoodMap = HashIntIntMaps.getDefaultFactory().withDefaultValue(-1).newMutableMap();
      this.removedNeighbourhoodMap = HashIntIntMaps.getDefaultFactory().withDefaultValue(-1).newMutableMap();
   }

   public BasicVertexNeighbourhood(MainGraph graph) {
      this();
      this.graph = graph;
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
   public int filter(AtomicBitSetArray vtag, AtomicBitSetArray etag) {
      IntIntCursor cur = neighbourhoodMap.cursor();
      int numVertices = neighbourhoodMap.size();
      int removedEdges = 0;
      while (cur.moveNext()) {
         if (!vtag.contains(cur.key()) || !etag.contains(cur.value())) {
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
   public int filter(Predicate<Vertex> vpred, Predicate<Edge> epred) {
      IntIntCursor cur = neighbourhoodMap.cursor();
      int numVertices = neighbourhoodMap.size();
      int removedEdges = 0;
      while (cur.moveNext()) {
         if (!vpred.test(graph.getVertex(cur.key())) ||
                 !epred.test(graph.getEdge(cur.value()))) {
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
   public int filterVertices(AtomicBitSetArray tag) {
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
   public int filterEdges(AtomicBitSetArray tag) {
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
