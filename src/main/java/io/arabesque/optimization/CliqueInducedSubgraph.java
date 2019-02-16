package io.arabesque.optimization;

import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.IntObjCursor;
import com.koloboke.collect.set.hash.HashIntSet;
import io.arabesque.utils.pool.HashIntSetPool;

import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.graph.MainGraph;
import io.arabesque.graph.VertexNeighbourhood;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.pool.IntArrayListPool;
import io.arabesque.utils.Utils;

public class CliqueInducedSubgraph {

   protected HashIntObjMap<IntArrayList> adjLists;

   public CliqueInducedSubgraph() {
      adjLists = HashIntObjMaps.newMutableMap();
   }

   public CliqueInducedSubgraph copy() {
      CliqueInducedSubgraph sg = CliqueInducedSubgraphPool.instance().createObject();
      
      IntObjCursor<IntArrayList> cur = adjLists.cursor();
      while (cur.moveNext()) {
         IntArrayList neighbors = IntArrayListPool.instance().createObject();
         neighbors.addAll(cur.value());
         sg.adjLists.put(cur.key(), neighbors);
      }

      return sg;
   }

   public HashIntObjMap<IntArrayList> getAdjList() {
      return adjLists;
   }

   public void clear() {
      if (adjLists == null) return;

      IntObjCursor<IntArrayList> cur = adjLists.cursor();
      while (cur.moveNext()) {
         IntArrayListPool.instance().reclaimObject(cur.value());
         cur.remove();
      }
   }

   public static CliqueInducedSubgraph bootstrap(Configuration config, int u) {
      CliqueInducedSubgraph sg = CliqueInducedSubgraphPool.instance().createObject();
      //CliqueInducedSubgraph sg = new CliqueInducedSubgraph();
      VertexNeighbourhood neighborhood = config.getMainGraph().
         getVertexNeighbourhood(u);

      if (neighborhood == null) {
         sg.adjLists = HashIntObjMaps.newMutableMap();
         return sg;
      }

      IntArrayList orderedVertices = neighborhood.getOrderedVertices();

      //sg.adjLists.clear();
      sg.adjLists.ensureCapacity(orderedVertices.size());

      for (int i = 0; i < orderedVertices.size(); ++i) {
         int v = orderedVertices.getUnchecked(i);
         if (v > u) {
            sg.adjLists.put(v, IntArrayListPool.instance().createObject());
         }
      }
      
      IntObjCursor<IntArrayList> cur = sg.adjLists.cursor();
      while (cur.moveNext()) {
         int v = cur.key();
         neighborhood = config.getMainGraph().getVertexNeighbourhood(v);

         if (neighborhood == null) continue;

         IntArrayList orderedVertices2 = neighborhood.getOrderedVertices();

         for (int j = 0; j < orderedVertices2.size(); ++j) {
            int w = orderedVertices2.getUnchecked(j);
            if (w > v && sg.adjLists.containsKey(w)) {
               cur.value().add(w);
            }
         }
      }

      return sg;
   }

   public CliqueInducedSubgraph extend(Embedding embedding, int u) {
      CliqueInducedSubgraph sg = CliqueInducedSubgraphPool.instance().createObject();
      //CliqueInducedSubgraph sg = new CliqueInducedSubgraph();
      IntArrayList orderedVertices = adjLists.get(u);

      if (orderedVertices == null) {
         sg.adjLists = HashIntObjMaps.newMutableMap();
         return sg;
      }
      
      //sg.adjLists.clear();
      sg.adjLists.ensureCapacity(orderedVertices.size());
      
      for (int i = 0; i < orderedVertices.size(); ++i) {
         int v = orderedVertices.getUnchecked(i);
         IntArrayList orderedVertices2 = adjLists.get(v);
         IntArrayList target = IntArrayListPool.instance().createObject();
         Utils.sintersect(orderedVertices, orderedVertices2,
               i + 1, orderedVertices.size(), 0, orderedVertices2.size(), target);
         sg.adjLists.put(v, target);
      }


      //for (int i = 0; i < orderedVertices.size(); ++i) {
      //   int v = orderedVertices.getUnchecked(i);
      //   sg.adjLists.put(v, IntArrayListPool.instance().createObject());
      //}

      //for (int i = 0; i < orderedVertices.size(); ++i) {
      //   int v = orderedVertices.getUnchecked(i);
      //   IntArrayList orderedVertices2 = adjLists.get(v);

      //   if (orderedVertices2 == null) continue;

      //   for (int j = 0; j < orderedVertices2.size(); ++j) {
      //      int w = orderedVertices2.getUnchecked(j);
      //      if (sg.adjLists.containsKey(w)) {
      //         sg.adjLists.get(v).add(w);
      //      }
      //   }

      //}

      return sg;

   }
}
