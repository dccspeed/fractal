package br.ufmg.cs.systems.fractal.graph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.*;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayListView;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntIntMapPool;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.IntObjMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.koloboke.function.IntIntConsumer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

public class SmallVELabeledMainGraph implements MainGraph {
   protected static final Logger LOG =
           Logger.getLogger(SmallVELabeledMainGraph.class);

   private boolean initialized = false;

   protected int numEdges;
   protected int numVertices;

   protected IntObjMap<IntIntMap> adjListsEdges;
   protected IntObjMap<IntIntMap> adjLists;
   protected IntIntMap edgeToEdgeSrc;
   protected IntIntMap edgeToEdgeDst;

   protected IntIntMap vertexToVertexLabel;
   protected IntIntMap edgeToEdgeLabel;

   private final IntersectionConsumerFirst intersectionConsumerFirst =
           new IntersectionConsumerFirst();
   private final IntersectionPredicate intersectionPredicate =
           new IntersectionPredicate();

   private final Consumer<IntIntMap> intIntMapReclaimer =
           this::reclaimIntIntMap;

   private final ObjArrayList<IntIntMap> localMapPool = new ObjArrayList<>();

   private static final int MAX_POOL_SIZE = 50;

   public SmallVELabeledMainGraph() {
   }

   private IntIntMap getIntIntMap(int sizeHint) {
      IntIntMap map;
      if (!localMapPool.isEmpty()) {
         map = localMapPool.pop();
         map.clear();
         map.ensureCapacity(sizeHint);
      } else {
         map = HashIntIntMaps.newUpdatableMap(sizeHint);
      }

      return map;
   }

   private void reclaimIntIntMap(IntIntMap map) {
      if (localMapPool.size() < MAX_POOL_SIZE) {
         localMapPool.add(map);
      }
   }

   private final void addEdge(int u, int v, int e, boolean areVerticesValid) {
      IntIntMap uAdjListEdges = adjListsEdges.get(u);
      IntIntMap uAdjList = adjLists.get(u);
      IntIntMap vAdjListEdges = adjListsEdges.get(v);
      IntIntMap vAdjList = adjLists.get(v);

      uAdjListEdges.put(e, v);
      uAdjList.put(v, e);
      vAdjListEdges.put(e, u);
      vAdjList.put(u, e);

      if (u < v) {
         edgeToEdgeSrc.put(e, u);
         edgeToEdgeDst.put(e, v);
      } else {
         edgeToEdgeSrc.put(e, v);
         edgeToEdgeDst.put(e, u);
      }
   }

   private void addEdgeLabel(int e, int label) {
      edgeToEdgeLabel.put(e, label);
   }

   private void addVertex(int u) {
      adjListsEdges.put(u, getIntIntMap(10));
      adjLists.put(u, getIntIntMap(10));
   }

   private void addVertexLabel(int u, int label) {
      vertexToVertexLabel.put(u, label);
   }

   @Override
   public int edgeDst(int e) {
      return edgeToEdgeDst.get(e);
   }

   @Override
   public int firstEdgeLabel(int e) {
      return edgeToEdgeLabel.get(e);
   }

   @Override
   public int edgeSrc(int e) {
      return edgeToEdgeSrc.get(e);
   }

   public int edgeId(int u, int v) {
      return adjListsEdges.get(u).get(v);
   }

   @Override
   public void forEachCommonEdgeLabels(IntArrayList edges,
                                       IntConsumer consumer) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void forEachEdge(int u, int v, IntConsumer consumer) {
      IntIntMap edges = adjListsEdges.get(u);
      if (edges == null) {
         throw new RuntimeException("Adjacency list not found " + adjListsEdges);
      }

      edges.values().forEach(consumer);
   }

   @Override
   public void validExtensionsPatternInducedLabeled(Computation computation,
                                                    Subgraph subgraph,
                                                    IntArrayList intersectionVertexIdxs,
                                                    IntArrayList differenceVertexIdxs,
                                                    IntArrayList starts,
                                                    IntArrayList ends,
                                                    int vertexLowerBound,
                                                    int vertexUpperBound,
                                                    VertexPredicate vpred,
                                                    EdgePredicates epreds,
                                                    IntCollection result) {
      result.clear();
      IntArrayList vertices = subgraph.getVertices();

      // add first intersection set
      int u = vertices.getu(intersectionVertexIdxs.getu(0));
      EdgePredicate epred = epreds.getu(0);
      intersectionConsumerFirst.set(epred, vpred, vertexLowerBound,
              vertexUpperBound, result);
      IntIntMap uAdjList = adjLists.get(u);
      uAdjList.forEach(intersectionConsumerFirst);

      // ensure remaining intersections
      for (int i = 1; i < intersectionVertexIdxs.size(); ++i) {
         epred = epreds.getu(i);
         u = vertices.getu(intersectionVertexIdxs.get(i));
         uAdjList = adjLists.get(u);
         intersectionPredicate.set(epred, uAdjList);
         result.removeIf(intersectionPredicate);
      }

      // remove differences
      for (int i = 0; i < differenceVertexIdxs.size(); ++i) {
         u = vertices.getu(differenceVertexIdxs.getu(i));
         uAdjList = adjLists.get(u);
         result.removeAll(uAdjList.keySet());
      }
   }

   @Override
   public void validExtensionsPatternInduced(Computation computation,
                                             Subgraph subgraph,
                                             IntArrayList intersectionVertexIdxs,
                                             IntArrayList differenceVertexIdxs,
                                             IntArrayList starts,
                                             IntArrayList ends,
                                             int vertexLowerBound,
                                             int vertexUpperBound,
                                             IntCollection result) {
      validExtensionsPatternInducedLabeled(computation, subgraph,
              intersectionVertexIdxs, differenceVertexIdxs, starts, ends,
              vertexLowerBound, vertexUpperBound,
              VertexPredicate.trueVertexPredicate,
              EdgePredicates.trueEdgePredicates, result);
   }

   @Override
   public void validExtensionsEdgeInduced(Computation computation,
                                          Subgraph subgraph,
                                          IntCollection validExtensions) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void validExtensionsVertexInduced(Computation computation,
                                            Subgraph subgraph,
                                            IntCollection validExtensions) {
      throw new UnsupportedOperationException();
   }

   @Override
   public IntArrayListView neighborhoodVertices(int u) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void neighborhoodVertices(int u, IntArrayListView view) {
      throw new UnsupportedOperationException();
   }

   @Override
   public IntArrayListView neighborhoodEdges(int u) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void neighborhoodEdges(int u, IntArrayListView view) {
      throw new UnsupportedOperationException();
   }

   @Override
   public int numEdges() {
      return numEdges;
   }

   @Override
   public int numVertices() {
      return numVertices;
   }

   @Override
   public int firstVertexLabel(int u) {
      return vertexToVertexLabel.get(u);
   }

   private void forEachEdgeId(int u, int v, int startIdx, int endIdx,
                             IntConsumer consumer) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final void init(Configuration configuration) throws IOException {
   }

   public final void init(Subgraph subgraph) {
      IntArrayList vertices = subgraph.getVertices();
      int numVertices = vertices.size();
      IntArrayList edges = subgraph.getEdges();
      int numEdges = edges.size();
      this.numVertices = numVertices;
      this.numEdges = numEdges;
      MainGraph underlyingGraph = subgraph.getMainGraph();

      // maybe init structures
      if (!initialized) {
         adjListsEdges = HashIntObjMaps.newUpdatableMap(numVertices);
         adjLists = HashIntObjMaps.newUpdatableMap(numVertices);
         edgeToEdgeSrc = getIntIntMap(numEdges);
         edgeToEdgeDst = getIntIntMap(numEdges);
         vertexToVertexLabel = getIntIntMap(numVertices);
         edgeToEdgeLabel = getIntIntMap(numEdges);
         initialized = true;
      } else { // reclaim
         adjListsEdges.values().forEach(intIntMapReclaimer);
         adjListsEdges.clear();
         adjListsEdges.ensureCapacity(numVertices);
         adjLists.values().forEach(intIntMapReclaimer);
         adjLists.clear();
         adjLists.ensureCapacity(numVertices);
         edgeToEdgeSrc.ensureCapacity(numEdges);
         edgeToEdgeDst.ensureCapacity(numEdges);
         vertexToVertexLabel.ensureCapacity(numVertices);
         edgeToEdgeLabel.ensureCapacity(numEdges);
      }

      for (int i = 0; i < numVertices; ++i) {
         int u = vertices.getu(i);
         int ulabel = underlyingGraph.firstVertexLabel(u);
         addVertex(u);
         addVertexLabel(u, ulabel);
      }

      for (int i = 0; i < numEdges; ++i) {
         int e = edges.getu(i);
         int elabel = underlyingGraph.firstEdgeLabel(e);
         int u = underlyingGraph.edgeSrc(e);
         int v = underlyingGraph.edgeDst(e);
         addEdge(u, v, e, true);
         addEdgeLabel(e, elabel);
      }
   }

   @Override
   public final synchronized boolean isEdgeValid(int e) {
      return true;
   }

   @Override
   public final synchronized boolean isVertexValid(int u) {
      return true;
   }

   @Override
   public final synchronized boolean isEdgeValid(int u, int v, int e) {
      return true;
   }

   @Override
   public int vertexDegree(int u) {
      return adjLists.get(u).size();
   }

   private class IntersectionPredicate implements IntPredicate {
      private EdgePredicate epred;
      private IntIntMap newAdjList; // vertex -> edge

      public void set(EdgePredicate epred, IntIntMap newAdjList) {
         this.epred = epred;
         this.newAdjList = newAdjList;
      }

      @Override
      public boolean test(int u) { // remove if ...
         return !newAdjList.containsKey(u) || !epred.test(newAdjList.get(u));
      }
   }

   private class IntersectionConsumerFirst implements IntIntConsumer {

      private EdgePredicate epred;
      private VertexPredicate vpred;
      private IntCollection intersection;

      private int lowerBound;
      private int upperBound;

      public void set(EdgePredicate epred, VertexPredicate vpred,
                      int lowerBound, int upperBound,
                      IntCollection intersection) {
         this.epred = epred;
         this.vpred = vpred;
         this.lowerBound = lowerBound;
         this.upperBound = upperBound;
         this.intersection = intersection;
      }

      @Override
      public void accept(int v, int e) {
         if (v > lowerBound && v < upperBound && vpred.test(v) && epred.test(e)) {
            intersection.add(v);
         }
      }
   }
}
