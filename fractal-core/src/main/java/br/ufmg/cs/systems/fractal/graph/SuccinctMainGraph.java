package br.ufmg.cs.systems.fractal.graph;

import br.ufmg.cs.systems.fractal.util.*;
import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ReclaimableIntCollection;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import com.koloboke.collect.IntCollection;
import com.koloboke.function.IntIntConsumer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

public class SuccinctMainGraph implements MainGraph {
   private static final Logger LOG = Logger.getLogger(SuccinctMainGraph.class);

   /* Succinct graph representation */
   protected int numEdges;
   protected int numVertices;
   protected IntArrayList vertexNeighborhoodIdx;
   protected IntArrayList vertexNeighborhoods; // by default, sorted
   protected IntArrayList edgeNeighborhoods; // by default, not sorted
   protected IntArrayList edgeSrcs;
   protected IntArrayList edgeDsts;

   /* Labels */
   protected IntArrayList vertexLabelsIdx;
   protected IntArrayList vertexLabels;
   protected IntArrayList edgeLabelsIdx;
   protected IntArrayList edgeLabels;

   /* Auxiliary */
   protected IntArrayListPool intArrayListPool;

   public SuccinctMainGraph() {
   }

   public SuccinctMainGraph(String name) {
      this(name, false, false);
   }

   public SuccinctMainGraph(String name, boolean isEdgeLabelled, boolean isMultiGraph) {
      init(name, isEdgeLabelled, isMultiGraph);
   }

   public SuccinctMainGraph(Path filePath, boolean isEdgeLabelled, boolean isMultiGraph)
           throws IOException {
      this(filePath.getFileName().toString(), isEdgeLabelled, isMultiGraph);
   }

   public SuccinctMainGraph(org.apache.hadoop.fs.Path hdfsPath, boolean isEdgeLabelled, boolean isMultiGraph)
           throws IOException {
      this(hdfsPath.getName(), isEdgeLabelled, isMultiGraph);
   }

   public void initProperties(Object path) throws IOException {

   }

   private void init(String name, boolean isEdgeLabelled, boolean isMultiGraph) {
   }

   public void init(Object path) throws IOException {
      intArrayListPool = IntArrayListPool.instance();
      if (path instanceof Path) {
         Path filePath = (Path) path;
         readFromFile(filePath);
      } else if (path instanceof org.apache.hadoop.fs.Path) {
         org.apache.hadoop.fs.Path hadoopPath = (org.apache.hadoop.fs.Path) path;
         readFromHdfs(hadoopPath);
      } else {
         throw new RuntimeException("Invalid path: " + path);
      }
   }

   protected void readFromHdfs(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
      FileSystem fs = FileSystem.get(new org.apache.hadoop.conf.Configuration());
      InputStream is = fs.open(hdfsPath);
      readFromInputStream(is);
      is.close();
   }

   protected void readFromFile(Path filePath) throws IOException {
      InputStream is = Files.newInputStream(filePath);
      readFromInputStream(is);
      is.close();
   }

   protected void readFromInputStream(InputStream is) {
      long start = System.currentTimeMillis();
      try {
         TextFileParser stream = new TextFileParser(is);
         int u, v, ulabel, e, elabel;
         boolean edgeHasLabel;

         numVertices = stream.nextInt();
         numEdges = stream.nextInt();

         vertexNeighborhoodIdx = new IntArrayList(numVertices + 1);
         vertexNeighborhoods = new IntArrayList(numEdges * 2);
         edgeNeighborhoods = new IntArrayList(numEdges * 2);
         edgeSrcs = new IntArrayList(numEdges);
         edgeDsts = new IntArrayList(numEdges);

         vertexLabelsIdx = new IntArrayList(numVertices + 1);
         vertexLabels = new IntArrayList(numVertices); // at least

         edgeLabelsIdx = new IntArrayList(numEdges + 1);
         edgeLabels = new IntArrayList(numEdges); // at least

         for (u = 0; u < numVertices; ++u) {
            addVertex(u);

            // read labels of vertex u
            do {
               ulabel = stream.nextInt();
               addVertexLabel(u, ulabel);
            } while (stream.read() == ',');

            while (!stream.skipBlank()) {
               // read neighbor v
               v = stream.nextInt();
               // read edge id of neighbor v
               if (stream.read() != ',') {
                  throw new RuntimeException("Invalid format, expecting edge id after neighbor id");
               }
               e = stream.nextInt();
               addEdge(u, v, e);

               // read labels of edge (u,v)
               edgeHasLabel = false;
               while (stream.read() == ',') {
                  elabel = stream.nextInt();
                  if (u < v) {
                     addEdgeLabel(e, elabel);
                  }
                  edgeHasLabel = true;
               }

               if (u < v && !edgeHasLabel) {
                  addEdgeLabel(e, 1);
               }
            }
         }

         // for convenience
         vertexNeighborhoodIdx.add(vertexNeighborhoods.size());
         vertexLabelsIdx.add(vertexLabels.size());
         edgeLabelsIdx.add(edgeLabels.size());

      } catch (IOException e) {
         throw new RuntimeException(e);
      }

      System.out.println("vertexNeighborhoodIdx " + vertexNeighborhoodIdx.size());
      System.out.println("vertexNeighborhoods " + vertexNeighborhoods.size());
      System.out.println("edgeNeighborhoods " + edgeNeighborhoods.size());
      System.out.println("edgeSrcs " + edgeSrcs.size());
      System.out.println("edgeDsts " + edgeDsts.size());

      System.out.println("vertexLabelsIdx " + vertexLabelsIdx.size());
      System.out.println("vertexLabels " + vertexLabels.size());
      System.out.println("edgeLabelsIdx " + edgeLabelsIdx.size());
      System.out.println("edgeLabels " + edgeLabels.size());

      long elapsed = System.currentTimeMillis() - start;
      System.out.println("GraphReading took " + elapsed + " ms");

      //start = System.currentTimeMillis();
      //for (int e = 0; e < edgeSrcs.size(); ++e) {
      //   int u = edgeSrc(e);
      //   int v = edgeDst(e);
      //   int edgeId1 = edgeId(u, v);
      //   int edgeId2 = edgeId(v, u);
      //   if (edgeId1 != edgeId2) {
      //      throw new RuntimeException("(u,v) != (v,u)");
      //   }
      //}
      //elapsed = System.currentTimeMillis() - start;
      //System.out.println("NeighborhoodArraysLookup " + elapsed + " ms");
   }

   public void addVertex(int u) {
      vertexNeighborhoodIdx.add(vertexNeighborhoods.size());
   }

   public void addVertexLabel(int u, int label) {
      vertexLabelsIdx.add(vertexLabels.size());
      vertexLabels.add(label);
   }

   public void addEdge(int u, int v, int e) {
      vertexNeighborhoods.add(v);
      edgeNeighborhoods.add(e);
      if (u < v) {
         edgeSrcs.add(u);
         edgeDsts.add(v);
      }
   }

   public void addEdgeLabel(int e, int label) {
      edgeLabelsIdx.add(edgeLabels.size());
      edgeLabels.add(label);
   }

   @Override
   public int edgeSrc(int e) {
      return edgeSrcs.get(e);
   }

   @Override
   public int edgeDst(int e) {
      return edgeDsts.get(e);
   }

   public int edgeId(int u, int v) {
      return edgeNeighborhoods.getUnchecked(vertexNeighborhoods.binarySearch(v, vertexNeighborhoodIdx.getUnchecked(u), vertexNeighborhoodIdx.getUnchecked(u+1)));
   }

   @Override
   public int getId() {
      return 0;
   }

   @Override
   public void setId(int id) {

   }

   @Override
   public void reset() {

   }

   @Override
   public boolean isNeighborVertex(int v1, int v2) {
      throw new UnsupportedOperationException();
   }

   @Override
   public MainGraph addVertex(Vertex vertex) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Vertex[] getVertices() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Vertex getVertex(int vertexId) {
      throw new UnsupportedOperationException();
   }

   @Override
   public int getNumberVertices() {
      return numVertices;
   }

   @Override
   public Edge[] getEdges() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Edge getEdge(int edgeId) {
      throw new UnsupportedOperationException();
   }

   @Override
   public int getNumberEdges() {
      return numEdges;
   }

   @Override
   public ReclaimableIntCollection getEdgeIds(int v1, int v2) {
      throw new UnsupportedOperationException();
   }

   @Override
   public MainGraph addEdge(Edge edge) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean areEdgesNeighbors(int edge1Id, int edge2Id) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isNeighborEdge(int src1, int dest1, int edge2) {
      throw new UnsupportedOperationException();
   }

   @Override
   public VertexNeighbourhood getVertexNeighbourhood(int vertexId) {
      throw new UnsupportedOperationException();
   }

   @Override
   public IntCollection getVertexNeighbours(int vertexId) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isEdgeLabelled() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isMultiGraph() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void forEachEdgeId(int u, int v, IntConsumer consumer) {
      int startIdx = vertexNeighborhoodIdx.getUnchecked(u);
      int endIdx = vertexNeighborhoodIdx.getUnchecked(u+1);
      forEachEdgeId(u, v, startIdx, endIdx, consumer);
   }

   public void forEachEdgeId(int u, int v, int startIdx, int endIdx, IntConsumer consumer) {
      int idx = vertexNeighborhoods.binarySearch(v, startIdx, endIdx);
      int nedges = 0;

      if (idx < startIdx || idx >= endIdx) return;

      // accept first edge (u,v) found
      consumer.accept(edgeNeighborhoods.getUnchecked(idx));
      nedges++;

      // accept all edges (u,v) rightwards
      for (int i = idx - 1; i >= startIdx && vertexNeighborhoods.getUnchecked(i) == v; --i) {
         consumer.accept(edgeNeighborhoods.getUnchecked(i));
         nedges++;
      }

      // accept all edges (u,v) leftwards
      for (int i = idx + 1; i < endIdx && vertexNeighborhoods.getUnchecked(i) == v; ++i) {
         consumer.accept(edgeNeighborhoods.getUnchecked(i));
         nedges++;
      }
   }

   @Override
   public int filterVertices(AtomicBitSetArray tag) {
      return 0;
   }

   @Override
   public int filterEdges(AtomicBitSetArray tag) {
      return 0;
   }

   @Override
   public int undoVertexFilter() {
      return 0;
   }

   @Override
   public int undoEdgeFilter() {
      return 0;
   }

   @Override
   public int filter(AtomicBitSetArray vtag, AtomicBitSetArray etag) {
      return 0;
   }

   @Override
   public void buildSortedNeighborhood() {

   }

   @Override
   public boolean containsVertex(int u) {
      return u < numVertices;
   }

   @Override
   public boolean containsEdge(int e) {
      return e < numEdges;
   }

   @Override
   public int filterEdges(Predicate epred) {
      return 0;
   }

   @Override
   public int filterVertices(Predicate vpred) {
      return 0;
   }

   @Override
   public void neighborhoodTraversal(IntArrayList intersection, IntArrayList difference, int vertexLowerBound,
                                     IntConsumer consumer, IntPredicate vertexPredicate, EdgePredicates edgePredicates) {
      if (intersection.size() == 0) return;

      /* Declarations */
      IntArrayList validVertices = null;
      IntArrayList resultSet = null;
      EdgePredicate edgePredicate = null;
      int u, v, e, idx, size;
      int intersectionSize = intersection.size(), differenceSize = difference.size();

      if (intersectionSize == 1 && differenceSize == 0) {
         /* Initialize validVertices with the first intersection and considering vertexPredicate */
         u = intersection.getUnchecked(0);
         idx = vertexNeighborhoodIdx.getUnchecked(u);
         size = vertexNeighborhoodIdx.getUnchecked(u+1);
         idx = vertexNeighborhoods.binarySearch(vertexLowerBound, idx, size);
         idx = (idx < 0) ? (-idx - 1) : idx;
         edgePredicate = edgePredicates.getUnchecked(0);
         for (int i = idx; i < size; ++i) {
            v = vertexNeighborhoods.getUnchecked(i);
            if (!vertexPredicate.test(v)) continue;
            e = edgeNeighborhoods.getUnchecked(i);
            if (!edgePredicate.test(e)) continue;
            consumer.accept(v);
         }

         return;
      }

      validVertices = intArrayListPool.createObject();
      resultSet = intArrayListPool.createObject();

      if (intersectionSize == 1) {
         /* Initialize validVertices with the first intersection and considering vertexPredicate */
         u = intersection.getUnchecked(0);
         idx = vertexNeighborhoodIdx.getUnchecked(u);
         size = vertexNeighborhoodIdx.getUnchecked(u+1);
         idx = vertexNeighborhoods.binarySearch(vertexLowerBound, idx, size);
         idx = (idx < 0) ? (-idx - 1) : idx;
         edgePredicate = edgePredicates.getUnchecked(0);
         for (int i = idx; i < size; ++i) {
            v = vertexNeighborhoods.getUnchecked(i);
            if (!vertexPredicate.test(v)) continue;
            e = edgeNeighborhoods.getUnchecked(i);
            if (!edgePredicate.test(e)) continue;
            validVertices.add(v);
         }

         /* Now that we have valid vertices considering labels, we drop from this set neighborhoods in difference */
         for (int i = 0; i < differenceSize - 1; ++i) {
            u = difference.getUnchecked(i);
            idx = vertexNeighborhoodIdx.getUnchecked(u);
            size = vertexNeighborhoodIdx.getUnchecked(u+1);
            idx = vertexNeighborhoods.binarySearch(vertexLowerBound, idx, size);
            idx = (idx < 0) ? (-idx - 1) : idx;
            Utils.sdifference(validVertices, vertexNeighborhoods, 0, validVertices.size(), idx, size, resultSet);
            validVertices.clear();
            IntArrayList aux = validVertices;
            validVertices = resultSet;
            resultSet = aux;
         }

         u = difference.getLast();
         idx = vertexNeighborhoodIdx.getUnchecked(u);
         size = vertexNeighborhoodIdx.getUnchecked(u+1);
         idx = vertexNeighborhoods.binarySearch(vertexLowerBound, idx, size);
         idx = (idx < 0) ? (-idx - 1) : idx;
         Utils.sdifferenceConsume(validVertices, vertexNeighborhoods, 0, validVertices.size(), idx, size, consumer);

         intArrayListPool.reclaimObject(validVertices);
         intArrayListPool.reclaimObject(resultSet);

         return;
      }

      /* Initialize validVertices with the first intersection and considering vertexPredicate */
      u = intersection.getUnchecked(0);
      idx = vertexNeighborhoodIdx.getUnchecked(u);
      size = vertexNeighborhoodIdx.getUnchecked(u+1);
      idx = vertexNeighborhoods.binarySearch(vertexLowerBound, idx, size);
      idx = (idx < 0) ? (-idx - 1) : idx;
      edgePredicate = edgePredicates.getUnchecked(0);
      for (int i = idx; i < size; ++i) {
         v = vertexNeighborhoods.getUnchecked(i);
         if (!vertexPredicate.test(v)) continue;
         e = edgeNeighborhoods.getUnchecked(i);
         if (!edgePredicate.test(e)) continue;
         validVertices.add(v);
      }

      /* Now that we have valid vertices considering labels, we drop from this set neighborhoods in difference */
      for (int i = 0; i < differenceSize; ++i) {
         u = difference.getUnchecked(i);
         idx = vertexNeighborhoodIdx.getUnchecked(u);
         size = vertexNeighborhoodIdx.getUnchecked(u+1);
         idx = vertexNeighborhoods.binarySearch(vertexLowerBound, idx, size);
         idx = (idx < 0) ? (-idx - 1) : idx;
         Utils.sdifference(validVertices, vertexNeighborhoods, 0, validVertices.size(), idx, size, resultSet);
         validVertices.clear();
         IntArrayList aux = validVertices;
         validVertices = resultSet;
         resultSet = aux;
      }

      /* Now that we have excluded all difference neighborhoods, we intersect the current set with other intersections */
      for (int i = 1; i < intersectionSize - 1; ++i) {
         u = intersection.getUnchecked(i);
         idx = vertexNeighborhoodIdx.getUnchecked(u);
         size = vertexNeighborhoodIdx.getUnchecked(u+1);
         idx = vertexNeighborhoods.binarySearch(vertexLowerBound, idx, size);
         idx = (idx < 0) ? (-idx - 1) : idx;
         //Utils.sintersect(validVertices, vertexNeighborhoods, 0, validVertices.size(), idx, size, resultSet);
         Utils.sintersectWithKeyPred(validVertices, vertexNeighborhoods, 0, validVertices.size(), idx, size,
                 resultSet, edgeNeighborhoods, edgePredicates.getUnchecked(i));
         validVertices.clear();
         IntArrayList aux = validVertices;
         validVertices = resultSet;
         resultSet = aux;
      }

      /* Last intersection consumes the result set */
      u = intersection.getLast();
      idx = vertexNeighborhoodIdx.getUnchecked(u);
      size = vertexNeighborhoodIdx.getUnchecked(u+1);
      idx = vertexNeighborhoods.binarySearch(vertexLowerBound, idx, size);
      idx = (idx < 0) ? (-idx - 1) : idx;
      Utils.sintersectConsumeWithKeyPred(validVertices, vertexNeighborhoods, 0, validVertices.size(), idx, size,
              consumer, edgeNeighborhoods, edgePredicates.getLast());

      intArrayListPool.reclaimObject(validVertices);
      intArrayListPool.reclaimObject(resultSet);
   }

   public void neighborhoodTraversal(int u, IntIntConsumer consumer) {
      int startIdx = vertexNeighborhoodIdx.getUnchecked(u);
      int endIdx = vertexNeighborhoodIdx.getUnchecked(u+1);
      neighborhoodTraversal(startIdx, endIdx, consumer);
   }

   @Override
   public void neighborhoodTraversalEdgeRange(int u, int lowerBound, IntIntConsumer consumer) {
      int startIdx = vertexNeighborhoodIdx.getUnchecked(u);
      int endIdx = vertexNeighborhoodIdx.getUnchecked(u+1);
      startIdx = edgeNeighborhoods.binarySearch(lowerBound, startIdx, endIdx);
      startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx;
      neighborhoodTraversal(startIdx, endIdx, consumer);
   }

   @Override
   public void neighborhoodTraversalVertexRange(int u, int lowerBound, IntIntConsumer consumer) {
      int startIdx = vertexNeighborhoodIdx.getUnchecked(u);
      int endIdx = vertexNeighborhoodIdx.getUnchecked(u+1);
      startIdx = vertexNeighborhoods.binarySearch(lowerBound, startIdx, endIdx);
      startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx;
      neighborhoodTraversal(startIdx, endIdx, consumer);
   }

   public void neighborhoodTraversalVertexRange(int u, int lowerBound, int upperBound, IntIntConsumer consumer) {
      int startIdx = vertexNeighborhoodIdx.getUnchecked(u);
      int endIdx = vertexNeighborhoodIdx.getUnchecked(u+1);
      startIdx = vertexNeighborhoods.binarySearch(lowerBound, startIdx, endIdx);
      startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx;
      endIdx = vertexNeighborhoods.binarySearch(upperBound, startIdx, endIdx);
      endIdx = (endIdx < 0) ? (-endIdx - 1) : endIdx;
      neighborhoodTraversal(startIdx, endIdx, consumer);
   }

   public void neighborhoodTraversal(int startIdx, int endIdx, IntIntConsumer consumer) {
      for (int i = startIdx; i < endIdx; ++i) {
         consumer.accept(vertexNeighborhoods.getUnchecked(i), edgeNeighborhoods.getUnchecked(i));
      }
   }

   /* temporary: single label { */
   @Override
   public int vertexLabel(int u) {
      return vertexLabels.getUnchecked(vertexLabelsIdx.getUnchecked(u));
   }

   @Override
   public int edgeLabel(int e) {
      return edgeLabels.getUnchecked(edgeLabelsIdx.getUnchecked(e));
   }

   /* } */

   public int numVertices() {
      return numVertices;
   }

   public int numEdges() {
      return numEdges;
   }

   public static void main(String[] args) throws IOException {
      FileInputStream in = new FileInputStream(args[0]);
      SuccinctMainGraph graph = new SuccinctMainGraph();
      graph.readFromInputStream(in);
      //IntArrayList intersection = new IntArrayList();
      //intersection.add(1690);
      //intersection.add(2639);
      //intersection.add(2869);
      //intersection.add(2876);
      //IntArrayList difference = new IntArrayList();
      //EdgePredicates predicates = new EdgePredicates();
      //EdgePredicate edgePredicate = new EdgePredicate();
      //edgePredicate.set(graph, 1, 1);
      //predicates.add(edgePredicate);
      //predicates.add(edgePredicate);
      //predicates.add(edgePredicate);
      //predicates.add(edgePredicate);
      //graph.neighborhoodTraversal(intersection, difference, 0, (u,e) -> System.out.println("intersection " + u + " " + e),
      //        predicates);
   }
}
