package br.ufmg.cs.systems.fractal.graph;

import br.ufmg.cs.systems.fractal.util.*;
import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayListView;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import com.koloboke.collect.IntCollection;
import com.koloboke.function.IntIntConsumer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

public class SuccinctMainGraph implements MainGraph {
   private static final Logger LOG = Logger.getLogger(SuccinctMainGraph.class);

   /**
    * Default parameters
    */
   private final DefaultVertexPredicate defaultVertexPredicate = new DefaultVertexPredicate();
   private final DefaultEdgePredicate defaultEdgePredicate = new DefaultEdgePredicate();
   private final DefaultEdgePredicates defaultEdgePredicates = new DefaultEdgePredicates();

   /**
    * Unique graph id (per JVM)
    */
   private static AtomicInteger nextGraphId = new AtomicInteger(0);
   protected int id = nextGraphId.getAndIncrement();

   /* Succinct graph representation */
   protected int numEdges;
   protected int numVertices;
   protected IntArrayList vertexNeighborhoodIdx;
   protected IntArrayList vertexNeighborhoodIdxDag;
   protected IntArrayList vertexNeighborhoods; // by default, sorted
   protected IntArrayList edgeNeighborhoods; // by default, sorted
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

      long start = System.currentTimeMillis();

      if (path instanceof Path) {
         Path filePath = (Path) path;
         readFromFile(filePath);
      } else if (path instanceof org.apache.hadoop.fs.Path) {
         org.apache.hadoop.fs.Path hadoopPath = (org.apache.hadoop.fs.Path) path;
         readFromHdfs(hadoopPath);
      } else {
         throw new RuntimeException("Invalid path: " + path);
      }

      LOG.info("vertexNeighborhoodIdx " + vertexNeighborhoodIdx.size());
      LOG.info("vertexNeighborhoods " + vertexNeighborhoods.size());
      LOG.info("edgeNeighborhoods " + edgeNeighborhoods.size());
      LOG.info("edgeSrcs " + edgeSrcs.size());
      LOG.info("edgeDsts " + edgeDsts.size());

      LOG.info("vertexLabelsIdx " + vertexLabelsIdx.size());
      LOG.info("vertexLabels " + vertexLabels.size());
      LOG.info("edgeLabelsIdx " + edgeLabelsIdx.size());
      LOG.info("edgeLabels " + edgeLabels.size());

      long elapsed = System.currentTimeMillis() - start;
      LOG.info("GraphReading took " + elapsed + " ms");
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
         vertexNeighborhoodIdxDag = new IntArrayList(numVertices + 1);
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
         vertexNeighborhoodIdxDag.add(vertexNeighborhoods.size());
         vertexLabelsIdx.add(vertexLabels.size());
         edgeLabelsIdx.add(edgeLabels.size());

      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public void addVertex(int u) {
      vertexNeighborhoodIdx.add(vertexNeighborhoods.size());
      vertexNeighborhoodIdxDag.add(vertexNeighborhoods.size());
   }

   @Override
   public void addVertexLabel(int u, int label) {
      vertexLabelsIdx.add(vertexLabels.size());
      vertexLabels.add(label);
   }

   @Override
   public void addEdge(int u, int v, int e) {
      vertexNeighborhoods.add(v);
      edgeNeighborhoods.add(e);
      if (u < v) {
         edgeSrcs.add(u);
         edgeDsts.add(v);
      } else vertexNeighborhoodIdxDag.setUnchecked(u, vertexNeighborhoodIdxDag.getUnchecked(u) + 1);
}

   @Override
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
      return id;
   }

   @Override
   public void setId(int id) {
      this.id = id;
   }

   @Override
   public MainGraph addVertex(Vertex vertex) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Vertex getVertex(int vertexId) {
      throw new UnsupportedOperationException();
   }

   @Override
   public int numVertices() {
      return numVertices;
   }

   @Override
   public Edge getEdge(int edgeId) {
      throw new UnsupportedOperationException();
   }

   @Override
   public int numEdges() {
      return numEdges;
   }

   @Override
   public MainGraph addEdge(Edge edge) {
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
   public void forEachEdge(int u, int v, IntConsumer consumer) {
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
   public int undoVertexFilter() {
      return 0;
   }

   @Override
   public int undoEdgeFilter() {
      return 0;
   }

   @Override
   public int filter(AtomicBitSetArray vtag, AtomicBitSetArray etag) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void afterGraphUpdate() {
      throw new UnsupportedOperationException();
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
      throw new UnsupportedOperationException();
   }

   @Override
   public int filterVertices(Predicate vpred) {
      throw new UnsupportedOperationException();
   }

   public void neighborhoodTraversal(IntArrayList vertexNeighborhoodIdx, IntArrayList intersection, IntArrayList difference,
                                     int vertexLowerBound, int vertexUpperBound,
                                     IntConsumer consumer, IntPredicate vertexPredicate, EdgePredicates edgePredicates) {
      if (intersection.size() == 0) {
         for (int u = vertexLowerBound + 1; u < numVertices; ++u) {
            consumer.accept(u);
         }
         return;
      }

      /* Declarations */
      IntArrayList validVertices = null;
      IntArrayList resultSet = null;
      EdgePredicate edgePredicate = null;
      int u, v, e, idx, size;
      int intersectionSize = intersection.size();
      int differenceSize = difference == null ? 0 : difference.size();

      if (intersectionSize == 1 && differenceSize == 0) {
         /* Initialize validVertices with the first intersection and considering vertexPredicate */
         u = intersection.getUnchecked(0);
         idx = vertexNeighborhoodIdx.getUnchecked(u);
         size = vertexNeighborhoodIdx.getUnchecked(u+1);
         idx = vertexLowerBound == Integer.MIN_VALUE ? idx : vertexNeighborhoods.binarySearch(vertexLowerBound, idx, size);
         idx = (idx < 0) ? (-idx - 1) : idx;
         size = vertexUpperBound == Integer.MAX_VALUE ? size : vertexNeighborhoods.binarySearch(vertexUpperBound, idx, size);
         size = (size < 0) ? (-size - 1) : size;
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
         idx = vertexLowerBound == Integer.MIN_VALUE ? idx : vertexNeighborhoods.binarySearch(vertexLowerBound, idx, size);
         idx = (idx < 0) ? (-idx - 1) : idx;
         size = vertexUpperBound == Integer.MAX_VALUE ? size : vertexNeighborhoods.binarySearch(vertexUpperBound, idx, size);
         size = (size < 0) ? (-size - 1) : size;
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
            idx = vertexLowerBound == Integer.MIN_VALUE ? idx : vertexNeighborhoods.binarySearch(vertexLowerBound, idx, size);
            idx = (idx < 0) ? (-idx - 1) : idx;
            size = vertexUpperBound == Integer.MAX_VALUE ? size : vertexNeighborhoods.binarySearch(vertexUpperBound, idx, size);
            size = (size < 0) ? (-size - 1) : size;
            Utils.sdifference(validVertices, vertexNeighborhoods, 0, validVertices.size(), idx, size, resultSet);
            validVertices.clear();
            IntArrayList aux = validVertices;
            validVertices = resultSet;
            resultSet = aux;
         }

         u = difference.getLast();
         idx = vertexNeighborhoodIdx.getUnchecked(u);
         size = vertexNeighborhoodIdx.getUnchecked(u+1);
         idx = vertexLowerBound == Integer.MIN_VALUE ? idx : vertexNeighborhoods.binarySearch(vertexLowerBound, idx, size);
         idx = (idx < 0) ? (-idx - 1) : idx;
         size = vertexUpperBound == Integer.MAX_VALUE ? size : vertexNeighborhoods.binarySearch(vertexUpperBound, idx, size);
         size = (size < 0) ? (-size - 1) : size;
         Utils.sdifferenceConsume(validVertices, vertexNeighborhoods, 0, validVertices.size(), idx, size, consumer);

         intArrayListPool.reclaimObject(validVertices);
         intArrayListPool.reclaimObject(resultSet);

         return;
      }

      /* Initialize validVertices with the first intersection and considering vertexPredicate */
      u = intersection.getUnchecked(0);
      idx = vertexNeighborhoodIdx.getUnchecked(u);
      size = vertexNeighborhoodIdx.getUnchecked(u+1);
      idx = vertexLowerBound == Integer.MIN_VALUE ? idx : vertexNeighborhoods.binarySearch(vertexLowerBound, idx, size);
      idx = (idx < 0) ? (-idx - 1) : idx;
      size = vertexUpperBound == Integer.MAX_VALUE ? size : vertexNeighborhoods.binarySearch(vertexUpperBound, idx, size);
      size = (size < 0) ? (-size - 1) : size;
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
         idx = vertexLowerBound == Integer.MIN_VALUE ? idx : vertexNeighborhoods.binarySearch(vertexLowerBound, idx, size);
         idx = (idx < 0) ? (-idx - 1) : idx;
         size = vertexUpperBound == Integer.MAX_VALUE ? size : vertexNeighborhoods.binarySearch(vertexUpperBound, idx, size);
         size = (size < 0) ? (-size - 1) : size;
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
         idx = vertexLowerBound == Integer.MIN_VALUE ? idx : vertexNeighborhoods.binarySearch(vertexLowerBound, idx, size);
         idx = (idx < 0) ? (-idx - 1) : idx;
         size = vertexUpperBound == Integer.MAX_VALUE ? size : vertexNeighborhoods.binarySearch(vertexUpperBound, idx, size);
         size = (size < 0) ? (-size - 1) : size;
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
      idx = vertexLowerBound == Integer.MIN_VALUE ? idx : vertexNeighborhoods.binarySearch(vertexLowerBound, idx, size);
      idx = (idx < 0) ? (-idx - 1) : idx;
      size = vertexUpperBound == Integer.MAX_VALUE ? size : vertexNeighborhoods.binarySearch(vertexUpperBound, idx, size);
      size = (size < 0) ? (-size - 1) : size;
      Utils.sintersectConsumeWithKeyPred(validVertices, vertexNeighborhoods, 0, validVertices.size(), idx, size,
              consumer, edgeNeighborhoods, edgePredicates.getLast());

      intArrayListPool.reclaimObject(validVertices);
      intArrayListPool.reclaimObject(resultSet);
   }

   @Override
   public void forEachEdge(IntConsumer consumer) {
      for (int e = 0; e < numEdges; ++e) {
         consumer.accept(e);
      }
   }

   @Override
   public IntArrayListView neighborhoodVertices(int u) {
      int from = vertexNeighborhoodIdx.getUnchecked(u);
      int to = vertexNeighborhoodIdx.getUnchecked(u+1);
      return vertexNeighborhoods.view(from, to);
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
   public void neighborhoodTraversal(IntArrayList intersection, IntArrayList difference, int vertexLowerBound, int vertexUpperBound, IntConsumer consumer, IntPredicate vertexPredicate, EdgePredicates edgePredicates) {
      neighborhoodTraversal(vertexNeighborhoodIdx, intersection, difference, vertexLowerBound, vertexUpperBound, consumer, vertexPredicate, edgePredicates);
   }

   @Override
   public void neighborhoodTraversalVertexRange(int u, int lowerBound, IntIntConsumer consumer) {
      int startIdx = vertexNeighborhoodIdx.getUnchecked(u);
      int endIdx = vertexNeighborhoodIdx.getUnchecked(u+1);
      startIdx = vertexNeighborhoods.binarySearch(lowerBound, startIdx, endIdx);
      startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx;
      neighborhoodTraversal(startIdx, endIdx, consumer);
   }

   private void neighborhoodTraversal(int startIdx, int endIdx, IntIntConsumer consumer) {
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

   /**
    * Default parameters
    */
   private class DefaultVertexPredicate implements IntPredicate {
      @Override
      public boolean test(int u) {
         return true;
      }
   }

   private class DefaultEdgePredicate extends EdgePredicate {
      @Override
      public boolean test(int e) {
         return true;
      }
   }

   private class DefaultEdgePredicates extends EdgePredicates {
      @Override
      public EdgePredicate get(int i) {
         return SuccinctMainGraph.this.defaultEdgePredicate;
      }

      @Override
      public EdgePredicate getUnchecked(int i) {
         return SuccinctMainGraph.this.defaultEdgePredicate;
      }

      @Override
      public EdgePredicate getLast() {
         return SuccinctMainGraph.this.defaultEdgePredicate;
      }
   }
}
