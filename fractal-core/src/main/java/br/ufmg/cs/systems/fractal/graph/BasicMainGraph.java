package br.ufmg.cs.systems.fractal.graph;

import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.ReclaimableIntCollection;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.map.IntObjCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.IntObjMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import java.util.function.IntConsumer;

import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.koloboke.function.IntIntConsumer;
import com.koloboke.function.IntObjConsumer;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public class BasicMainGraph<V,E> implements MainGraph<V,E> {
   private static final Logger LOG = Logger.getLogger(BasicMainGraph.class);

   // we keep a local (per JVM) pool of configurations potentially
   // representing several active fractal applications
   private static AtomicInteger nextGraphId = new AtomicInteger(0);
   protected int id = newGraphId();

   private static final int INITIAL_ARRAY_SIZE = 4096;
   
   protected IntIntMap vertexIdMap = 
      HashIntIntMaps.getDefaultFactory().withDefaultValue(-1).newMutableMap();

   protected Vertex<V>[] vertexIndexF;
   protected Edge<E>[] edgeIndexF;
   
   protected V[] vertexProperties;
   protected E[] edgeProperties;

   protected int numVertices;
   protected int numEdges;
   
   private int numVertexLabels;
   private int numEdgeLabels;

   private VertexNeighbourhood[] vertexNeighborhoods;

   private IntObjMap<VertexNeighbourhood> removedNeighborhoods;
   private IntObjMap<Vertex<V>> removedVertices;
   private IntObjMap<Edge<E>> removedEdges;

   protected final EdgeRemover edgeRemover = new EdgeRemover();
   protected final VertexReset vertexReset = new VertexReset();
   protected final EdgeReset edgeReset = new EdgeReset();
   protected final NeighborhoodReset neighborhoodReset = new NeighborhoodReset();

   protected boolean isEdgeLabelled;
   protected boolean isMultiGraph;
   private String name;

   private static int newGraphId() {
      return nextGraphId.getAndIncrement();
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
   public int undoVertexFilter() {
      //LOG.info("removedVertices " + removedVertices);
      removedVertices.forEach(vertexReset);
      //LOG.info("removedNeighborhoods " + removedNeighborhoods);
      removedNeighborhoods.forEach(neighborhoodReset);
      removedVertices.clear();
      removedNeighborhoods.clear();
      return 0;
   }
   
   @Override
   public int undoEdgeFilter() {
      //LOG.info("removedEdges" + removedEdges);
      removedEdges.forEach(edgeReset);
      removedEdges.clear();
      return 0;
   }
   
   @Override
   public int filter(AtomicBitSetArray vtag, AtomicBitSetArray etag) {
      int removedEdges = 0;
      int removedVertices = 0;
      for (int i = 0; i < vertexNeighborhoods.length; ++i) {
         if (vertexNeighborhoods[i] != null) {
            if (!vtag.contains(i)) {
               vertexNeighborhoods[i] = null;
               ++removedVertices;
            } else {
               removedEdges += vertexNeighborhoods[i].filter(vtag, etag);
            }
         }
      }

      LOG.info("GraphTagging removedVertices=" + removedVertices +
         " removedEdges=" + removedEdges);

      return numVertices - removedVertices;
   }

   @Override
   public int filterVertices(Predicate<Vertex<V>> vpred) {
      int numRemovedVertices = 0;
      for (int vertexId = 0; vertexId < numVertices; ++vertexId) {
         Vertex vertex = getVertex(vertexId);
         if (vertex != null) {
            if (!vpred.test(vertex)) {
               removeVertex(vertex);
               ++numRemovedVertices;
            }
         } else {
            vertex = removedVertices.remove(vertexId);
            if (vpred.test(vertex)) {
               addVertex(vertex);
               addNeighborhood(vertexId, removedNeighborhoods.remove(vertexId));
            } else {
               ++numRemovedVertices;
            }
         }
      }

      IntObjCursor<Vertex<V>> cur = removedVertices.cursor();
      while (cur.moveNext()) {
         vertexIndexF[cur.key()] = null;
         vertexNeighborhoods[cur.key()] = null;
      }

      return numRemovedVertices;
   }

   @Override
   public int filterEdges(Predicate<Edge<E>> epred) {
      int numRemovedEdges = 0;
      for (int edgeId = 0; edgeId < numEdges; ++edgeId) {
         Edge edge = getEdge(edgeId);
         if (edge != null) {
            if (!epred.test(edge)) {
               removeEdge(edge);
               ++numRemovedEdges;
            }
         } else {
            edge = removedEdges.remove(edgeId);
            if (epred.test(edge)) {
               int src = edge.getSourceId();
               int dst = edge.getDestinationId();
               VertexNeighbourhood neighborhood = vertexNeighborhoods[src];
               if (neighborhood == null) {
                  Vertex vertex = removedVertices.remove(src);
                  addVertex(vertex);
                  addNeighborhood(src, removedNeighborhoods.remove(src));
                  neighborhood = vertexNeighborhoods[src];
               }
               neighborhood.addEdge(dst, edge.getEdgeId());

               neighborhood = vertexNeighborhoods[dst];
               if (neighborhood == null) {
                  Vertex vertex = removedVertices.remove(dst);
                  addVertex(vertex);
                  addNeighborhood(dst, removedNeighborhoods.remove(dst));
                  neighborhood = vertexNeighborhoods[dst];
               }
               neighborhood.addEdge(src, edge.getEdgeId());
               edgeIndexF[edge.getEdgeId()] = edge;
            } else {
               ++numRemovedEdges;
            }
         }
      }

      return numRemovedEdges;
   }

   @Override
   public int filterVertices(AtomicBitSetArray tag) {
      int removedEdges = 0;
      int removedVertices = 0;
      for (int i = 0; i < vertexNeighborhoods.length; ++i) {
         if (vertexNeighborhoods[i] != null) {
            if (!tag.contains(i)) {
               vertexNeighborhoods[i] = null;
               ++removedVertices;
            } else {
               removedEdges += vertexNeighborhoods[i].filterVertices(tag);
            }
         }
      }

      LOG.info("GraphTagging removedVertices=" + removedVertices +
         " removedEdges=" + removedEdges);

      return numVertices - removedVertices;
   }

   @Override
   public int filterEdges(AtomicBitSetArray tag) {
      int numEdges = 0;
      for (int i = 0; i < vertexNeighborhoods.length; ++i) {
         if (vertexNeighborhoods[i] != null) {
            int neighborhoodSize = vertexNeighborhoods[i].filterEdges(tag);
            numEdges += neighborhoodSize;
         }
      }

      return numEdges;
   }

   protected void removeVertex(Vertex vertex) {
      int vertexId = vertex.getVertexId();
      VertexNeighbourhood neighborhood = vertexNeighborhoods[vertexId];
      if (neighborhood != null) {
         synchronized (removedNeighborhoods) {
            //vertexNeighborhoods[vertexId] = null;
            removedNeighborhoods.put(vertexId, neighborhood);
         }
         synchronized (removedVertices) {
            //vertexIndexF[vertexId] = null;
            removedVertices.put(vertexId, vertex);
         }
         synchronized (neighborhood) {
            IntCursor cur = neighborhood.getNeighborVertices().cursor();
            while (cur.moveNext()) {
               VertexNeighbourhood otherNeighborhood = vertexNeighborhoods[cur.elem()];
               IntCollection edgeIds = otherNeighborhood.getEdgesWithNeighbourVertex(vertexId);
               IntCursor cur2 = edgeIds.cursor();
               while (cur2.moveNext()) {
                  synchronized (removedEdges) {
                     removedEdges.put(cur2.elem(), getEdge(cur2.elem()));
                     edgeIndexF[cur2.elem()] = null;
                  }
               }
               otherNeighborhood.removeVertex(vertexId);
               cur.remove();
            }
         }
      }
   }

   protected void removeEdge(int edgeId) {
      removeEdge(getEdge(edgeId));
   }

   protected void removeEdge(Edge edge) {
     int edgeId = edge.getEdgeId();
     synchronized (removedEdges) {
        edgeIndexF[edgeId] = null;
        removedEdges.put(edgeId, edge);
     }

      int src = edge.getSourceId();
      int dst = edge.getDestinationId();

      VertexNeighbourhood neighborhood = vertexNeighborhoods[src];
      if (neighborhood != null) {
         neighborhood.removeVertex(dst);
      }

      neighborhood = vertexNeighborhoods[dst];
      if (neighborhood != null) {
         neighborhood.removeVertex(src);
      }
   }

   private void init(String name, boolean isEdgeLabelled, boolean isMultiGraph) {
      this.name = name;
      long start = 0;

      if (LOG.isInfoEnabled()) {
         start = System.currentTimeMillis();
         LOG.info("Initializing graph," +
               " id=" + id +
               " name=" + name +
               " isEdgeLabelled=" + isEdgeLabelled +
               " isMultiGraph=" + isMultiGraph +
               " class=" + getClass());
      }

      vertexIndexF = null;
      edgeIndexF = null;

      vertexNeighborhoods = null;
      removedNeighborhoods = HashIntObjMaps.newMutableMap();
      removedVertices = HashIntObjMaps.newMutableMap();
      removedEdges = HashIntObjMaps.newMutableMap();

      reset();

      this.isEdgeLabelled = isEdgeLabelled;
      this.isMultiGraph = isMultiGraph;

      if (LOG.isInfoEnabled()) {
         LOG.info("Done initializing graph," +
               " id=" + id +
               " name=" + name +
               " isEdgeLabelled=" + isEdgeLabelled +
               " isMultiGraph=" + isMultiGraph +
               " class=" + getClass() +
               " elapsed=" + (System.currentTimeMillis() - start) + " ms");
      }
   }

   public void init(Object path) throws IOException {
      long start = 0;

      if (LOG.isInfoEnabled()) {
         LOG.info("Reading graph," +
               " id=" + id +
               " name=" + name +
               " path=" + path +
               " isEdgeLabelled=" + isEdgeLabelled +
               " isMultiGraph=" + isMultiGraph +
               " class=" + getClass());
         start = System.currentTimeMillis();
      }

      if (path instanceof Path) {
         Path filePath = (Path) path;
         readFromFile(filePath);
      } else if (path instanceof org.apache.hadoop.fs.Path) {
         org.apache.hadoop.fs.Path hadoopPath = (org.apache.hadoop.fs.Path) path;
         readFromHdfs(hadoopPath);
      } else {
         throw new RuntimeException("Invalid path: " + path);
      }

      buildSortedNeighborhood();      

      if (LOG.isInfoEnabled()) {
         LOG.info("Done reading graph," +
               " id=" + id +
               " name=" + name +
               " path=" + path +
               " isEdgeLabelled=" + isEdgeLabelled +
               " isMultiGraph=" + isMultiGraph +
               " class=" + getClass() +
               " numVertices=" + numVertices +
               " numEdges=" + numEdges +
               " elapsed=" + (System.currentTimeMillis() - start));
      }
   }

   @Override
   public void buildSortedNeighborhood() {
      // build sorted neighborhood for fast subgraph enumeration
      for (int i = 0; i < vertexNeighborhoods.length; ++i) {
         if (vertexNeighborhoods[i] != null) {
            vertexNeighborhoods[i].buildSortedNeighborhood();
            //LOG.info(vertexNeighborhoods[i]);
         }
      }
   }

   public void initProperties(Object path) throws IOException {
      long start = 0;

      if (LOG.isInfoEnabled()) {
         LOG.info("Reading graph properties");
         start = System.currentTimeMillis();
      }

      if (path instanceof Path) {
         Path filePath = (Path) path;
         readPropertiesFromFile(filePath);
      } else if (path instanceof org.apache.hadoop.fs.Path) {
         org.apache.hadoop.fs.Path hadoopPath = (org.apache.hadoop.fs.Path) path;
         readPropertiesFromHdfs(hadoopPath);
      } else {
         throw new RuntimeException("Invalid path: " + path);
      }

      if (LOG.isInfoEnabled()) {
         LOG.info("Properties read done in " +
               (System.currentTimeMillis() - start) +
               " numVertexProperties=" + numVertexLabels +
               " numEdgeProperties=" + numEdgeLabels);
      }
   }

   private void prepareStructures(int numVertices, int numEdges) {
      ensureCanStoreNewVertices(numVertices);
      ensureCanStoreNewEdges(numEdges);
   }

   @Override
   public void reset() {
      numVertices = 0;
      numEdges = 0;
      numVertexLabels = 0;
      numEdgeLabels = 0;
   }

   private <T> T[] maybeExpandArray(T[] currArray, int maxId) {
      int targetSize = maxId + 1;
      T[] returnArray;

      if (currArray == null) {
         returnArray = (T[]) new Object[Math.max(targetSize, INITIAL_ARRAY_SIZE)];
      } else if (currArray.length < targetSize) {
         returnArray = Arrays.copyOf(currArray,
               getSizeWithPaddingWithoutOverflow(targetSize, currArray.length));
      } else {
         returnArray = currArray;
      }

      return returnArray;
   }

   private void ensureCanStoreNewVertexLabel(int newMaxVertexLabelId) {
      vertexProperties = maybeExpandArray(vertexProperties, newMaxVertexLabelId);
   }
   
   private void ensureCanStoreNewEdgeLabel(int newMaxEdgeLabelId) {
      edgeProperties = maybeExpandArray(edgeProperties, newMaxEdgeLabelId);
   }

   private void ensureCanStoreNewVertices(int numVerticesToAdd) {
      int newMaxVertexId = numVertices + numVerticesToAdd;
      ensureCanStoreUpToVertex(newMaxVertexId);
   }

   private void ensureCanStoreUpToVertex(int maxVertexId) {
      int targetSize = maxVertexId + 1;

      if (vertexIndexF == null) {
         vertexIndexF = new Vertex[Math.max(targetSize, INITIAL_ARRAY_SIZE)];
      } else if (vertexIndexF.length < targetSize) {
         vertexIndexF = Arrays.copyOf(vertexIndexF, getSizeWithPaddingWithoutOverflow(targetSize, vertexIndexF.length));
      }

      if (vertexNeighborhoods == null) {
         vertexNeighborhoods = new VertexNeighbourhood[Math.max(targetSize, INITIAL_ARRAY_SIZE)];
      } else if (vertexNeighborhoods.length < targetSize) {
         vertexNeighborhoods = Arrays.copyOf(vertexNeighborhoods, getSizeWithPaddingWithoutOverflow(targetSize, vertexNeighborhoods.length));
      }
   }

   private int getSizeWithPaddingWithoutOverflow(int targetSize, int currentSize) {
      if (currentSize > targetSize) {
         return currentSize;
      }

      int sizeWithPadding = Math.max(currentSize, 1);

      while (true) {
         int previousSizeWithPadding = sizeWithPadding;

         // Multiply by 2
         sizeWithPadding <<= 1;

         // If we saw an overflow, return simple targetSize
         if (previousSizeWithPadding > sizeWithPadding) {
            return targetSize;
         }

         if (sizeWithPadding >= targetSize) {
            return sizeWithPadding;
         }
      }
   }

   private void ensureCanStoreNewVertex() {
      ensureCanStoreNewVertices(1);
   }

   private void ensureCanStoreNewEdges(int numEdgesToAdd) {
      if (edgeIndexF == null) {
         edgeIndexF = new Edge[Math.max(numEdgesToAdd, INITIAL_ARRAY_SIZE)];
      } else if (edgeIndexF.length < numEdges + numEdgesToAdd) {
         int targetSize = edgeIndexF.length + numEdgesToAdd;
         edgeIndexF = Arrays.copyOf(edgeIndexF, getSizeWithPaddingWithoutOverflow(targetSize, edgeIndexF.length));
      }
   }

   private void ensureCanStoreNewEdge() {
      ensureCanStoreNewEdges(1);
   }

   public BasicMainGraph(String name) {
      this(name, false, false);
   }

   public BasicMainGraph(String name, boolean isEdgeLabelled, boolean isMultiGraph) {
      init(name, isEdgeLabelled, isMultiGraph);
   }

   public BasicMainGraph(Path filePath, boolean isEdgeLabelled, boolean isMultiGraph)
           throws IOException {
      this(filePath.getFileName().toString(), isEdgeLabelled, isMultiGraph);
   }

   public BasicMainGraph(org.apache.hadoop.fs.Path hdfsPath, boolean isEdgeLabelled, boolean isMultiGraph)
           throws IOException {
      this(hdfsPath.getName(), isEdgeLabelled, isMultiGraph);
   }

   @Override
   public boolean isNeighborVertex(int v1, int v2) {
      VertexNeighbourhood v1Neighbourhood = vertexNeighborhoods[v1];
      return v1Neighbourhood != null && v1Neighbourhood.isNeighbourVertex(v2);
   }

   protected void addNeighborhood(int vertexId, VertexNeighbourhood neighborhood) {
      vertexNeighborhoods[vertexId] = neighborhood;
   }

   @Override
   public MainGraph addVertex(Vertex vertex) {
      if (vertex.getVertexId() == numVertices) {
         ensureCanStoreNewVertex();
         ++numVertices;
      }
      vertexIndexF[vertex.getVertexId()] = vertex;
      return this;
   }

   @Override
   public Vertex<V>[] getVertices() {
      return vertexIndexF;
   }

   @Override
   public Vertex<V> getVertex(int vertexId) {
      return vertexIndexF[vertexId];
   }

   @Override
   public int getNumberVertices() {
      return numVertices;
   }

   @Override
   public Edge<E>[] getEdges() {
      return edgeIndexF;
   }

   @Override
   public Edge<E> getEdge(int edgeId) {
      return edgeIndexF[edgeId];
   }

   @Override
   public int getNumberEdges() {
      return numEdges;
   }

   @Override
   public ReclaimableIntCollection getEdgeIds(int v1, int v2) {
      int minv;
      int maxv;

      // TODO: Change this for directed edges
      if (v1 < v2) {
         minv = v1;
         maxv = v2;
      } else {
         minv = v2;
         maxv = v1;
      }

      VertexNeighbourhood vertexNeighbourhood = this.vertexNeighborhoods[minv];

      return vertexNeighbourhood.getEdgesWithNeighbourVertex(maxv);
   }

   @Override
   public void forEachEdgeId(int v1, int v2, IntConsumer intConsumer) {
      int minv;
      int maxv;

      // TODO: Change this for directed edges
      if (v1 < v2) {
         minv = v1;
         maxv = v2;
      } else {
         minv = v2;
         maxv = v1;
      }

      VertexNeighbourhood vertexNeighbourhood = this.vertexNeighborhoods[minv];

      vertexNeighbourhood.forEachEdgeId(maxv, intConsumer);
   }

   @Override
   public MainGraph addEdge(Edge edge) {
      // Assuming input graph contains all edges but we treat it as undirected
      // TODO: What if input only contains one of the edges? Should we enforce
      // this via a sanity check?
      // TODO: Handle this when directed graphs
      if (edge.getSourceId() > edge.getDestinationId()) {
         return this;
      }

      if (edge.getEdgeId() == -1) {
         edge.setEdgeId(numEdges);
      }
      //else if (edge.getEdgeId() != numEdges) {
      //   throw new RuntimeException("Sanity check, edge with id " + edge.getEdgeId() + " added at position " + numEdges);
      //}

      if (edge.getEdgeId() == numEdges) {
         ensureCanStoreNewEdge();
         ++numEdges;
      } else if (edge.getEdgeId() > numEdges) {
         throw new RuntimeException("Sanity check, edge with id " + edge.getEdgeId() + " added at position " + numEdges);
      }

      ensureCanStoreUpToVertex(Math.max(edge.getSourceId(), edge.getDestinationId()));
      edgeIndexF[edge.getEdgeId()] = edge;

      try {
         VertexNeighbourhood vertexNeighbourhood = vertexNeighborhoods[edge.getSourceId()];

         if (vertexNeighbourhood == null) {
            vertexNeighbourhood = createVertexNeighbourhood();
            vertexNeighborhoods[edge.getSourceId()] = vertexNeighbourhood;
         }

         vertexNeighbourhood.addEdge(edge.getDestinationId(), edge.getEdgeId());
      } catch (ArrayIndexOutOfBoundsException e) {
         LOG.error("Tried to access index " + edge.getSourceId() + " of array with size " + vertexNeighborhoods.length);
         LOG.error("vertexIndexF.length=" + vertexIndexF.length);
         LOG.error("vertexNeighborhoods.length=" + vertexNeighborhoods.length);
         throw e;
      }

      try {
         VertexNeighbourhood vertexNeighbourhood = vertexNeighborhoods[edge.getDestinationId()];

         if (vertexNeighbourhood == null) {
            vertexNeighbourhood = createVertexNeighbourhood();
            vertexNeighborhoods[edge.getDestinationId()] = vertexNeighbourhood;
         }

         vertexNeighbourhood.addEdge(edge.getSourceId(), edge.getEdgeId());
      } catch (ArrayIndexOutOfBoundsException e) {
         LOG.error("Tried to access index " + edge.getDestinationId() + " of array with size " + vertexNeighborhoods.length);
         LOG.error("vertexIndexF.length=" + vertexIndexF.length);
         LOG.error("vertexNeighborhoods.length=" + vertexNeighborhoods.length);
         throw e;
      }

      return this;
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

   protected void readPropertiesFromHdfs(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
      FileSystem fs = FileSystem.get(new org.apache.hadoop.conf.Configuration());
      InputStream is = fs.open(hdfsPath);
      readPropertiesFromInputStream(is);
      is.close();
   }

   protected void readPropertiesFromFile(Path filePath) throws IOException {
      InputStream is = Files.newInputStream(filePath);
      readPropertiesFromInputStream(is);
      is.close();
   }

   protected void readPropertiesFromInputStream(InputStream is) {
      try {
         BufferedReader reader = new BufferedReader(
               new InputStreamReader(new BOMInputStream(is)));

         String line = reader.readLine();

         while (line != null) {
            StringTokenizer tokenizer = new StringTokenizer(line);

            String type = tokenizer.nextToken();
            int wordId = Integer.parseInt(tokenizer.nextToken());

            if (type.equals("v")) {
               V vproperty = parseVertexProperty(tokenizer);
               ensureCanStoreNewVertexLabel(wordId);
               vertexProperties[wordId] = vproperty;
               numVertexLabels = Math.max(numVertexLabels, wordId + 1);
               // vertexIndexF[wordId].setProperty(vproperty);
            } else if (type.equals("e")) {
               E eproperty = parseEdgeProperty(tokenizer);
               ensureCanStoreNewEdgeLabel(wordId);
               edgeProperties[wordId] = eproperty;
               numEdgeLabels = Math.max(numEdgeLabels, wordId + 1);
               // edgeIndexF[wordId].setProperty(eproperty);
            } else {
               throw new RuntimeException("Unknown property type: " + type);
            }

            line = reader.readLine();
         }

         reader.close();
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   protected void readFromInputStream(InputStream is) {
      try {
         BufferedReader reader = new BufferedReader(
               new InputStreamReader(new BOMInputStream(is)));

         String line = reader.readLine();
         boolean firstLine = true;

         while (line != null) {
            StringTokenizer tokenizer = new StringTokenizer(line);

            if (firstLine) {
               firstLine = false;

               if (line.startsWith("#")) {
                  LOG.info("Found hints regarding number of vertices and edges");
                  // Skip #
                  tokenizer.nextToken();

                  int numVertices = Integer.parseInt(tokenizer.nextToken());
                  int numEdges = Integer.parseInt(tokenizer.nextToken());

                  LOG.info("Hinted numVertices=" + numVertices);
                  LOG.info("Hinted numEdges=" + numEdges);

                  prepareStructures(numVertices, numEdges);

                  line = reader.readLine();
                  continue;
               }
            }

            Vertex vertex = parseVertex(tokenizer);

            int vertexId = vertex.getVertexId();

            while (tokenizer.hasMoreTokens()) {
               parseEdge(tokenizer, vertexId);
               if (numEdges % 1e7 == 0) {
                  LOG.info("Stats numVertices=" + numVertices +
                        " numEdges=" + numEdges);
               }
            }

            line = reader.readLine();
         }

         reader.close();
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   protected Edge parseEdge(StringTokenizer tokenizer, int vertexId) {
      int neighborId = Integer.parseInt(tokenizer.nextToken());
      int neighborIdx = neighborId;

      //int neighborIdx = vertexIdMap.get(neighborId);
      //if (neighborIdx == -1) {
      //   neighborIdx = vertexIdMap.size();
      //   vertexIdMap.put(neighborId, neighborIdx);
      //   addVertex(createVertex(neighborIdx, neighborId, -1));
      //}

      Edge edge;

      if (isEdgeLabelled) {
         int edgeLabel = Integer.parseInt(tokenizer.nextToken());
         edge = createEdge(vertexId, neighborIdx, edgeLabel);
      } else {
         edge = createEdge(vertexId, neighborIdx);
      }

      addEdge(edge);

      return edge;
   }

   protected Vertex parseVertex(StringTokenizer tokenizer) {
      int vertexId = Integer.parseInt(tokenizer.nextToken());
      int vertexLabel = parseVertexLabel(tokenizer);

      int vertexIdx = vertexIdMap.get(vertexId);
      if (vertexIdx == -1) {
         //vertexIdx = vertexIdMap.size();
         vertexIdx = vertexId;
         //vertexIdMap.put(vertexId, vertexIdx);
         Vertex vertex = createVertex(vertexIdx, vertexId, vertexLabel);
         addVertex(vertex);
         return vertex;
      } else {
         Vertex vertex = vertexIndexF[vertexIdx];
         int currVertexLabel = vertex.getVertexLabel();
         if (currVertexLabel == -1) {
            vertex.setVertexLabel(vertexLabel);
         } else if (currVertexLabel != vertexLabel) {
            throw new RuntimeException("Invalid state vertexLabel=" +
                  vertexLabel + " vertexId=" + vertexId +
                  " vertexIdx=" + vertexIdx + " vertex=" + vertex +
                  " numVertices=" + numVertices +
                  " vertexIdMapSize=" + vertexIdMap.size());
         }
         return vertex;
      }
   }

   protected int parseVertexLabel(StringTokenizer tokenizer) {
      return Integer.parseInt(tokenizer.nextToken());
   }

   protected V parseVertexProperty(StringTokenizer tokenizer) {
      return null;
   }
   
   protected E parseEdgeProperty(StringTokenizer tokenizer) {
      return null;
   }

   @Override
   public String toString() {
      return "Graph(id=" + id + ", name=" + name +
         ", isEdgeLabelled=" + isEdgeLabelled +
         ", isMultiGraph=" + isMultiGraph +
         ", class=" + getClass() + ")";
   }

   public String toDetailedString() {
      return "Vertices: " + Arrays.toString(vertexIndexF) + "\n Edges: " + Arrays.toString(edgeIndexF);
   }

   public String toDebugString() {
      String str = toString() + "\n" + toDetailedString() + "\n";
      for (int i = 0; i < numVertices; ++i) {
         str += vertexNeighborhoods[i] + "\n";
      }
      return str;
   }

   @Override
   public boolean areEdgesNeighbors(int edge1Id, int edge2Id) {
      Edge edge1 = edgeIndexF[edge1Id];
      Edge edge2 = edgeIndexF[edge2Id];

      return edge1.neighborWith(edge2);
   }

   @Override
   public boolean isNeighborEdge(int src1, int dest1, int edge2) {
      int src2 = edgeIndexF[edge2].getSourceId();

      if (src1 == src2) return true;

      int dest2 = edgeIndexF[edge2].getDestinationId();

      return (dest1 == src2 || dest1 == dest2 || src1 == dest2);
   }

   protected Vertex createVertex(int id, int originalId, int label) {
      Vertex vertex = new Vertex(id, originalId, label);
      if (vertexProperties != null) {
         vertex.setProperty(vertexProperties[label]);
      }
      return vertex;
   }

   protected Edge createEdge(int srcId, int destId) {
      return new Edge(srcId, destId);
   }

   protected Edge createEdge(int srcId, int destId, int label) {
      Edge edge = new LabelledEdge(srcId, destId, label);
      if (edgeProperties != null) {
         edge.setProperty(edgeProperties[label]);
      }
      return edge;
   }

   private VertexNeighbourhood createVertexNeighbourhood() {
      if (!isMultiGraph) {
         return new BasicVertexNeighbourhood(this);
      } else {
         return new MultiVertexNeighbourhood(this);
      }
   }

   @Override
   public VertexNeighbourhood getVertexNeighbourhood(int vertexId) {
      return vertexNeighborhoods[vertexId];
   }

   @Override
   public IntCollection getVertexNeighbours(int vertexId) {
      VertexNeighbourhood vertexNeighbourhood = getVertexNeighbourhood(vertexId);

      if (vertexNeighbourhood == null) {
         return null;
      }

      return vertexNeighbourhood.getNeighborVertices();
   }

   @Override
   public boolean isEdgeLabelled() {
      return isEdgeLabelled;
   }

   @Override
   public boolean isMultiGraph() {
      return isMultiGraph;
   }

   public String getName() {
      return name;
   }

   private class EdgeRemover implements IntConsumer {
      @Override
      public void accept(int edgeId) {
         BasicMainGraph.this.removeEdge(edgeId);
      }
   }
   
   private class NeighborhoodReset implements IntObjConsumer<VertexNeighbourhood> {
      @Override
      public void accept(int vertexId, VertexNeighbourhood neighborhood) {
         BasicMainGraph.this.addNeighborhood(vertexId, neighborhood);
      }
   }

   private class VertexReset implements IntObjConsumer<Vertex<V>> {
      @Override
      public void accept(int vertexId, Vertex<V> vertex) {
         BasicMainGraph.this.addVertex(vertex);
      }
   }

   private class EdgeReset implements IntObjConsumer<Edge<E>> {
      @Override
      public void accept(int edgeId, Edge<E> edge) {
         int src = edge.getSourceId();
         int dst = edge.getDestinationId();
         VertexNeighbourhood neighborhood =
                 BasicMainGraph.this.vertexNeighborhoods[src];
         if (neighborhood == null) return;
         neighborhood.addEdge(dst, edge.getEdgeId());
         neighborhood = BasicMainGraph.this.vertexNeighborhoods[dst];
         if (neighborhood == null) return;
         neighborhood.addEdge(src, edge.getEdgeId());
         BasicMainGraph.this.edgeIndexF[edge.getEdgeId()] = edge;
      }
   }
}
