package br.ufmg.cs.systems.fractal.graph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.*;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayListView;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import br.ufmg.cs.systems.fractal.util.pool.IntSetPool;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.set.IntSet;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.function.IntConsumer;

public class VELabeledMainGraph implements MainGraph {
   protected static final Logger LOG =
           Logger.getLogger(VELabeledMainGraph.class);

   /**
    * Default parameters
    */
   private final DefaultEdgePredicate defaultEdgePredicate =
           new DefaultEdgePredicate();

   /* CSR-like graph representation */
   protected int numEdges;
   protected int numValidEdges; // allowed to skip edges while reading
   protected int numVertices;
   protected IntArrayList vertexNeighborhoodIdx;
   protected IntArrayList vertexNeighborhoods; // assumes sorted
   protected IntArrayList edgeNeighborhoods; // assumes sorted
   protected IntArrayList edgeSrcs;
   protected IntArrayList edgeDsts;

   /* Labels */
   protected IntArrayList vertexLabelsIdx;
   protected IntArrayList vertexLabels;
   protected IntArrayList edgeLabelsIdx;
   protected IntArrayList edgeLabels;

   /* Auxiliary */
   protected IntArrayListPool intArrayListPool;

   /* Filtering */
   protected EdgeFilteringPredicate edgePredicate;
   protected IntArrayListView uLabelsView;
   protected IntArrayListView vLabelsView;
   protected IntArrayListView eLabelsView;

   public VELabeledMainGraph() {
   }

   private final void addEdge(int u, int v, int e) {
      if (u < v) { // first time seeing this edge
         if (isEdgeValid(u, v, e)) { // valid
            vertexNeighborhoods.add(v);
            edgeNeighborhoods.add(e);
            edgeSrcs.add(u);
            edgeDsts.add(v);
         } else { // invalid
            edgeSrcs.add(-1);
            edgeDsts.add(v);
         }
      } else { // second time seeing this edge
         if (edgePredicate == null || edgeSrcs.getu(e) != -1) { // valid
            vertexNeighborhoods.add(v);
            edgeNeighborhoods.add(e);
         } else { // invalid
            edgeSrcs.setu(e, v); // fix edge source (consistency)
         }
      }
   }

   private void addEdgeLabel(int e, int label) {
      edgeLabels.add(label);
   }

   private void addVertex(int u) {
      vertexNeighborhoodIdx.add(vertexNeighborhoods.size());
   }

   private void addVertexLabel(int u, int label) {
      vertexLabels.add(label);
   }

   @Override
   public int edgeDst(int e) {
      return edgeDsts.get(e);
   }

   @Override
   public int firstEdgeLabel(int e) {
      return edgeLabels.getu(edgeLabelsIdx.getu(e));
   }

   @Override
   public int edgeSrc(int e) {
      return edgeSrcs.get(e);
   }

   @Override
   public void forEachCommonEdgeLabels(IntArrayList edges,
                                       IntConsumer consumer) {
      int numEdges = edges.size();
      if (numEdges == 0) return;

      int e1, e2;

      if (numEdges == 1) {
         e1 = edges.getu(0);
         int from = edgeLabelsIdx.getu(e1);
         int to = edgeLabelsIdx.getu(e1 + 1);
         for (int i = from; i < to; ++i) {
            consumer.accept(edgeLabels.getu(i));
         }
         return;
      }

      if (numEdges == 2) {
         e1 = edges.get(0);
         e2 = edges.getu(1);
         int from1 = edgeLabelsIdx.getu(e1);
         int to1 = edgeLabelsIdx.getu(e1 + 1);
         int from2 = edgeLabelsIdx.getu(e2);
         int to2 = edgeLabelsIdx.getu(e2 + 1);
         Utils.sintersectConsume(edgeLabels, edgeLabels, from1, to1, from2, to2,
                 consumer);

         return;
      }

      IntArrayList previous = IntArrayListPool.instance().createObject();
      IntArrayList result = IntArrayListPool.instance().createObject();
      IntArrayList aux;

      // first intersection
      e1 = edges.getu(0);
      e2 = edges.getu(1);
      int from1 = edgeLabelsIdx.getu(e1);
      int to1 = edgeLabelsIdx.getu(e1 + 1);
      int from2 = edgeLabelsIdx.getu(e2);
      int to2 = edgeLabelsIdx.getu(e2 + 1);
      result.clear();
      Utils.sintersect(edgeLabels, edgeLabels, from1, to1, from2, to2, result);

      for (int i = 2; i < numEdges - 1; ++i) {
         aux = result;
         result = previous;
         previous = aux;
         e1 = edges.getu(i);
         from1 = edgeLabelsIdx.getu(e1);
         to1 = edgeLabelsIdx.getu(e1 + 1);
         result.clear();
         Utils.sintersect(previous, edgeLabels, 0, previous.size(), from1, to1,
                 result);
      }

      // last intersection (consume)
      e1 = edges.getLast();
      from1 = edgeLabelsIdx.getu(e1);
      to1 = edgeLabelsIdx.getu(e1 + 1);
      Utils.sintersectConsume(result, edgeLabels, 0, result.size(), from1, to1,
              consumer);

      IntArrayListPool.instance().reclaimObject(previous);
      IntArrayListPool.instance().reclaimObject(result);

   }

   @Override
   public void forEachEdge(int u, int v, IntConsumer consumer) {
      int startIdxu = vertexNeighborhoodIdx.getu(u);
      int endIdxu = vertexNeighborhoodIdx.getu(u + 1);
      int startIdxv = vertexNeighborhoodIdx.getu(v);
      int endIdxv = vertexNeighborhoodIdx.getu(v + 1);

      if ((endIdxu - startIdxu) < (endIdxv - startIdxv)) {
         forEachEdgeId(u, v, startIdxu, endIdxu, consumer);
      } else {
         forEachEdgeId(v, u, startIdxv, endIdxv, consumer);
      }
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
      int numIntersectionVertices = intersectionVertexIdxs.size();
      int numDifferenceVertices = differenceVertexIdxs.size();
      int numVertices = numIntersectionVertices + numDifferenceVertices;
      if (numIntersectionVertices == 0) return;

      starts.clear();
      ends.clear();
      result.clear();

      for (int i = 0; i < numIntersectionVertices; ++i) {
         int u = subgraph.getVertices().getu(intersectionVertexIdxs.getu(i));
         int startIdx = vertexNeighborhoodIdx.getu(u);
         int endIdx = vertexNeighborhoodIdx.getu(u + 1);
         startIdx = vertexLowerBound == Integer.MIN_VALUE ? startIdx :
                 vertexNeighborhoods
                         .binarySearch(vertexLowerBound, startIdx, endIdx);
         startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx;
         endIdx = vertexUpperBound == Integer.MAX_VALUE ? endIdx :
                 vertexNeighborhoods
                         .binarySearch(vertexUpperBound, startIdx, endIdx);
         endIdx = (endIdx < 0) ? (-endIdx - 1) : endIdx;

         starts.add(startIdx);
         ends.add(endIdx);

         if (startIdx >= endIdx) return;
      }

      if (Configuration.INSTRUMENTATION_ENABLED) {
         IntArrayList extensionCandidates =
                 IntArrayListPool.instance().createObject();
         for (int i = 0; i < subgraph.getNumVertices(); ++i) {
            extensionCandidates.add(-1);
         }
         for (int i = 0; i < numIntersectionVertices; ++i) {
            extensionCandidates.setu(intersectionVertexIdxs.getu(i),
                    ends.getu(i) - starts.getu(i));
         }
         computation.addExpansionNeighborhood(extensionCandidates);
         IntArrayListPool.instance().reclaimObject(extensionCandidates);
      }

      for (int i = 0; i < numDifferenceVertices; ++i) {
         int u = subgraph.getVertices().getu(differenceVertexIdxs.getu(i));
         int startIdx = vertexNeighborhoodIdx.getu(u);
         int endIdx = vertexNeighborhoodIdx.getu(u + 1);
         startIdx = vertexLowerBound == Integer.MIN_VALUE ? startIdx :
                 vertexNeighborhoods
                         .binarySearch(vertexLowerBound, startIdx, endIdx);
         startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx;
         endIdx = vertexUpperBound == Integer.MAX_VALUE ? endIdx :
                 vertexNeighborhoods
                         .binarySearch(vertexUpperBound, startIdx, endIdx);
         endIdx = (endIdx < 0) ? (-endIdx - 1) : endIdx;

         starts.add(startIdx);
         ends.add(endIdx);
      }

      int vertexCandidate = Integer.MIN_VALUE;
      int i;
      while (true) {
         for (i = 0; i < numIntersectionVertices; ++i) {
            // ensure >= vertexCandidate
            int startIdx = starts.getu(i);
            int endIdx = ends.getu(i);

            int v = Integer.MIN_VALUE;
            for (; startIdx < endIdx; ++startIdx) {
               v = vertexNeighborhoods.getu(startIdx);
               if (v >= vertexCandidate) break;
            }

            if (startIdx == endIdx) {
               return;
            } else if (v > vertexCandidate) {
               vertexCandidate = v;
               starts.setu(i, startIdx);
               i = -1; // start again, new vertexCandidate found
            } else { // vertexCandidate == v
               starts.setu(i, startIdx);
            }
         }

         // check vertexCandidate is not in the differences
         for (i = numIntersectionVertices; i < numVertices; ++i) {
            int startIdx = starts.getu(i);
            int endIdx = ends.getu(i);

            if (startIdx >= endIdx) continue;

            int v = Integer.MIN_VALUE;
            for (; startIdx < endIdx; ++startIdx) {
               v = vertexNeighborhoods.getu(startIdx);
               if (v >= vertexCandidate) break;
            }

            starts.setu(i, startIdx);

            if (v == vertexCandidate) break;
         }

         if (i == numVertices && vpred.test(vertexCandidate)) {
            result.add(vertexCandidate);
         }

         for (i = 0; i < numIntersectionVertices; ++i) starts.increment(i, 1);
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
      int numIntersectionVertices = intersectionVertexIdxs.size();
      int numDifferenceVertices = differenceVertexIdxs.size();
      int numVertices = numIntersectionVertices + numDifferenceVertices;
      if (numIntersectionVertices == 0) return;

      starts.clear();
      ends.clear();
      result.clear();

      for (int i = 0; i < numIntersectionVertices; ++i) {
         int u = subgraph.getVertices().getu(intersectionVertexIdxs.getu(i));
         int startIdx = vertexNeighborhoodIdx.getu(u);
         int endIdx = vertexNeighborhoodIdx.getu(u + 1);
         startIdx = vertexLowerBound == Integer.MIN_VALUE ? startIdx :
                 vertexNeighborhoods
                         .binarySearch(vertexLowerBound, startIdx, endIdx);
         startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx;
         endIdx = vertexUpperBound == Integer.MAX_VALUE ? endIdx :
                 vertexNeighborhoods
                         .binarySearch(vertexUpperBound, startIdx, endIdx);
         endIdx = (endIdx < 0) ? (-endIdx - 1) : endIdx;

         starts.add(startIdx);
         ends.add(endIdx);

         if (startIdx >= endIdx) return;
      }

      if (Configuration.INSTRUMENTATION_ENABLED) {
         IntArrayList extensionCandidates =
                 IntArrayListPool.instance().createObject();
         for (int i = 0; i < subgraph.getNumVertices(); ++i) {
            extensionCandidates.add(-1);
         }
         for (int i = 0; i < numIntersectionVertices; ++i) {
            extensionCandidates.setu(intersectionVertexIdxs.getu(i),
                    ends.getu(i) - starts.getu(i));
         }
         computation.addExpansionNeighborhood(extensionCandidates);
         IntArrayListPool.instance().reclaimObject(extensionCandidates);
      }

      for (int i = 0; i < numDifferenceVertices; ++i) {
         int u = subgraph.getVertices().getu(differenceVertexIdxs.getu(i));
         int startIdx = vertexNeighborhoodIdx.getu(u);
         int endIdx = vertexNeighborhoodIdx.getu(u + 1);
         startIdx = vertexLowerBound == Integer.MIN_VALUE ? startIdx :
                 vertexNeighborhoods
                         .binarySearch(vertexLowerBound, startIdx, endIdx);
         startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx;
         endIdx = vertexUpperBound == Integer.MAX_VALUE ? endIdx :
                 vertexNeighborhoods
                         .binarySearch(vertexUpperBound, startIdx, endIdx);
         endIdx = (endIdx < 0) ? (-endIdx - 1) : endIdx;

         starts.add(startIdx);
         ends.add(endIdx);
      }

      int vertexCandidate = Integer.MIN_VALUE;
      int i;
      while (true) {
         for (i = 0; i < numIntersectionVertices; ++i) {
            // ensure >= vertexCandidate
            int startIdx = starts.getu(i);
            int endIdx = ends.getu(i);

            int v = Integer.MIN_VALUE;
            for (; startIdx < endIdx; ++startIdx) {
               v = vertexNeighborhoods.getu(startIdx);
               if (v >= vertexCandidate) break;
            }

            if (startIdx == endIdx) {
               return;
            } else if (v > vertexCandidate) {
               vertexCandidate = v;
               starts.setu(i, startIdx);
               i = -1; // start again, new vertexCandidate found
            } else { // vertexCandidate == v
               starts.setu(i, startIdx);
            }
         }

         // check vertexCandidate is not in the differences
         for (i = numIntersectionVertices; i < numVertices; ++i) {
            int startIdx = starts.getu(i);
            int endIdx = ends.getu(i);

            if (startIdx >= endIdx) continue;

            int v = Integer.MIN_VALUE;
            for (; startIdx < endIdx; ++startIdx) {
               v = vertexNeighborhoods.getu(startIdx);
               if (v >= vertexCandidate) break;
            }

            starts.setu(i, startIdx);

            if (v == vertexCandidate) break;
         }

         if (i == numVertices) result.add(vertexCandidate);

         for (i = 0; i < numIntersectionVertices; ++i) starts.increment(i, 1);
      }
   }

   @Override
   public void validExtensionsEdgeInduced(Computation computation,
                                          Subgraph subgraph,
                                          IntCollection validExtensions) {
      IntArrayList vertices = subgraph.getVertices();
      IntArrayList edges = subgraph.getEdges();
      int numVertices = vertices.size();
      int numEdges = edges.size();
      int lowerBound = edges.getu(0);
      int firstEdge = lowerBound;
      int currVertexIdx = numVertices - 1;

      if (Configuration.INSTRUMENTATION_ENABLED) {
         IntArrayList extensionCandidates =
                 IntArrayListPool.instance().createObject();
         for (int i = 0; i < numEdges; ++i) extensionCandidates.add(0);

         for (int i = numEdges - 1; i >= 0; --i) {
            int e = edges.getu(i);
            int numVerticesAddedWithEdge = subgraph.numVerticesAdded(i);

            for (int k = 0; k < numVerticesAddedWithEdge; ++k) {
               int u = vertices.getu(currVertexIdx);

               int startIdx = vertexNeighborhoodIdx.getu(u);
               int endIdx = vertexNeighborhoodIdx.getu(u + 1);
               startIdx = edgeNeighborhoods
                       .binarySearch(firstEdge, startIdx, endIdx);
               startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx;
               extensionCandidates.setu(i, endIdx - startIdx);
               for (int j = startIdx; j < endIdx; ++j) {
                  int w = edgeNeighborhoods.getu(j);
                  if (w > lowerBound) validExtensions.add(w);
                  else validExtensions.removeInt(w);
               }
               --currVertexIdx;
            }

            lowerBound = Math.max(e, lowerBound);
         }

         computation.addExpansionNeighborhood(extensionCandidates);
         IntArrayListPool.instance().reclaimObject(extensionCandidates);
      } else {
         for (int i = numEdges - 1; i >= 0; --i) {
            int e = edges.getu(i);
            int numVerticesAddedWithEdge = subgraph.numVerticesAdded(i);

            for (int k = 0; k < numVerticesAddedWithEdge; ++k) {
               int u = vertices.getu(currVertexIdx);

               int startIdx = vertexNeighborhoodIdx.getu(u);
               int endIdx = vertexNeighborhoodIdx.getu(u + 1);
               startIdx = edgeNeighborhoods
                       .binarySearch(firstEdge, startIdx, endIdx);
               startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx;
               for (int j = startIdx; j < endIdx; ++j) {
                  int w = edgeNeighborhoods.getu(j);
                  if (w > lowerBound) validExtensions.add(w);
                  else validExtensions.removeInt(w);
               }
               --currVertexIdx;
            }

            lowerBound = Math.max(e, lowerBound);
         }
      }
   }

   @Override
   public void validExtensionsVertexInduced(Computation computation,
                                            Subgraph subgraph,
                                            IntCollection validExtensions) {
      IntArrayList vertices = subgraph.getVertices();
      int numVertices = vertices.size();
      int lowerBound = vertices.getu(0);
      int firstVertex = lowerBound;

      if (Configuration.INSTRUMENTATION_ENABLED) {
         IntArrayList extensionCandidates =
                 IntArrayListPool.instance().createObject();
         IntSet uniqueExtensionCandidates =
                 IntSetPool.instance().createObject();
         for (int i = 0; i < numVertices; ++i) extensionCandidates.add(0);
         for (int i = numVertices - 1; i >= 0; --i) {
            int u = vertices.getu(i);
            int startIdx = vertexNeighborhoodIdx.getu(u);
            int endIdx = vertexNeighborhoodIdx.getu(u + 1);
            startIdx = vertexNeighborhoods
                    .binarySearch(firstVertex, startIdx, endIdx);
            startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx;
            extensionCandidates.setu(i, endIdx - startIdx);
            for (int j = startIdx; j < endIdx; ++j) {
               int v = vertexNeighborhoods.getu(j);
               uniqueExtensionCandidates.add(v);
               if (v > lowerBound) validExtensions.add(v);
               else validExtensions.removeInt(v);
            }
            lowerBound = Math.max(u, lowerBound);
         }
         computation.addExpansionNeighborhood(extensionCandidates);
         computation.addExtensionUniqueCandidates(uniqueExtensionCandidates.size());
         IntArrayListPool.instance().reclaimObject(extensionCandidates);
         IntSetPool.instance().reclaimObject(uniqueExtensionCandidates);
      } else {
         for (int i = numVertices - 1; i >= 0; --i) {
            int u = vertices.getu(i);
            int startIdx = vertexNeighborhoodIdx.getu(u);
            int endIdx = vertexNeighborhoodIdx.getu(u + 1);
            startIdx = vertexNeighborhoods
                    .binarySearch(firstVertex, startIdx, endIdx);
            startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx;
            for (int j = startIdx; j < endIdx; ++j) {
               int v = vertexNeighborhoods.getu(j);
               if (v > lowerBound) validExtensions.add(v);
               else validExtensions.removeInt(v);
            }
            lowerBound = Math.max(u, lowerBound);
         }
      }
   }

   @Override
   public IntArrayListView neighborhoodVertices(int u) {
      int from = vertexNeighborhoodIdx.getu(u);
      int to = vertexNeighborhoodIdx.getu(u + 1);
      return vertexNeighborhoods.view(from, to);
   }

   @Override
   public void neighborhoodVertices(int u, IntArrayListView view) {
      int from = vertexNeighborhoodIdx.getu(u);
      int to = vertexNeighborhoodIdx.getu(u + 1);
      view.set(vertexNeighborhoods, from, to);
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
      return vertexLabels.getu(vertexLabelsIdx.getu(u));
   }

   private void forEachEdgeId(int u, int v, int startIdx, int endIdx,
                             IntConsumer consumer) {

      int idx = vertexNeighborhoods.binarySearch(v, startIdx, endIdx);

      if (idx < startIdx || idx >= endIdx) return;

      // accept first edge (u,v) found
      consumer.accept(edgeNeighborhoods.getu(idx));

      // accept all edges (u,v) rightwards
      for (int i = idx - 1; i >= startIdx && vertexNeighborhoods.getu(i) == v;
           --i) {
         consumer.accept(edgeNeighborhoods.getu(i));
      }

      // accept all edges (u,v) leftwards
      for (int i = idx + 1; i < endIdx && vertexNeighborhoods.getu(i) == v;
           ++i) {
         consumer.accept(edgeNeighborhoods.getu(i));
      }
   }

   @Override
   public final void init(Configuration configuration) throws IOException {
      intArrayListPool = IntArrayListPool.instance();
      edgePredicate = configuration.getEdgeFilteringPredicate();
      long start = System.currentTimeMillis();

      String graphPath = configuration.getMainGraphPath();
      String metadataGraphPath = graphPath + "/metadata";
      String vlabelsGraphPath = graphPath + "/vlabels";
      String elabelsGraphPath = graphPath + "/elabels";
      String adjListsGraphPath = graphPath + "/adjlists";

      Path metadataHadoopPath;
      Path vlabelsHadoopPath;
      Path elabelsHadoopPath;
      Path adjListsHadoopPath;

      InputStream metadataIs = null;
      InputStream vlabelsIs = null;
      InputStream elabelsIs = null;
      InputStream adjListsIs = null;

      try {
         metadataHadoopPath = new Path(metadataGraphPath);
         metadataIs = getInputStream(metadataHadoopPath);
         readMetadataFromInputStream(metadataIs);

         vlabelsHadoopPath = new Path(vlabelsGraphPath);
         vlabelsIs = getInputStream(vlabelsHadoopPath);
         if (vlabelsIs != null) {
            readVertexLabelsFromInputStream(vlabelsIs);
         }

         elabelsHadoopPath = new Path(elabelsGraphPath);
         elabelsIs = getInputStream(elabelsHadoopPath);
         if (elabelsIs != null) {
            readEdgeLabelsFromInputStream(elabelsIs);
         }

         adjListsHadoopPath = new Path(adjListsGraphPath);
         adjListsIs = getInputStream(adjListsHadoopPath);
         readAdjacencyListsFromInputStream(adjListsIs);

      } finally {
         if (metadataIs != null) metadataIs.close();
         if (vlabelsIs != null) vlabelsIs.close();
         if (elabelsIs != null) elabelsIs.close();
         if (adjListsIs != null) adjListsIs.close();
      }

      LOG.info("vertexNeighborhoodIdx " + vertexNeighborhoodIdx.size());
      LOG.info("vertexNeighborhoods " + vertexNeighborhoods.size());
      LOG.info("edgeNeighborhoods " + edgeNeighborhoods.size());
      LOG.info("edgeSrcs " + edgeSrcs.size());
      LOG.info("edgeDsts " + edgeDsts.size());

      if (vertexLabelsIdx != null) {
         LOG.info("vertexLabelsIdx " + vertexLabelsIdx.size());
      }

      if (vertexLabels != null) {
         LOG.info("vertexLabels " + vertexLabels.size());
      }

      if (edgeLabelsIdx != null) {
         LOG.info("edgeLabelsIdx " + edgeLabelsIdx.size());
      }

      if (edgeLabels != null) {
         LOG.info("edgeLabels " + edgeLabels.size());
      }

      long elapsed = System.currentTimeMillis() - start;
      LOG.info("GraphReading took " + elapsed + " ms");
   }

   protected final InputStream getInputStream(org.apache.hadoop.fs.Path hdfsPath)
           throws IOException {

      FileSystem fs =
              FileSystem.get(new org.apache.hadoop.conf.Configuration());

      if (!fs.exists(hdfsPath)) return null;

      InputStream is = null;

      // single file: use provided path directly
      if (fs.isFile(hdfsPath)) {
         LOG.info("Provided path is file: " + hdfsPath);
         is = fs.open(hdfsPath);
      }

      // directory: concatenate input streams of 'part-****' files
      else {
         LOG.info("Provided path is directory: " + hdfsPath);
         ArrayList<org.apache.hadoop.fs.Path> partsPaths = new ArrayList<>();

         RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(
                 hdfsPath, false);
         while(fileStatusListIterator.hasNext()){
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            if (fileStatus.getPath().getName().contains("part-")) {
               partsPaths.add(fileStatus.getPath());
            }
         }

         LOG.info("Found the following 'part-***' files: " + partsPaths);

         // single part: use single input stream
         if (partsPaths.size() == 1) {
            is = fs.open(partsPaths.get(0));
         }

         // multiple parts: chain multiple input streams
         else {

            // make sure we consume parts in order
            partsPaths.sort((p1, p2) -> {
               int part1 = Integer.parseInt(p1.getName().split("-")[1]);
               int part2 = Integer.parseInt(p2.getName().split("-")[1]);
               return Integer.compare(part1, part2);
            });

            SequenceInputStream sis = new SequenceInputStream(
                    fs.open(partsPaths.get(0)),
                    fs.open(partsPaths.get(1))
            );

            for (int i = 2; i < partsPaths.size(); ++i) {
               sis = new SequenceInputStream(sis, fs.open(partsPaths.get(i)));
            }

            is = sis;
         }
      }

      return is;
   }

   private void readMetadataFromInputStream(InputStream is) {
      LOG.info("Reading metadata from input stream: " + is);
      try {
         TextFileParser stream = new TextFileParser(is);

         numVertices = stream.nextInt();
         numEdges = stream.nextInt();
         numValidEdges = 0;

         uLabelsView = new IntArrayListView();
         vLabelsView = new IntArrayListView();
         eLabelsView = new IntArrayListView();

      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   private void readAdjacencyListsFromInputStream(InputStream is) {
      LOG.info("Reading adjacency lists from input stream: " + is);

      vertexNeighborhoodIdx = new IntArrayList(numVertices + 1);
      vertexNeighborhoods = new IntArrayList(numEdges * 2);
      edgeNeighborhoods = new IntArrayList(numEdges * 2);
      edgeSrcs = new IntArrayList(numEdges);
      edgeDsts = new IntArrayList(numEdges);

      try {
         TextFileParser stream = new TextFileParser(is);
         int u, v, e;
         for (u = 0; u < numVertices; ++u) {
            addVertex(u);

            while (!stream.skipNewLine()) {
               // read neighbor v
               v = stream.nextInt();
               // read edge id of neighbor v
               if (stream.read() != ',') {
                  throw new RuntimeException("Invalid format, expecting edge " +
                          "id after neighbor id " + u + " " + v);
               }

               e = stream.nextInt();
               addEdge(u, v, e);
            }
         }

         // for convenience
         vertexNeighborhoodIdx.add(vertexNeighborhoods.size());

      } catch (IOException e) {
         throw new RuntimeException(e);
      }

      // sanity check
      if (vertexNeighborhoodIdx.size() != numVertices + 1
              //|| vertexNeighborhoods.size() != numEdges*2
              //|| edgeNeighborhoods.size() != numEdges*2
              || edgeSrcs.size() != numEdges
              || edgeDsts.size() != numEdges) {
         throw new RuntimeException("Issue reading adjacency lists.");
      }
   }

   protected void readVertexLabelsFromInputStream(InputStream is) {
      LOG.info("Reading vertex labels from input stream: " + is);
      vertexLabelsIdx = new IntArrayList(numVertices + 1);
      vertexLabels = new IntArrayList(numVertices); // at least
      try {
         TextFileParser stream = new TextFileParser(is);
         int u, ulabel;
         for (u = 0; u < numVertices; ++u) {
            // read labels of vertex u
            vertexLabelsIdx.add(vertexLabels.size());
            while (!stream.skipNewLine()) {
               ulabel = stream.nextInt();
               addVertexLabel(u, ulabel);
            }
         }

         // for convenience
         vertexLabelsIdx.add(vertexLabels.size());

      } catch (IOException e) {
         throw new RuntimeException(e);
      }

      // sanity check
      if (vertexLabelsIdx.size() != numVertices + 1
              || vertexLabels.size() < numVertices) {
         throw new RuntimeException("Issue reading vertex labels.");
      }
   }

   protected void readEdgeLabelsFromInputStream(InputStream is) {
      LOG.info("Reading edge labels from input stream: " + is);
      edgeLabelsIdx = new IntArrayList(numEdges + 1);
      edgeLabels = new IntArrayList(numEdges); // at least
      try {
         TextFileParser stream = new TextFileParser(is);
         int e, elabel;

         for (e = 0; e < numEdges; ++e) {
            edgeLabelsIdx.add(edgeLabels.size());
            do {
               elabel = stream.nextInt();
               addEdgeLabel(e, elabel);
            } while (stream.read() == ',');
         }

         // for convenience
         edgeLabelsIdx.add(edgeLabels.size());

      } catch (IOException e) {
         throw new RuntimeException(e);
      }

      // sanity check
      if (edgeLabelsIdx.size() != numEdges + 1
              || edgeLabels.size() < numEdges) {
         throw new RuntimeException("Issue reading edge labels.");
      }
   }


   @Override
   public final boolean isEdgeValid(int e) {
      return isEdgeValid(edgeSrcs.getu(e), edgeDsts.getu(e), e);
   }

   private final boolean isEdgeValid(int u, int v, int e) {
      if (edgePredicate == null) return true;

      IntArrayListView ulabels, vLabels, eLabels;
      ulabels = vLabels = eLabels = null;

      if (vertexLabels != null) {
         uLabelsView.set(vertexLabels, vertexLabelsIdx.getu(u),
                 vertexLabelsIdx.getu(u + 1));
         vLabelsView.set(vertexLabels, vertexLabelsIdx.getu(v),
                 vertexLabelsIdx.getu(v + 1));

         ulabels = uLabelsView;
         vLabels = vLabelsView;
      }

      if (edgeLabels != null) {
         eLabelsView.set(edgeLabels, edgeLabelsIdx.getu(e),
                 edgeLabelsIdx.getu(e + 1));
         eLabels = eLabelsView;
      }

      return edgePredicate.test(u, ulabels, v, vLabels, e, eLabels);
   }

   private class DefaultEdgePredicate extends EdgePredicate {
      @Override
      public boolean test(int e) {
         return true;
      }
   }

}
