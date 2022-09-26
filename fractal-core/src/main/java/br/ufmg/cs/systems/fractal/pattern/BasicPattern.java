package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.pool.PatternEdgeThreadUnsafePool;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.ReflectionSerializationUtils;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntCollectionAddConsumer;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import br.ufmg.cs.systems.fractal.util.pool.IntSetPool;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMapFactory;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.collect.set.IntSet;
import org.apache.log4j.Logger;

import java.io.*;

public abstract class BasicPattern implements Pattern {
   private static final Logger LOG = Logger.getLogger(BasicPattern.class);
   private static final int EDGE_POOL_SIZE = 10;

   protected Configuration configuration;

   /**
    * Pattern configuration attributes
    */
   protected boolean edgeLabeled;
   protected boolean induced;
   protected boolean vertexLabeled;

   protected HashIntIntMapFactory positionMapFactory =
           HashIntIntMaps.getDefaultFactory().withDefaultValue(-1);
   protected volatile boolean dirtyVertexPositionEquivalences;
   protected volatile boolean dirtyEdgePositionEquivalences;
   protected volatile boolean dirtyCanonicalLabelling;
   protected IntCollectionAddConsumer intAddConsumer = new IntCollectionAddConsumer();

   /**
    * Pattern structure
    */
   private IntArrayList vertices;
   private PatternEdgeArrayList edges;
   private int firstVertexLabel;
   private ObjArrayList<IntArrayList> vertexPosToEdgeIndices;
   private boolean dirtyVertexPosToEdgeIndices;

   // K = vertex id, V = vertex position
   private IntIntMap vertexPositions;

   // Incremental building
   private IntArrayList previousWords; // TODO: is it previous or current ?
   private int numVerticesAddedFromPrevious;
   private int numAddedEdgesFromPrevious;

   // Isomorphisms
   private VertexPositionEquivalences vertexPositionEquivalences;
   private IntIntMap canonicalLabelling;
   private ObjArrayList<IntArrayList> vsymmetryBreakerLowerBound;
   private ObjArrayList<IntArrayList> vsymmetryBreakerUpperBound;

   // Others
   private PatternEdgeThreadUnsafePool patternEdgePool;
   private PatternExplorationPlan explorationPlan;

   public BasicPattern(BasicPattern basicPattern) {
      this();

      intAddConsumer.setCollection(vertices);
      basicPattern.vertices.forEach(intAddConsumer);

      edgeLabeled = basicPattern.edgeLabeled;
      induced = basicPattern.induced;
      vertexLabeled = basicPattern.vertexLabeled;
      firstVertexLabel = basicPattern.firstVertexLabel;

      edges = createPatternEdgeArrayList(edgeLabeled);

      //patternEdgePool = PatternEdgePool.instance(edgeLabeled);
      patternEdgePool = PatternEdgeThreadUnsafePool.instance(edgeLabeled,
              EDGE_POOL_SIZE);

      edges.ensureCapacity(basicPattern.edges.size());

      for (PatternEdge otherEdge : basicPattern.edges) {
         edges.add(createPatternEdge(otherEdge));
      }

      vertexPositions.putAll(basicPattern.vertexPositions);

   }

   public BasicPattern() {
      vertices = new IntArrayList();
      vertexPositions = positionMapFactory.newMutableMap();
      previousWords = new IntArrayList();
      induced = false;
      vertexLabeled = true;
      reset();
   }

   protected PatternEdgeArrayList createPatternEdgeArrayList(boolean areEdgesLabelled) {
      return new PatternEdgeArrayList(areEdgesLabelled, patternEdgePool);
   }

   protected PatternEdge createPatternEdge(PatternEdge otherEdge) {
      PatternEdge patternEdge = patternEdgePool.createObject();
      patternEdge.setFromOther(otherEdge);
      return patternEdge;
   }

   protected void setDirty() {
      dirtyCanonicalLabelling = true;
      dirtyVertexPositionEquivalences = true;
      dirtyEdgePositionEquivalences = true;
      dirtyVertexPosToEdgeIndices = true;
   }

   private void resetIncremental() {
      numVerticesAddedFromPrevious = 0;
      numAddedEdgesFromPrevious = 0;
      if (previousWords != null) {
         previousWords.clear();
      }
   }

   private static void vsymmetryBreakerRec(Pattern pattern, int[][] sbreaker,
                                           IntArrayList vertexLabels,
                                           IntArrayList edgeLabels,
                                           int nextLabel) {
      int numVertices = sbreaker.length;
      VertexPositionEquivalences vertexPositionEquivalences =
              pattern.getVertexPositionEquivalences(vertexLabels, edgeLabels);

      LOG.debug(String.format(
              "symmetryBreakerRec{pattern=%s,vertices=%s,vertexLabels=%s," +
                      "vertexEquivalences=%s,nextLabel=%s}\n",
              pattern, pattern.getVertices(), vertexLabels,
              vertexPositionEquivalences, nextLabel));

      IntSet equivalenceToBreakSet = null;
      for (int i = 0; i < numVertices; ++i) {
         IntSet eq = vertexPositionEquivalences.getEquivalences(i);
         if (equivalenceToBreakSet == null ||
                 eq.size() > equivalenceToBreakSet.size()) {
            equivalenceToBreakSet = eq;
         }
      }

      if (equivalenceToBreakSet.size() > 1) {
         IntArrayList equivalenceToBreak = new IntArrayList(
                 equivalenceToBreakSet.size());
         equivalenceToBreak.addAll(equivalenceToBreakSet);
         equivalenceToBreak.sort();
         IntCursor cur = equivalenceToBreak.cursor();
         cur.moveNext();
         int fixed = cur.elem();
         while (cur.moveNext()) {
            int elem = cur.elem();
            sbreaker[fixed][elem] = -1;
            sbreaker[elem][fixed] = 1;
         }

         vertexLabels.set(fixed, nextLabel);

         // recursive call
         vsymmetryBreakerRec(pattern.copy(), sbreaker, vertexLabels, edgeLabels,
                 nextLabel + 1);
      }
   }

   public int addVertex(int vertexId) {

      // this is only to be used for single-vertex patterns
      if (vertices.isEmpty()) {
         firstVertexLabel = getMainGraph().firstVertexLabel(vertexId);
      }

      int pos = vertexPositions.get(vertexId);
      if (pos == -1) {
         pos = vertices.size();
         vertices.add(vertexId);
         vertexPositions.put(vertexId, pos);
         setDirty();
      }

      return pos;
   }

   @Override
   public void addVertexStandalone(int vlabel) {
      int vertex = vertices.size();
      vertices.add(vertex);

      if (vertex == 0) {
         firstVertexLabel = vlabel;
      }
   }

   @Override
   public void addVertexStandalone() {
      int vertex = vertices.size();
      vertices.add(vertex);
   }

   /**
    * Can do incremental only if the last word is different.
    *
    * @param subgraph
    * @return
    */
   private boolean canDoIncremental(Subgraph subgraph) {
      if (subgraph.getNumWords() == 0 || previousWords.size() != subgraph.getNumWords()) {
         return false;
      }

      // Maximum we want 1 change (which we know by default that it exists in the last position).
      // so we check
      final IntArrayList words = subgraph.getWords();
      for (int i = previousWords.size() - 2; i >= 0; i--) {
         if (words.getu(i) != previousWords.getu(i)) {
            return false;
         }
      }

      return true;
   }

   protected PatternEdge createPatternEdge(int edgeId, int srcPos, int dstPos, int srcId) {
      PatternEdge patternEdge = patternEdgePool.createObject();

      patternEdge.setFromEdge(getConfig().getMainGraph(), edgeId, srcPos, dstPos, srcId);

      return patternEdge;
   }

   private void ensureCanStoreNewEdges(int numAddedEdgesFromPrevious) {
      int newNumEdges = edges.size() + numAddedEdgesFromPrevious;

      edges.ensureCapacity(newNumEdges);
   }

   private void ensureCanStoreNewVertices(int numVerticesAddedFromPrevious) {
      int newNumVertices = vertices.size() + numVerticesAddedFromPrevious;

      vertices.ensureCapacity(newNumVertices);
      vertexPositions.ensureCapacity(newNumVertices);
   }

   @Override
   public int hashCode() {
      return edges.isEmpty() ? firstVertexLabel : edges.hashCode();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      BasicPattern that = (BasicPattern) o;

      return edges.equals(that.edges);

   }

   public MainGraph getMainGraph() {
      return getConfig().getMainGraph();
   }

   @Override
   public void init(Configuration config) {
      if (config == null) return;

      configuration = config;

      edgeLabeled = config.isGraphEdgeLabeled();

      if (edges == null) {
         edges = createPatternEdgeArrayList(edgeLabeled);
      }

      if (patternEdgePool == null) {
         //patternEdgePool = PatternEdgePool.instance(edgeLabeled);
         patternEdgePool = PatternEdgeThreadUnsafePool.instance(edgeLabeled,
                 EDGE_POOL_SIZE);
      }

      if (explorationPlan != null) {
         explorationPlan.init(this);
      }
   }

   @Override
   public void reset() {
      if (vertices != null) {
         vertices.clear();
      }

      if (patternEdgePool != null) {
         patternEdgePool.reclaimObjects(edges);
      }

      if (edges != null) {
         edges.clear();
      }

      if (vertexPositions != null) {
         vertexPositions.clear();
      }

      setDirty();

      resetIncremental();
   }

   @Override
   public void setSubgraph(Subgraph subgraph) {
      //try {
         if (canDoIncremental(subgraph)) {
            setSubgraphIncremental(subgraph);
         } else {
            setSubgraphFromScratch(subgraph);
         }
      //}
      //catch (RuntimeException e) {
      //   LOG.error("subgraph: " + subgraph + " " + e);
      //   throw e;
      //}
   }

   @Override
   public int getNumberOfVertices() {
      return vertices.getSize();
   }

   @Override
   public boolean addEdge(int edgeId) {
      int srcId = getMainGraph().edgeSrc(edgeId);
      int srcPos = addVertex(srcId);
      int dstPos = addVertex(getMainGraph().edgeDst(edgeId));
      PatternEdge patternEdge = createPatternEdge(edgeId, srcPos, dstPos, srcId);
      return addEdge(patternEdge);
   }

   @Override
   public boolean addEdge(PatternEdge edge) {
      // TODO: Remove when we have directed edges
      if (edge.getSrcPos() > edge.getDestPos()) {
         edge.invert();
      }

      edges.add(edge);

      setDirty();

      return true;
   }

   @Override
   public void addEdgeStandalone(PatternEdge edge) {
      int numVertices = getNumberOfVertices();
      int src = edge.getSrcPos();
      if (src >= numVertices) throw new RuntimeException();

      int dst = edge.getDestPos();
      if (dst >= numVertices) throw new RuntimeException();

      if (src > dst) edge.invert();

      edges.add(edge);
   }

   @Override
   public int getNumberOfEdges() {
      return edges.size();
   }

   @Override
   public boolean relabel(IntIntMap labeling) {
      IntIntCursor labelingCursor = labeling.cursor();

      boolean allEqual = true;

      while (labelingCursor.moveNext()) {
         int oldPos = labelingCursor.key();
         int newPos = labelingCursor.value();

         if (oldPos != newPos) {
            allEqual = false;
         }
      }

      if (allEqual) {
         //edges.sort();
         return false;
      }

      IntArrayList oldVertices = new IntArrayList(vertices);

      for (int i = 0; i < vertices.size(); ++i) {
         int newPos = labeling.get(i);

         // If position didn't change, do nothing
         if (newPos == i) {
            continue;
         }

         int vertexId = oldVertices.getu(i);
         vertices.setu(newPos, vertexId);

         vertexPositions.put(vertexId, newPos);
      }

      for (int i = 0; i < edges.size(); ++i) {
         PatternEdge edge = edges.get(i);

         int srcPos = edge.getSrcPos();
         int dstPos = edge.getDestPos();

         int convertedSrcPos = labeling.get(srcPos);
         int convertedDstPos = labeling.get(dstPos);

         if (convertedSrcPos < convertedDstPos) {
            edge.setSrcPos(convertedSrcPos);
            edge.setDestPos(convertedDstPos);
         } else {
            // If we changed the position of source and destination due to
            // relabel, we also have to change the labels to match this
            // change.
            int tmp = edge.getSrcLabel();
            edge.setSrcPos(convertedDstPos);
            edge.setSrcLabel(edge.getDestLabel());
            edge.setDestPos(convertedSrcPos);
            edge.setDestLabel(tmp);
         }
      }

      return true;
   }

   @Override
   public boolean turnCanonical() {
      resetIncremental();
      IntIntMap canonicalLabelling = getCanonicalLabeling();
      boolean changed = relabel(canonicalLabelling);
      edges.sort();
      return changed;
   }

   @Override
   public IntArrayList getVertices() {
      return vertices;
   }

   @Override
   public PatternEdgeArrayList getEdges() {
      return edges;
   }

   @Override
   public VertexPositionEquivalences getVertexPositionEquivalences() {
      return getVertexPositionEquivalences(null, null);
   }

   @Override
   public VertexPositionEquivalences getVertexPositionEquivalences(IntArrayList vertexLabels, IntArrayList edgeLabels) {
      if (dirtyVertexPositionEquivalences) {
         synchronized (this) {
            if (dirtyVertexPositionEquivalences) {
               if (vertexPositionEquivalences == null) {
                  vertexPositionEquivalences = new VertexPositionEquivalences();
               }

               int numVertices = getNumberOfVertices();
               if (edgeLabels != null) numVertices += getNumberOfEdges();

               vertexPositionEquivalences.setNumVertices(numVertices);
               vertexPositionEquivalences.clear();

               fillVertexPositionEquivalences(vertexPositionEquivalences,
                       vertexLabels, edgeLabels);

               dirtyVertexPositionEquivalences = false;
            }
         }
      }

      return vertexPositionEquivalences;
   }

   @Override
   public IntIntMap getCanonicalLabeling() {
      if (dirtyCanonicalLabelling) {
         synchronized (this) {
            if (dirtyCanonicalLabelling) {
               if (canonicalLabelling == null) {
                  canonicalLabelling = HashIntIntMaps.newMutableMap(
                          getNumberOfVertices());
               }
               canonicalLabelling.clear();
               fillCanonicalLabelling(canonicalLabelling);
               dirtyCanonicalLabelling = false;
            }
         }
      }
      return canonicalLabelling;
   }

   @Override
   public ObjArrayList<IntArrayList> vsymmetryBreakerUpperBound() {
      if (vsymmetryBreakerUpperBound == null) {
         synchronized (this) {
            if (vsymmetryBreakerUpperBound == null) {
               updateSymmetryBreaker();
            }
         }

         LOG.debug("vsymmetryBreakerUpperBound " + vsymmetryBreakerUpperBound);
      }

      return vsymmetryBreakerUpperBound;
   }

   @Override
   public ObjArrayList<IntArrayList> vsymmetryBreakerLowerBound() {
      if (vsymmetryBreakerLowerBound == null) {
         synchronized (this) {
            if (vsymmetryBreakerLowerBound == null) {
               updateSymmetryBreaker();
            }
         }

         LOG.debug("vsymmetryBreakerLowerBound " + vsymmetryBreakerLowerBound);
      }

      return vsymmetryBreakerLowerBound;
   }

   @Override
   public void updateSymmetryBreaker() {
      int[][] symmetryBreaker = vsymmetryBreakerMatrix(vertexLabeled, edgeLabeled);
      vsymmetryBreakerLowerBound = computeVsymmetryBreakerLowerBound(symmetryBreaker);
      vsymmetryBreakerUpperBound = computeVsymmetryBreakerUpperBound(symmetryBreaker);
   }

   @Override
   public void updateSymmetryBreakerVertexUnlabeled() {
      int[][] symmetryBreaker = vsymmetryBreakerMatrix(false, false);
      vsymmetryBreakerLowerBound = computeVsymmetryBreakerLowerBound(symmetryBreaker);
      vsymmetryBreakerUpperBound = computeVsymmetryBreakerUpperBound(symmetryBreaker);
   }

   @Override
   public int sbUpperBound(Subgraph subgraph, int pos) {
      IntArrayList conditions = vsymmetryBreakerUpperBound().getu(pos);
      IntArrayList vertices = subgraph.getVertices();
      int numConditions = conditions.size();
      int upperBound = Integer.MAX_VALUE;
      for (int i = 0; i < numConditions; ++i) {
         upperBound = Math.min(upperBound, vertices.getu(conditions.get(i)));
      }
      return upperBound;
   }

   @Override
   public int sbLowerBound(Subgraph subgraph, int pos) {
      IntArrayList conditions = vsymmetryBreakerLowerBound().getu(pos);
      IntArrayList vertices = subgraph.getVertices();
      int numConditions = conditions.size();
      int lowerBound = Integer.MIN_VALUE;
      for (int i = 0; i < numConditions; ++i) {
         lowerBound = Math.max(lowerBound, vertices.getu(conditions.get(i)));
      }
      return lowerBound;
   }

   @Override
   public boolean sbValidOrdering(IntArrayList ordering) {
      int[][] sbMatrix = vsymmetryBreakerMatrix();
      for (int i = 0; i < ordering.size(); ++i) {
         int u = ordering.get(i);
         for (int j = i + 1; j < ordering.size(); ++j) {
            int v = ordering.get(j);
            // the only chance this ordering is invalid is if u > v, i.e., if it comes after
            if (sbMatrix[u][v] == 1) return false;
         }
      }
      return true;
   }

   @Override
   public boolean connectedValidOrdering(IntArrayList ordering) {
      if (ordering.size() <= 1) return true;
      boolean valid = false;

      // first edge in ordering must exist
      int ord1 = ordering.getu(0);
      int ord2 = ordering.getu(1);
      for (PatternEdge pedge : edges) {
         int src = pedge.getSrcPos();
         int dst = pedge.getDestPos();
         if ((src == ord1 && dst == ord2) || (src == ord2 && dst == ord1)) {
            valid = true;
            break;
         }
      }

      if (!valid) return false;

      // remaining vertices must connect to existing vertices in the order
      IntSet visited = IntSetPool.instance().createObject();
      visited.add(ord1);
      visited.add(ord2);
      for (int i = 2; i < ordering.size(); ++i) {
         ord1 = ordering.getu(i);
         // ensure new vertex connects to visited vertices
         valid = false;
         for (PatternEdge pedge : edges) {
            if (ord1 == pedge.getSrcPos()) {
               if (visited.contains(pedge.getDestPos())) {
                  visited.add(ord1);
                  valid = true;
                  break;
               }
            } else if (ord1 == pedge.getDestPos()) {
               if (visited.contains(pedge.getSrcPos())) {
                  visited.add(ord1);
                  valid = true;
                  break;
               }
            }
         }

         if (!valid) return false;
      }

      return true;
   }

   @Override
   public void updateSymmetryBreaker(IntArrayList ordering) {
      if (!sbValidOrdering(ordering)) {
         throw new RuntimeException("Invalid ordering according to current partial order");
      }

      int[][] sbMatrix = vsymmetryBreakerMatrix();

      for (int i = 0; i < ordering.size(); ++i) {
         int u = ordering.getu(i);
         for (int j = i + 1; j < ordering.size(); ++j) {
            int v = ordering.getu(j);
            // make u < v in the partial ordering (symmetry breaking conditions)
            sbMatrix[u][v] = -1;
            sbMatrix[v][u] = 1;
         }
      }

      vsymmetryBreakerLowerBound = computeVsymmetryBreakerLowerBound(sbMatrix);
      vsymmetryBreakerUpperBound = computeVsymmetryBreakerUpperBound(sbMatrix);
   }

   @Override
   public boolean induced() {
      return induced;
   }

   @Override
   public void setInduced(boolean induced) {
      this.induced = induced;
   }

   @Override
   public boolean vertexLabeled() {
      return vertexLabeled;
   }

   @Override
   public void setVertexLabeled(boolean vertexLabeled) {
      this.vertexLabeled = vertexLabeled;
   }

   @Override
   public void setEdgeLabeled(boolean edgeLabeled) {
      this.edgeLabeled = edgeLabeled;
   }

   @Override
   public Configuration getConfig() {
      return configuration;
   }

   @Override
   public String toString() {
      if (getNumberOfEdges() > 0) {
         StringBuffer sb = new StringBuffer("edges=[");
         IntArrayList vlabels = IntArrayListPool.instance().createObject();
         IntArrayList elabels = IntArrayListPool.instance().createObject();
         for (int v = 0; v < getNumberOfVertices(); ++v) {
            vlabels.add(-1);
         }
         for (int i = 0; i < getNumberOfEdges(); ++i) {
            PatternEdge pe = edges.getu(i);
            sb.append("(" + pe.getSrcPos() + "," + pe.getDestPos() + ")");
            if (i != getNumberOfEdges() - 1) sb.append(",");
            vlabels.setu(pe.getSrcPos(), pe.getSrcLabel());
            vlabels.setu(pe.getDestPos(), pe.getDestLabel());
            elabels.add(pe.getLabel());
         }
         sb.append("],vlabels=" + vlabels + ",elabels=" + elabels);
         return sb.toString();
      } else if (getNumberOfVertices() == 1) {
         return "O(" + firstVertexLabel + ")";
      } else {
         return "";
      }
   }

   public boolean equals(Object o, int upTo) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BasicPattern that = (BasicPattern) o;

      if (this.getNumberOfEdges() < upTo || that.getNumberOfEdges() < upTo)
         return false;

      PatternEdgeArrayList otherEdges = that.getEdges();
      for (int i = 0; i < upTo; i++) {
         if (!edges.getu(i).equals(otherEdges.getu(i)))
            return false;
      }

      return true;
   }

   public ObjArrayList<IntArrayList> computeVsymmetryBreakerUpperBound(int[][] symmetryBreaker) {
      ObjArrayList<IntArrayList> vsymmetryBreaker = new ObjArrayList<IntArrayList>(
              symmetryBreaker.length);

      for (int i = 0; i < symmetryBreaker.length; ++i) {
         vsymmetryBreaker.add(new IntArrayList());
         for (int j = 0; j < i; ++j) {
            if (symmetryBreaker[i][j] == -1) {
               vsymmetryBreaker.get(i).add(j);
            }
         }
      }

      return vsymmetryBreaker;
   }

   public ObjArrayList<IntArrayList> computeVsymmetryBreakerLowerBound(int[][] symmetryBreaker) {
      ObjArrayList<IntArrayList> vsymmetryBreaker = new ObjArrayList<IntArrayList>(
              symmetryBreaker.length);

      for (int i = 0; i < symmetryBreaker.length; ++i) {
         vsymmetryBreaker.add(new IntArrayList());
         for (int j = 0; j < i; ++j) {
            if (symmetryBreaker[i][j] == 1) {
               vsymmetryBreaker.get(i).add(j);
            }
         }
      }

      return vsymmetryBreaker;
   }

   protected abstract void fillVertexPositionEquivalences(
           VertexPositionEquivalences vertexPositionEquivalences,
           IntArrayList vertexLabels,
           IntArrayList edgeLabels);

   protected abstract void fillCanonicalLabelling(IntIntMap canonicalLabelling);

   @Override
   public void removeLastNEdges(int n) {
      int targetI = edges.size() - n;

      for (int i = edges.size() - 1; i >= targetI; --i) {
         patternEdgePool.reclaimObject(edges.remove(i));
      }
   }

   @Override
   public void removeLastNVertices(int n) {
      int targetI = vertices.size() - n;

      for (int i = vertices.size() - 1; i >= targetI; --i) {
         try {
            vertexPositions.remove(vertices.getu(i));
         } catch (IllegalArgumentException e) {
            System.err.println(e.toString());
            System.err.println("i=" + i);
            System.err.println("targetI=" + targetI);
            throw e;
         }
      }

      vertices.removeLast(n);
   }

   private void resetToPrevious() {
      removeLastNEdges(numAddedEdgesFromPrevious);
      removeLastNVertices(numVerticesAddedFromPrevious);
      setDirty();
   }

   /**
    * Reset everything and do everything from scratch.
    *
    * @param subgraph
    */
   private void setSubgraphFromScratch(Subgraph subgraph) {
      reset();

      int numEdgesInSubgraph = subgraph.getNumEdges();
      int numVerticesInSubgraph = subgraph.getNumVertices();

      if (numEdgesInSubgraph == 0 && numVerticesInSubgraph == 0) {
         return;
      }

      ensureCanStoreNewVertices(numVerticesInSubgraph);
      ensureCanStoreNewEdges(numEdgesInSubgraph);

      IntArrayList subgraphVertices = subgraph.getVertices();

      for (int i = 0; i < numVerticesInSubgraph; ++i) {
         addVertex(subgraphVertices.getu(i));
      }

      numVerticesAddedFromPrevious = subgraph.numVerticesAdded();

      IntArrayList subgraphEdges = subgraph.getEdges();

      for (int i = 0; i < numEdgesInSubgraph; ++i) {
         addEdge(subgraphEdges.getu(i));
      }

      numAddedEdgesFromPrevious = subgraph.numEdgesAdded();

      updateUsedSubgraphFromScratch(subgraph);
   }

   /**
    * Only the last word has changed, so skipped processing for the previous vertices.
    *
    * @param subgraph
    */
   private void setSubgraphIncremental(Subgraph subgraph) {
      resetToPrevious();

      numVerticesAddedFromPrevious = subgraph.numVerticesAdded();
      numAddedEdgesFromPrevious = subgraph.numEdgesAdded();

      ensureCanStoreNewVertices(numVerticesAddedFromPrevious);
      ensureCanStoreNewEdges(numAddedEdgesFromPrevious);

      IntArrayList subgraphVertices = subgraph.getVertices();
      int numVerticesInSubgraph = subgraph.getNumVertices();
      for (int i = (numVerticesInSubgraph - numVerticesAddedFromPrevious); i < numVerticesInSubgraph; ++i) {
         addVertex(subgraphVertices.getu(i));
      }

      IntArrayList subgraphEdges = subgraph.getEdges();
      int numEdgesInSubgraph = subgraph.getNumEdges();
      for (int i = (numEdgesInSubgraph - numAddedEdgesFromPrevious); i < numEdgesInSubgraph; ++i) {
         addEdge(subgraphEdges.getu(i));
      }

      updateUsedSubgraphIncremental(subgraph);
   }

   private void updateUsedSubgraphFromScratch(Subgraph subgraph) {
      previousWords.clear();

      int subgraphNumWords = subgraph.getNumWords();

      previousWords.ensureCapacity(subgraphNumWords);

      IntArrayList words = subgraph.getWords();

      for (int i = 0; i < subgraphNumWords; i++) {
         previousWords.add(words.getu(i));
      }
   }

   /**
    * By default only the last word changed.
    *
    * @param subgraph
    */
   private void updateUsedSubgraphIncremental(Subgraph subgraph) {
      previousWords.setu(previousWords.size() - 1, subgraph.getWords().getu(previousWords.size() - 1));
   }

   private int[][] vsymmetryBreakerMatrix() {
      //return vsymmetryBreakerMatrix(true, true);
      return vsymmetryBreakerMatrix(vertexLabeled, edgeLabeled);
   }

   @Override
   public IntArrayList getVertexLabels(boolean shouldConsiderVertexLabels) {
      int numVertices = getNumberOfVertices();
      IntArrayList vertexLabels = new IntArrayList(numVertices);

      for (int i = 0; i < numVertices; ++i) vertexLabels.add(1);

      if (shouldConsiderVertexLabels) {
         for (PatternEdge pedge : edges) {
            vertexLabels.set(pedge.getSrcPos(), pedge.getSrcLabel());
            vertexLabels.set(pedge.getDestPos(), pedge.getDestLabel());
         }
      }

      return vertexLabels;
   }

   @Override
   public IntArrayList getEdgeLabels(boolean shouldConsiderEdgeLabels) {
      if (shouldConsiderEdgeLabels) {
         int numEdges = getNumberOfEdges();
         IntArrayList edgeLabels = new IntArrayList(numEdges);
         for (int i = 0; i < numEdges; ++i) {
            LabelledPatternEdge lpedge = (LabelledPatternEdge) edges.get(i);
            edgeLabels.add(lpedge.getLabel());
         }
         return edgeLabels;
      } else {
         return null;
      }
   }

   private int[][] vsymmetryBreakerMatrix(boolean shouldConsiderVertexLabels,
                                          boolean shouldConsiderEdgeLabels) {
      int numVertices = getNumberOfVertices();
      int[][] symmetryBreaker = new int[numVertices][numVertices];

      IntArrayList vertexLabels = getVertexLabels(shouldConsiderVertexLabels);
      IntArrayList edgeLabels = getEdgeLabels(shouldConsiderEdgeLabels);

      //IntArrayList vertexLabels = new IntArrayList(numVertices);
      //IntCursor vertexCursor = vertices.cursor();
      //if (shouldConsiderVertexLabels) {
      //   while (vertexCursor.moveNext()) {
      //      vertexLabels.add(getConfig().getMainGraph().
      //              getVertex(vertexCursor.elem()).getVertexLabel());
      //   }
      //} else {
      //   while (vertexCursor.moveNext()) {
      //      vertexLabels.add(1);
      //   }
      //}

      int maxLabel = Integer.MIN_VALUE;
      for (int i = 0; i < vertexLabels.size(); ++i) {
         maxLabel = Math.max(maxLabel, vertexLabels.get(i));
      }

      vsymmetryBreakerRec(this.copy(), symmetryBreaker, vertexLabels,
              edgeLabels, maxLabel + 1);

      StringBuffer sb = new StringBuffer();
      sb.append("symmetryBreaker {");
      for (int i = 0; i < numVertices; ++i) {
         for (int j = 0; j < numVertices; ++j) {
            sb.append(String.format("%2s ", symmetryBreaker[i][j]));
         }
         sb.append("\n");
      }
      sb.append("}");

      LOG.debug(sb.toString());

      return symmetryBreaker;
   }

   @Override
   public void writeExternal(ObjectOutput objOutput) throws IOException {
      write(objOutput);
   }

   @Override
   public void readExternal(ObjectInput objInput) throws IOException {
      readFields(objInput);
   }

   public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeBoolean(induced);
      dataOutput.writeBoolean(vertexLabeled);
      dataOutput.writeBoolean(edgeLabeled);
      dataOutput.writeInt(edges.size());
      for (int i = 0; i < edges.size(); ++i) {
         edges.getu(i).write(dataOutput);
      }
      vertices.write(dataOutput);
      dataOutput.writeInt(firstVertexLabel);

      if (vsymmetryBreakerLowerBound != null) {
         dataOutput.writeBoolean(true);
         dataOutput.writeInt(vsymmetryBreakerLowerBound.size());
         for (int i = 0; i < vsymmetryBreakerLowerBound.size(); ++i) {
            vsymmetryBreakerLowerBound.getu(i).write(dataOutput);
         }
      } else {
         dataOutput.writeBoolean(false);
      }

      if (vsymmetryBreakerUpperBound != null) {
         dataOutput.writeBoolean(true);
         dataOutput.writeInt(vsymmetryBreakerUpperBound.size());
         for (int i = 0; i < vsymmetryBreakerUpperBound.size(); ++i) {
            vsymmetryBreakerUpperBound.getu(i).write(dataOutput);
         }
      } else {
         dataOutput.writeBoolean(false);
      }

      if (explorationPlan != null) {
         dataOutput.writeBoolean(true);
         dataOutput.writeUTF(explorationPlan.getClass().getTypeName());
         explorationPlan.write(dataOutput);
      } else {
         dataOutput.writeBoolean(false);
      }
   }

   public void readFields(DataInput dataInput) throws IOException {
      reset();

      induced = dataInput.readBoolean();
      vertexLabeled = dataInput.readBoolean();
      edgeLabeled = dataInput.readBoolean();

      if (patternEdgePool == null) {
         patternEdgePool = PatternEdgeThreadUnsafePool.instance(edgeLabeled,
                 EDGE_POOL_SIZE);
      }

      if (edges == null) {
         edges = createPatternEdgeArrayList(edgeLabeled);
      }

      int numEdges = dataInput.readInt();
      for (int i = 0; i < numEdges; ++i) {
         PatternEdge patternEdge = edges.createObject();
         patternEdge.readFields(dataInput);
         edges.add(patternEdge);
      }
      vertices.readFields(dataInput);
      firstVertexLabel = dataInput.readInt();


      for (int i = 0; i < vertices.size(); ++i) {
         vertexPositions.put(vertices.getu(i), i);
      }

      boolean hasLowerVsymmetryBreaker = dataInput.readBoolean();
      if (hasLowerVsymmetryBreaker) {
         int size = dataInput.readInt();
         vsymmetryBreakerLowerBound = new ObjArrayList<>(size);
         for (int i = 0; i < size; ++i) {
            IntArrayList conds = new IntArrayList();
            conds.readFields(dataInput);
            vsymmetryBreakerLowerBound.add(conds);
         }
      }

      boolean hasUpperVsymmetryBreaker = dataInput.readBoolean();
      if (hasUpperVsymmetryBreaker) {
         int size = dataInput.readInt();
         vsymmetryBreakerUpperBound = new ObjArrayList<>(size);
         for (int i = 0; i < size; ++i) {
            IntArrayList conds = new IntArrayList();
            conds.readFields(dataInput);
            vsymmetryBreakerUpperBound.add(conds);
         }
      }

      boolean hasExplorationPlan = dataInput.readBoolean();
      if (hasExplorationPlan) {
         try {
            explorationPlan = ReflectionSerializationUtils.newInstance(
                    (Class<? extends PatternExplorationPlan>) Class.forName(dataInput.readUTF())
            );
         } catch (ClassNotFoundException e) {
            e.printStackTrace();
         }
         explorationPlan.readFields(dataInput);
      }
   }

   @Override
   public PatternExplorationPlan explorationPlan() {
      return explorationPlan;
   }

   @Override
   public void setExplorationPlan(PatternExplorationPlan explorationPlan) {
      this.explorationPlan = explorationPlan;
   }

   @Override
   public ObjArrayList<IntArrayList> getVertexPosToEdgeIndices() {
      if (vertexPosToEdgeIndices == null) {
         vertexPosToEdgeIndices = new ObjArrayList<>(getNumberOfVertices());
      }

      if (dirtyVertexPosToEdgeIndices) {
         int numVertices = getNumberOfVertices();
         int numEdges = getNumberOfEdges();
         // reclaim old arrays
         for (int i = 0; i < vertexPosToEdgeIndices.size(); ++i) {
            vertexPosToEdgeIndices.getu(i).reclaim();
         }

         // clear mapping and construct it from scratch
         vertexPosToEdgeIndices.clear();

         for (int i = 0; i < numVertices; ++i) {
            vertexPosToEdgeIndices.add(IntArrayListPool.instance().createObject());
         }

         for (int i = 0; i < numEdges; ++i) {
            PatternEdge pedge = edges.getu(i);
            vertexPosToEdgeIndices.getu(pedge.getSrcPos()).add(i);
            vertexPosToEdgeIndices.getu(pedge.getDestPos()).add(i);
         }

         dirtyVertexPosToEdgeIndices = false;
      }

      return vertexPosToEdgeIndices;
   }

   @Override
   public void setVertexLabels(int... vlabels) {
      if (vlabels.length != getNumberOfVertices()) {
         throw new RuntimeException(
                 String.format("Found %d labels. Expected %d labels",
                         vlabels.length, getNumberOfVertices()));
      }

      setVertexLabeled(true);

      for (PatternEdge pedge : edges) {
         pedge.setSrcLabel(vlabels[pedge.getSrcPos()]);
         pedge.setDestLabel(vlabels[pedge.getDestPos()]);
      }
   }

   @Override
   public int getFirstVertexLabel() {
      return firstVertexLabel;
   }

   @Override
   public boolean edgeLabeled() {
      return edgeLabeled;
   }
}
