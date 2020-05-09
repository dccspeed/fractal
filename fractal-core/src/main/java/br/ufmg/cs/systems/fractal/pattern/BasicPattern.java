package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.Edge;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.graph.Vertex;
import br.ufmg.cs.systems.fractal.pattern.pool.PatternEdgePool;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.ReflectionUtils;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntCollectionAddConsumer;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntSetPool;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.ObjCursor;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMapFactory;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.collect.set.IntSet;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.StringTokenizer;

public abstract class BasicPattern implements Pattern {
   private static final Logger LOG = Logger.getLogger(BasicPattern.class);

   protected int configurationId;
   protected Configuration configuration;
   protected boolean isGraphEdgeLabelled;
   protected boolean induced;
   protected boolean vertexLabeled;

   protected HashIntIntMapFactory positionMapFactory =
           HashIntIntMaps.getDefaultFactory().withDefaultValue(-1);
   protected volatile boolean dirtyVertexPositionEquivalences;
   protected volatile boolean dirtyEdgePositionEquivalences;
   protected volatile boolean dirtyCanonicalLabelling;
   // }}
   protected IntCollectionAddConsumer intAddConsumer = new IntCollectionAddConsumer();
   // Basic structure {{
   private IntArrayList vertices;
   private PatternEdgeArrayList edges;
   // }}
   // K = vertex id, V = vertex position
   private IntIntMap vertexPositions;
   // Incremental building {{
   private IntArrayList previousWords; // TODO: is it previous or current ?
   private int numVerticesAddedFromPrevious;
   private int numAddedEdgesFromPrevious;
   // Isomorphisms {{
   private VertexPositionEquivalences vertexPositionEquivalences;
   // }}
   private EdgePositionEquivalences edgePositionEquivalences;
   private IntIntMap canonicalLabelling;
   private ObjArrayList<IntArrayList> vsymmetryBreakerLowerBound;
   private ObjArrayList<IntArrayList> vsymmetryBreakerUpperBound;
   // Others {{
   private PatternEdgePool patternEdgePool;
   private PatternExplorationPlan explorationPlan;
   // }}

   public BasicPattern(BasicPattern basicPattern) {
      this();

      intAddConsumer.setCollection(vertices);
      basicPattern.vertices.forEach(intAddConsumer);

      isGraphEdgeLabelled = basicPattern.getConfig().isGraphEdgeLabelled();
      induced = basicPattern.induced;
      vertexLabeled = basicPattern.vertexLabeled;

      edges = createPatternEdgeArrayList(isGraphEdgeLabelled);

      patternEdgePool = PatternEdgePool.instance(isGraphEdgeLabelled);

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
      return new PatternEdgeArrayList(areEdgesLabelled);
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
   }

   private void resetIncremental() {
      numVerticesAddedFromPrevious = 0;
      numAddedEdgesFromPrevious = 0;
      if (previousWords != null) {
         previousWords.clear();
      }
   }

   private static void vsymmetryBreakerRec(Pattern pattern, int[][] sbreaker,
                                           IntArrayList vertexLabels, int nextLabel) {
      int numVertices = sbreaker.length;
      VertexPositionEquivalences vertexPositionEquivalences =
              pattern.getVertexPositionEquivalences(vertexLabels);

      LOG.info(String.format(
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
         vsymmetryBreakerRec(pattern.copy(), sbreaker, vertexLabels, --nextLabel);
      }
   }

   private static void esymmetryBreakerRec(Pattern pattern, int[][] sbreaker,
                                           IntArrayList edgeLabels, int nextLabel) {
      int numEdges = sbreaker.length;
      EdgePositionEquivalences edgePositionEquivalences =
              pattern.getEdgePositionEquivalences(edgeLabels);

      LOG.info(String.format(
              "symmetryBreakerRec{pattern=%s,vertices=%s,edgeLabels=%s," +
                      "edgeEquivalences=%s,nextLabel=%s}\n",
              pattern, pattern.getVertices(), edgeLabels,
              edgePositionEquivalences, nextLabel));

      IntSet equivalenceToBreakSet = null;
      for (int i = 0; i < numEdges; ++i) {
         IntSet eq = edgePositionEquivalences.getEquivalences(i);
         if (equivalenceToBreakSet == null || eq.size() > equivalenceToBreakSet.size()) {
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

         edgeLabels.set(fixed, nextLabel);

         // recursive call
         esymmetryBreakerRec(pattern.copy(), sbreaker, edgeLabels, --nextLabel);
      }
   }

   public int addVertex(int vertexId) {
      int pos = vertexPositions.get(vertexId);

      if (pos == -1) {
         pos = vertices.size();
         vertices.add(vertexId);
         vertexPositions.put(vertexId, pos);
         setDirty();
      }

      return pos;
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
         if (words.getUnchecked(i) != previousWords.getUnchecked(i)) {
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

   protected PatternEdge createPatternEdge(Edge edge, int srcPos, int dstPos, int srcId) {
      PatternEdge patternEdge = patternEdgePool.createObject();

      patternEdge.setFromEdge(getConfig().getMainGraph(),
              edge, srcPos, dstPos, srcId);

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

   public int[][] esymmetryBreaker() {
      int numEdges = getNumberOfEdges();
      int[][] symmetryBreaker = new int[numEdges][numEdges];
      IntArrayList edgeLabels = new IntArrayList(numEdges);

      ObjCursor<PatternEdge> edgeCursor = edges.cursor();
      while (edgeCursor.moveNext()) {
         edgeLabels.add(edgeCursor.elem().getLabel());
      }

      esymmetryBreakerRec(this.copy(), symmetryBreaker, edgeLabels, -1);

      StringBuffer sb = new StringBuffer();
      sb.append("symmetryBreaker {");
      for (int i = 0; i < numEdges; ++i) {
         for (int j = 0; j < numEdges; ++j) {
            sb.append(String.format("%2s ", symmetryBreaker[i][j]));
         }
         sb.append("\n");
      }
      sb.append("}");

      LOG.info(sb.toString());

      return symmetryBreaker;
   }

   @Override
   public int hashCode() {
      // TODO
      return //edges.isEmpty() ? mainGraph.getVertex(vertices.getUnchecked(0)).getVertexLabel() :
              edges.hashCode();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      BasicPattern that = (BasicPattern) o;

      return edges.equals(that.edges);

   }

   @Override
   public String toString() {
      return toOutputString();
   }

   public MainGraph getMainGraph() {
      return getConfig().getMainGraph();
   }

   @Override
   public void init(Configuration config) {
      configurationId = config.getId();

      configuration = config;

      isGraphEdgeLabelled = config.isGraphEdgeLabelled();

      if (edges == null) {
         edges = createPatternEdgeArrayList(isGraphEdgeLabelled);
      }

      if (patternEdgePool == null) {
         patternEdgePool = PatternEdgePool.instance(isGraphEdgeLabelled);
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
      try {
         if (canDoIncremental(subgraph)) {
            setSubgraphIncremental(subgraph);
         } else {
            setSubgraphFromScratch(subgraph);
         }
      } catch (RuntimeException e) {
         LOG.error("subgraph: " + subgraph + " " + e);
         throw e;
      }
   }

   @Override
   public int getNumberOfVertices() {
      return vertices.getSize();
   }

   @Override
   public boolean addEdge(int edgeId) {
      //Edge edge = getMainGraph().getEdge(edgeId);
      //return addEdge(edge);
      int srcId = getMainGraph().edgeSrc(edgeId);
      int srcPos = addVertex(srcId);
      int dstPos = addVertex(getMainGraph().edgeDst(edgeId));

      PatternEdge patternEdge = createPatternEdge(edgeId, srcPos, dstPos, srcId);

      return addEdge(patternEdge);
   }

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
         edges.sort();
         return false;
      }

      IntArrayList oldVertices = new IntArrayList(vertices);

      for (int i = 0; i < vertices.size(); ++i) {
         int newPos = labeling.get(i);

         // If position didn't change, do nothing
         if (newPos == i) {
            continue;
         }

         int vertexId = oldVertices.getUnchecked(i);
         vertices.setUnchecked(newPos, vertexId);

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
      return getVertexPositionEquivalences(null);
   }

   @Override
   public VertexPositionEquivalences getVertexPositionEquivalences(IntArrayList vertexLabels) {
      if (dirtyVertexPositionEquivalences) {
         synchronized (this) {
            if (dirtyVertexPositionEquivalences) {
               if (vertexPositionEquivalences == null) {
                  vertexPositionEquivalences = new VertexPositionEquivalences();
               }

               vertexPositionEquivalences.setNumVertices(getNumberOfVertices());
               vertexPositionEquivalences.clear();

               fillVertexPositionEquivalences(vertexPositionEquivalences, vertexLabels);

               dirtyVertexPositionEquivalences = false;
            }
         }
      }

      return vertexPositionEquivalences;
   }

   @Override
   public EdgePositionEquivalences getEdgePositionEquivalences() {
      return getEdgePositionEquivalences(null);
   }

   @Override
   public EdgePositionEquivalences getEdgePositionEquivalences(IntArrayList edgeLabels) {
      if (dirtyEdgePositionEquivalences) {
         synchronized (this) {
            if (dirtyEdgePositionEquivalences) {
               if (edgePositionEquivalences == null) {
                  edgePositionEquivalences = new EdgePositionEquivalences();
               }

               edgePositionEquivalences.setNumEdges(getNumberOfEdges());
               edgePositionEquivalences.clear();

               fillEdgePositionEquivalences(edgePositionEquivalences, edgeLabels);

               dirtyEdgePositionEquivalences = false;
            }
         }
      }

      return edgePositionEquivalences;
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

         LOG.info("vsymmetryBreakerUpperBound " + vsymmetryBreakerUpperBound);
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

         LOG.info("vsymmetryBreakerLowerBound " + vsymmetryBreakerLowerBound);
      }

      return vsymmetryBreakerLowerBound;
   }

   @Override
   public void updateSymmetryBreaker() {
      int[][] symmetryBreaker = vsymmetryBreakerMatrix();
      vsymmetryBreakerLowerBound = computeVsymmetryBreakerLowerBound(symmetryBreaker);
      vsymmetryBreakerUpperBound = computeVsymmetryBreakerUpperBound(symmetryBreaker);
   }

   @Override
   public int sbUpperBound(Subgraph subgraph, int pos) {
      IntArrayList conditions = vsymmetryBreakerUpperBound().getUnchecked(pos);
      IntArrayList vertices = subgraph.getVertices();
      int numConditions = conditions.size();
      int upperBound = Integer.MAX_VALUE;
      for (int i = 0; i < numConditions; ++i) {
         upperBound = Math.min(upperBound, vertices.getUnchecked(conditions.get(i)));
      }
      return upperBound;
   }

   @Override
   public int sbLowerBound(Subgraph subgraph, int pos) {
      IntArrayList conditions = vsymmetryBreakerLowerBound().getUnchecked(pos);
      IntArrayList vertices = subgraph.getVertices();
      int numConditions = conditions.size();
      int lowerBound = Integer.MIN_VALUE;
      for (int i = 0; i < numConditions; ++i) {
         lowerBound = Math.max(lowerBound, vertices.getUnchecked(conditions.get(i)));
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
      int ord1 = ordering.getUnchecked(0);
      int ord2 = ordering.getUnchecked(1);
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
         ord1 = ordering.getUnchecked(i);
         // ensure new vertex connects to visited vertices
         valid = false;
         for (PatternEdge pedge : edges) {
            if (ord1 == pedge.getSrcPos()) {
               if (visited.contains(pedge.getDestPos())) {
                  valid = true;
                  break;
               }
            } else if (ord1 == pedge.getDestPos()) {
               if (visited.contains(pedge.getSrcPos())) {
                  valid = true;
                  break;
               }
            }
            //if (visited.contains(pedge.getSrcPos()) || visited.contains(pedge.getDestPos())) {
            //   valid = true;
            //   break;
            //}
         }

         if (!valid) return false;
      }

      return true;
   }

   @Override
   public void updateSymmetryBreaker(IntArrayList ordering) {
      //if (!connectedValidOrdering(ordering) || !sbValidOrdering(ordering)) {
      if (!sbValidOrdering(ordering)) {
         throw new RuntimeException("Invalid ordering according to current partial order");
      }

      int[][] sbMatrix = vsymmetryBreakerMatrix();

      for (int i = 0; i < ordering.size(); ++i) {
         int u = ordering.getUnchecked(i);
         for (int j = i + 1; j < ordering.size(); ++j) {
            int v = ordering.getUnchecked(j);
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
   public Configuration getConfig() {
      return configuration;
   }

   @Override
   public String toOutputString() {
      if (getNumberOfEdges() > 0) {
         return StringUtils.join(edges, ",");
      } else if (getNumberOfVertices() == 1) {
         Vertex vertex = getMainGraph().getVertex(vertices.getUnchecked(0));

         return "0(" + vertex.getVertexLabel() + ")";
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
         if (!edges.getUnchecked(i).equals(otherEdges.getUnchecked(i)))
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

   private int[][] readFromInputStream(InputStream is) {
      int[][] sbreaker;
      try {
         BufferedReader reader = new BufferedReader(
                 new InputStreamReader(new BOMInputStream(is)));

         String line = reader.readLine();
         StringTokenizer tokenizer = new StringTokenizer(line);
         int numVertices = Integer.parseInt(tokenizer.nextToken());

         sbreaker = new int[numVertices][numVertices];

         line = reader.readLine();

         while (line != null) {
            tokenizer = new StringTokenizer(line);

            int pos1 = Integer.parseInt(tokenizer.nextToken());
            int pos2 = Integer.parseInt(tokenizer.nextToken());

            sbreaker[pos1][pos2] = -1;
            sbreaker[pos2][pos1] = 1;

            line = reader.readLine();
         }
      } catch (IOException e) {
         throw new RuntimeException(e);
      }

      return sbreaker;
   }

   protected abstract void fillEdgePositionEquivalences(
           EdgePositionEquivalences edgePositionEquivalences,
           IntArrayList edgeLabels);

   protected abstract void fillVertexPositionEquivalences(
           VertexPositionEquivalences vertexPositionEquivalences,
           IntArrayList vertexLabels);

   protected abstract void fillCanonicalLabelling(IntIntMap canonicalLabelling);

   @Override
   public void removeLastNEdges(int n) {
      int targetI = edges.size() - n;

      for (int i = edges.size() - 1; i >= targetI; --i) {
         patternEdgePool.reclaimObject(edges.remove(i));
      }
   }

   private void removeLastNVertices(int n) {
      int targetI = vertices.size() - n;

      for (int i = vertices.size() - 1; i >= targetI; --i) {
         try {
            vertexPositions.remove(vertices.getUnchecked(i));
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

      IntArrayList SubgraphVertices = subgraph.getVertices();

      for (int i = 0; i < numVerticesInSubgraph; ++i) {
         addVertex(SubgraphVertices.getUnchecked(i));
      }

      numVerticesAddedFromPrevious = subgraph.numVerticesAdded();

      IntArrayList SubgraphEdges = subgraph.getEdges();

      for (int i = 0; i < numEdgesInSubgraph; ++i) {
         addEdge(SubgraphEdges.getUnchecked(i));
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

      IntArrayList SubgraphVertices = subgraph.getVertices();
      int numVerticesInSubgraph = subgraph.getNumVertices();
      for (int i = (numVerticesInSubgraph - numVerticesAddedFromPrevious); i < numVerticesInSubgraph; ++i) {
         addVertex(SubgraphVertices.getUnchecked(i));
      }

      IntArrayList SubgraphEdges = subgraph.getEdges();
      int numEdgesInSubgraph = subgraph.getNumEdges();
      for (int i = (numEdgesInSubgraph - numAddedEdgesFromPrevious); i < numEdgesInSubgraph; ++i) {
         addEdge(SubgraphEdges.getUnchecked(i));
      }

      updateUsedSubgraphIncremental(subgraph);
   }

   private void updateUsedSubgraphFromScratch(Subgraph subgraph) {
      previousWords.clear();

      int SubgraphNumWords = subgraph.getNumWords();

      previousWords.ensureCapacity(SubgraphNumWords);

      IntArrayList words = subgraph.getWords();

      for (int i = 0; i < SubgraphNumWords; i++) {
         previousWords.add(words.getUnchecked(i));
      }
   }

   /**
    * By default only the last word changed.
    *
    * @param subgraph
    */
   private void updateUsedSubgraphIncremental(Subgraph subgraph) {
      previousWords.setUnchecked(previousWords.size() - 1, subgraph.getWords().getUnchecked(previousWords.size() - 1));
   }

   private int[][] vsymmetryBreakerMatrix() {
      int numVertices = getNumberOfVertices();
      int[][] symmetryBreaker = new int[numVertices][numVertices];
      IntArrayList vertexLabels = new IntArrayList(numVertices);

      IntCursor vertexCursor = vertices.cursor();
      while (vertexCursor.moveNext()) {
         vertexLabels.add(getConfig().getMainGraph().
                 getVertex(vertexCursor.elem()).getVertexLabel());
      }

      vsymmetryBreakerRec(this.copy(), symmetryBreaker, vertexLabels, -1);

      StringBuffer sb = new StringBuffer();
      sb.append("symmetryBreaker {");
      for (int i = 0; i < numVertices; ++i) {
         for (int j = 0; j < numVertices; ++j) {
            sb.append(String.format("%2s ", symmetryBreaker[i][j]));
         }
         sb.append("\n");
      }
      sb.append("}");

      LOG.info(sb.toString());

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

   @Override
   public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeBoolean(induced);
      dataOutput.writeBoolean(vertexLabeled);
      dataOutput.writeBoolean(isGraphEdgeLabelled);
      dataOutput.writeInt(configurationId);
      edges.write(dataOutput);
      vertices.write(dataOutput);

      if (vsymmetryBreakerLowerBound != null) {
         dataOutput.writeBoolean(true);
         dataOutput.writeInt(vsymmetryBreakerLowerBound.size());
         for (int i = 0; i < vsymmetryBreakerLowerBound.size(); ++i) {
            vsymmetryBreakerLowerBound.getUnchecked(i).write(dataOutput);
         }
      } else {
         dataOutput.writeBoolean(false);
      }

      if (vsymmetryBreakerUpperBound != null) {
         dataOutput.writeBoolean(true);
         dataOutput.writeInt(vsymmetryBreakerUpperBound.size());
         for (int i = 0; i < vsymmetryBreakerUpperBound.size(); ++i) {
            vsymmetryBreakerUpperBound.getUnchecked(i).write(dataOutput);
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

   @Override
   public void readFields(DataInput dataInput) throws IOException {
      reset();

      induced = dataInput.readBoolean();
      vertexLabeled = dataInput.readBoolean();
      isGraphEdgeLabelled = dataInput.readBoolean();

      //init(Configuration.get(dataInput.readInt()));
      configurationId = dataInput.readInt();
      configuration = Configuration.get(configurationId);

      if (edges == null) {
         edges = createPatternEdgeArrayList(isGraphEdgeLabelled);
      }

      if (patternEdgePool == null) {
         patternEdgePool = PatternEdgePool.instance(isGraphEdgeLabelled);
      }

      edges.readFields(dataInput);
      vertices.readFields(dataInput);

      for (int i = 0; i < vertices.size(); ++i) {
         vertexPositions.put(vertices.getUnchecked(i), i);
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
         //explorationPlan = configuration.createExplorationPlan();
         //explorationPlan = new PatternExplorationPlanMCVC();
         try {
            explorationPlan = ReflectionUtils.newInstance(
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
}
