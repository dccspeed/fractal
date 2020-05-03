package br.ufmg.cs.systems.fractal.subgraph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.Edge;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.*;
import br.ufmg.cs.systems.fractal.util.EdgePredicate;
import br.ufmg.cs.systems.fractal.util.EdgePredicates;
import br.ufmg.cs.systems.fractal.util.Utils;
import br.ufmg.cs.systems.fractal.util.VertexPredicate;
import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import com.koloboke.collect.set.hash.HashIntSet;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.io.ObjectInput;
import java.util.function.IntConsumer;

public class PatternInducedSubgraph extends BasicSubgraph {
   private static final Logger LOG = Logger.getLogger(PatternInducedSubgraph.class);

   // Consumers and Predicates {{
   private UpdateEdgesConsumer updateEdgesConsumer;
   private EdgeTaggerConsumer edgeTagger;
   private UpdateExtensionsConsumer updateExtensionsConsumer;
   private EdgePredicates edgePredicates;
   private VertexPredicate vertexPredicate;
   private IntArrayList intersection;
   private IntArrayList difference;
   private ObjArrayList<IntArrayList> neighborhoods;
   // }}

   // Edge tracking for incremental modifications {{
   private IntArrayList numEdgesAddedWithWord;
   private IntArrayList numVerticesAddedWithWord;
   // }}

   public PatternInducedSubgraph() {
      super();
      numVerticesAddedWithWord = new IntArrayList();
      numEdgesAddedWithWord = new IntArrayList();
      updateEdgesConsumer = new UpdateEdgesConsumer();
      edgeTagger = new EdgeTaggerConsumer();
      updateExtensionsConsumer = new UpdateExtensionsConsumer();
      vertexPredicate = new VertexPredicate();
      edgePredicates = new EdgePredicates();
      intersection = new IntArrayList();
      difference = new IntArrayList();
      neighborhoods = new ObjArrayList<>();
   }

   @Override
   public void init(Configuration config) {
      super.init(config);
   }

   @Override
   public void reset() {
      super.reset();
      numVerticesAddedWithWord.clear();
   }

   @Override
   public IntArrayList getWords() {
      return vertices;
   }

   @Override
   public int getNumWords() {
      return vertices.size();
   }

   @Override
   public String toOutputString() {
      StringBuilder sb = new StringBuilder();

      IntArrayList vertices = getVertices();

      for (int i = 0; i < vertices.size(); ++i) {
         sb.append(vertices.getUnchecked(i));
         sb.append(" ");
      }

      return sb.toString();
   }
   
   @Override
   public int numVerticesAdded() {
      if (vertices.isEmpty()) {
         return 0;
      }

      return 1;
   }

   @Override
   public int numEdgesAdded() {
      // TODO: get this information via pattern edges
      return 0;
   }

   /**
    * Add word and update the number of vertices in this subgraph.
    *
    * @param word
    */
   @Override
   public void addWord(int word) {
      super.addWord(word);
      vertices.add(word);
   }

   protected void updateVertices(int word) {
      final Edge edge = configuration.getMainGraph().getEdge(word);

      int numVerticesAdded = 0;

      boolean srcIsNew = false;
      boolean dstIsNew = false;

      if (!vertices.contains(edge.getSourceId())) {
         srcIsNew = true;
      }

      if (!vertices.contains(edge.getDestinationId())) {
         dstIsNew = true;
      }

      if (srcIsNew) { 
         vertices.add(edge.getSourceId());
         ++numVerticesAdded;
      }

      if (dstIsNew) {
         vertices.add(edge.getDestinationId());
         ++numVerticesAdded;
      }

      numVerticesAddedWithWord.add(numVerticesAdded);
   }

   /**
    * Updates the list of edges of this subgraph based on the addition of a
    * new vertex.
    *
    * @param newVertexId The id of the new vertex that was just added.
    */
   private void updateEdges(int newVertexId, int positionAdded) {
      IntArrayList vertices = getVertices();

      int addedEdges = 0;

      // For each vertex (except the last one added)
      for (int i = 0; i < positionAdded; ++i) {
         int existingVertexId = vertices.getUnchecked(i);

         updateEdgesConsumer.reset();
         configuration.getMainGraph().forEachEdge(existingVertexId,
                 newVertexId, updateEdgesConsumer);
         addedEdges += updateEdgesConsumer.getNumAdded();
      }

      numEdgesAddedWithWord.add(addedEdges);
   }

   @Override
   public void removeLastWord() {
      vertices.removeLast();
      super.removeLastWord();
   }

   @Override
   protected void updateInitExtensions(Computation computation) {
      Pattern pattern = computation.getPattern();
      int totalNumWords = computation.getInitialNumWords();
      int numPartitions = computation.getNumberPartitions();
      int myPartitionId = computation.getPartitionId();
      int numWordsPerPartition = Math.max(totalNumWords / numPartitions, 1);
      int startMyWordRange = myPartitionId * numWordsPerPartition;
      int endMyWordRange = startMyWordRange + numWordsPerPartition;

      int targetLabel = pattern.getEdges().getUnchecked(0).getSrcLabel();

      // If we are the last partition or our range end goes over the total
      // number of vertices, set the range end to the total number of vertices
      if (myPartitionId == numPartitions - 1 ||
            endMyWordRange > totalNumWords) {
         endMyWordRange = totalNumWords;
      }

      for (int i = startMyWordRange; i < endMyWordRange; ++i) {
         int vertexLabel = configuration.getMainGraph().vertexLabel(i);
         if (vertexLabel == targetLabel) {
            extensionWordIds().add(i);
         }
      }

      computation.getExecutionEngine().aggregate(
            Configuration.NEIGHBORHOOD_LOOKUPS(getNumWords()),
            (endMyWordRange - startMyWordRange));
   }

   @Override
   protected void updateExtensions(Computation computation) {
      int numVertices = getNumVertices();
      Pattern pattern = computation.getPattern();
      PatternExplorationPlan explorationPlan = pattern.explorationPlan();
      vertexPredicate = explorationPlan.vertexPredicate(numVertices);
      edgePredicates = explorationPlan.edgePredicates(numVertices);
      IntArrayList intersectionIdx = explorationPlan.intersection(numVertices);
      IntArrayList differenceIdx = explorationPlan.difference(numVertices);

      intersection.clear();
      for (int i = 0; i < intersectionIdx.size(); ++i) {
         intersection.add(vertices.getUnchecked(intersectionIdx.getUnchecked(i)));
      }

      difference.clear();
      for (int i = 0; i < differenceIdx.size(); ++i) {
         difference.add(vertices.getUnchecked(differenceIdx.getUnchecked(i)));
      }

      /**
       * Find the lower bound on vertex ids concerning symmetry breaking conditions
       */
      int lowerBound = pattern.sbLowerBound(this, numVertices);
      int upperBound = pattern.sbUpperBound(this, numVertices);

      /**
       * Neighborhood traversal: intersection and/or difference among neighborhoods
       */
      HashIntSet extensionWordIds = extensionWordIds();
      extensionWordIds.clear();
      updateExtensionsConsumer.set(extensionWordIds);
      getConfig().getMainGraph().neighborhoodTraversal(
              intersection,
              difference,
              lowerBound, // symmetry breaking
              upperBound,
              updateExtensionsConsumer,
              vertexPredicate,
              edgePredicates);
   }

   private void ensureNeighborhoods(MainGraph graph, Pattern pattern, int mcvcSize, int pos) {
      if (neighborhoods.getUnchecked(pos) != null) return;
      /**
       * Single vertices: neighborhood view
       */
      if (pos < mcvcSize) {
         neighborhoods.setUnchecked(pos, graph.neighborhoodVertices(vertices.getUnchecked(pos)));
         return;
      }

      /**
       * Ensure dependencies are satisfied
       */
      PatternExplorationPlan explorationPlan = pattern.explorationPlan();
      IntArrayList intersections = explorationPlan.intersection(pos);
      for (int i = 0; i < intersections.size(); ++i) {
         ensureNeighborhoods(graph, pattern, mcvcSize, intersections.getUnchecked(i));
      }

      /**
       * In case intersections is single element, just get reference
       */
      if (intersections.size() == 1) {
         int dep = intersections.getUnchecked(0);
         neighborhoods.setUnchecked(pos, neighborhoods.getUnchecked(dep));
         return;
      }

      /**
       * In case intersections have two elements
       */
      if (intersections.size() == 2) {
         IntArrayList neighborhood1 = neighborhoods.getUnchecked(intersections.getUnchecked(0));
         IntArrayList neighborhood2 = neighborhoods.getUnchecked(intersections.getUnchecked(1));
         IntArrayList result = IntArrayListPool.instance().createObject();
         Utils.sintersect(neighborhood1, neighborhood2, 0, neighborhood1.size(), 0, neighborhood2.size(), result);
         neighborhoods.setUnchecked(pos, result);
         return;
      }

      /**
       * In case intersections have more than two elements
       */
      IntArrayList result = IntArrayListPool.instance().createObject();
      IntArrayList previous = IntArrayListPool.instance().createObject();
      IntArrayList neighborhood1 = neighborhoods.getUnchecked(intersections.getUnchecked(0));
      IntArrayList neighborhood2 = neighborhoods.getUnchecked(intersections.getUnchecked(1));
      Utils.sintersect(neighborhood1, neighborhood2, 0, neighborhood1.size(), 0, neighborhood2.size(), result);

      for (int i = 2; i < intersections.size(); ++i) {
         IntArrayList aux = result;
         result = previous;
         previous = aux;
         neighborhood1 = neighborhoods.getUnchecked(intersections.getUnchecked(i));
         Utils.sintersect(previous, neighborhood1, 0, previous.size(), 0, neighborhood1.size(), result);
      }

      neighborhoods.setUnchecked(pos, result);
      IntArrayListPool.instance().reclaimObject(previous);
   }

   public long completeMatch(Pattern pattern) {
      /**
       * Declarations
       */
      int numVertices = pattern.getNumberOfVertices();
      int nextVertexPos = getNumVertices();
      MainGraph graph = pattern.getConfig().getMainGraph();

      /**
       * Here we guarantee that the neighborhoods has proper size and all elements equal to null.
       * Also, we return existing arrays to pool
       */
      neighborhoods.clear();
      for (int i = 0; i < numVertices; ++i) {
         IntArrayList existing = neighborhoods.getUnchecked(i);
         if (existing != null) existing.reclaim();
         neighborhoods.add(null);
      }

      /**
       * Make sure the neighborhoods that will be queried exists
       */
      for (int pos = nextVertexPos; pos < numVertices; ++pos) {
         ensureNeighborhoods(graph, pattern, nextVertexPos, pos);
      }

      return completeMatchRec(pattern);
   }

   private long completeMatchRec(Pattern pattern) {
      long validSubgraphs = 0;
      int numVertices = getNumVertices();
      IntArrayList validExtensions = neighborhoods.getUnchecked(numVertices);
      int lowerBound = pattern.sbLowerBound(this, getNumVertices());
      int startIdx = validExtensions.binarySearch(lowerBound);
      startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx + 1;
      if (getNumVertices() < pattern.getNumberOfVertices() - 1) {
         for (; startIdx < validExtensions.size(); ++startIdx) {
            int u = validExtensions.getUnchecked(startIdx);
            if (vertices.contains(u)) continue;
            addWord(u);
            validSubgraphs += completeMatchRec(pattern);
            removeLastWord();
         }
      } else {
         for (; startIdx < validExtensions.size(); ++startIdx) {
            int u = validExtensions.getUnchecked(startIdx);
            if (vertices.contains(u)) continue;
            addWord(u);
            validSubgraphs += 1;
            // callback
            removeLastWord();
         }
      }

      return validSubgraphs;
   }

   protected void updateExtensions2(Computation computation) {
      MainGraph graph = getConfig().getMainGraph();
      Pattern pattern = computation.getPattern();
      IntArrayList intersection = IntArrayListPool.instance().createObject();
      IntArrayList difference = IntArrayListPool.instance().createObject();
      int numVertices = getNumVertices();
      int vertexLabel = 1;

      /**
       * Find the lower bound on vertex ids concerning symmetry breaking conditions
       */
      int lowerBound = pattern.sbLowerBound(this, numVertices);
      int upperBound = pattern.sbUpperBound(this, numVertices);

      /**
       * Find which edges must be added in this step
       */
      PatternEdgeArrayList patternEdges = pattern.getEdges();
      for (int i = 0; i < patternEdges.size(); ++i) {
         PatternEdge pedge = patternEdges.getUnchecked(i);
         int srcPos = pedge.getSrcPos();
         int destPos = pedge.getDestPos();
         if (destPos == numVertices && srcPos < numVertices) {
            intersection.add(vertices.getUnchecked(srcPos));
            EdgePredicate edgePredicate = null;
            if (edgePredicates.size() == intersection.size() - 1) {
               edgePredicate = new EdgePredicate();
               edgePredicates.add(edgePredicate);
            } else {
               edgePredicate = edgePredicates.get(intersection.size() - 1);
            }
            edgePredicate.setGraph(graph);
            edgePredicate.setLabel(pedge.getLabel());
            vertexLabel = pedge.getDestLabel();
         }
      }

      /**
       * In case this pattern is induced, we must include not existing edge sources to the difference set
       */
      if (pattern.induced()) {
         for (int i = 0; i < numVertices; ++i) {
            int u = vertices.getUnchecked(i);
            if (!intersection.contains(u)) difference.add(u);
         }
      }

      /**
       * Neighborhood traversal: intersection and/or difference among neighborhoods
       */
      HashIntSet extensionWordIds = extensionWordIds();
      extensionWordIds.clear();
      updateExtensionsConsumer.set(extensionWordIds);
      vertexPredicate.setGraph(graph);
      vertexPredicate.setLabel(vertexLabel);
      getConfig().getMainGraph().neighborhoodTraversal(
              intersection,
              difference,
              lowerBound, // symmetry breaking
              upperBound,
              updateExtensionsConsumer,
              vertexPredicate,
              edgePredicates);

      IntArrayListPool.instance().reclaimObject(intersection);
      IntArrayListPool.instance().reclaimObject(difference);
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      reset();

      init(Configuration.get(in.readInt()));

      edges.readFields(in);

      int numEdges = edges.size();

      for (int i = 0; i < numEdges; ++i) {
         updateVertices(edges.getUnchecked(i));
      }
   }

   @Override
   public void readExternal(ObjectInput objInput) throws IOException {
      readFields(objInput);
   }

  @Override
   public void applyTagFrom(Computation computation,
         AtomicBitSetArray vtag, AtomicBitSetArray etag, int pos) {
      PatternEdgeArrayList patternEdges = computation.getPattern().getEdges();
      int numPatternEdges = patternEdges.size();
      int numVertices = vertices.size();

      edgeTagger.setEtag(etag);

      for (int i = 0; i < numPatternEdges; ++i) {
         PatternEdge pedge = patternEdges.getUnchecked(i);
         int srcPos = pedge.getSrcPos();
         int destPos = pedge.getDestPos();
         if (destPos >= pos && destPos < numVertices) {
            int src = vertices.getUnchecked(srcPos);
            int dest = vertices.getUnchecked(destPos);
            vtag.insert(src);
            vtag.insert(dest);

            configuration.getMainGraph().forEachEdge(src, dest, edgeTagger);
         }
      }
   }
   
   @Override
   public void applyTagTo(Computation computation,
         AtomicBitSetArray vtag, AtomicBitSetArray etag, int pos) {

      PatternEdgeArrayList patternEdges = computation.getPattern().getEdges();
      int numPatternEdges = patternEdges.size();
      int numVertices = vertices.size();

      edgeTagger.setEtag(etag);

      for (int i = 0; i < numPatternEdges; ++i) {
         PatternEdge pedge = patternEdges.getUnchecked(i);
         int srcPos = pedge.getSrcPos();
         int destPos = pedge.getDestPos();
         if (destPos <= pos && destPos < numVertices) {
            int src = vertices.getUnchecked(srcPos);
            int dest = vertices.getUnchecked(destPos);
            vtag.insert(src);
            vtag.insert(dest);

            configuration.getMainGraph().forEachEdge(src, dest, edgeTagger);
         }
      }
   }

   private class UpdateExtensionsConsumer implements IntConsumer {
      private HashIntSet extensionWordIds;

      public void set(HashIntSet extensionWordIds) {
         this.extensionWordIds = extensionWordIds;
      }

      @Override
      public void accept(int u) {
         extensionWordIds.add(u);
      }
   }

   private class UpdateEdgesConsumer implements IntConsumer {
      private int numAdded;

      public void reset() {
         numAdded = 0;
      }

      public int getNumAdded() {
         return numAdded;
      }

      @Override
      public void accept(int i) {
         edges.add(i);
         ++numAdded;
      }
   }

   private class EdgeTaggerConsumer implements IntConsumer {
      AtomicBitSetArray etag;

      public EdgeTaggerConsumer setEtag(AtomicBitSetArray etag) {
         this.etag = etag;
         return this;
      }

      @Override
      public void accept(int i) {
         etag.insert(i);
      }
   }
}
