package br.ufmg.cs.systems.fractal.subgraph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.Edge;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.*;
import br.ufmg.cs.systems.fractal.util.EdgePredicates;
import br.ufmg.cs.systems.fractal.util.SubgraphCallback;
import br.ufmg.cs.systems.fractal.util.Utils;
import br.ufmg.cs.systems.fractal.util.VertexPredicate;
import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayListView;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSet;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.io.ObjectInput;
import java.util.BitSet;
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
   private IntArrayList originalVertices;
   private IntArrayList verticesBackup;
   private ObjArrayList<IntArrayList> neighborhoods;
   private ObjArrayList<IntArrayList> neighborhoodsToReclaim;
   private IntSet verticesSet;
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
      neighborhoodsToReclaim = new ObjArrayList<>();
      originalVertices = new IntArrayList();
      verticesBackup = new IntArrayList();
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

   public Pattern applyLabels(Pattern unlabeledPattern) {
      if (dirtyPattern) {
         if (pattern == null) {
            pattern = unlabeledPattern.copy();
            pattern.init(configuration);
         }

         // fix vertex ids
         IntArrayList patternVertices = pattern.getVertices();
         for (int i = 0; i < vertices.size(); ++i) {
            patternVertices.set(i, vertices.getu(i));
         }

         // fix pattern edges, TODO: consider edge labels
         PatternEdgeArrayList patternEdges = pattern.getEdges();
         MainGraph graph = getConfig().getMainGraph();
         for (int i = 0; i < patternEdges.size(); ++i) {
            PatternEdge pedge = patternEdges.getu(i);
            pedge.setSrcLabel(graph.vertexLabel(patternVertices.getu(pedge.getSrcPos())));
            pedge.setDestLabel(graph.vertexLabel(patternVertices.getu(pedge.getDestPos())));
         }

         dirtyPattern = false;
      }

      return pattern;
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
         sb.append(vertices.getu(i));
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
         int existingVertexId = vertices.getu(i);

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

      VertexPredicate vertexPredicate = pattern.explorationPlan()
              .vertexPredicate(0);
      boolean vertexLabeled = pattern.vertexLabeled();

      for (int u = myPartitionId; u < totalNumWords; u += numPartitions) {
         if (!vertexLabeled || vertexPredicate.test(u)) {
            extensionWordIds().add(u);
         }
      }

      computation.getExecutionEngine().aggregate(
            Configuration.NEIGHBORHOOD_LOOKUPS(getNumWords()),
            extensionWordIds().size());
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
         intersection.add(vertices.getu(intersectionIdx.getu(i)));
      }

      difference.clear();
      for (int i = 0; i < differenceIdx.size(); ++i) {
         difference.add(vertices.getu(differenceIdx.getu(i)));
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
      if (neighborhoods.getu(pos) != null) return;
      /**
       * Single vertices: neighborhood view
       */
      if (pos < mcvcSize) {
         IntArrayList view = graph.neighborhoodVertices(vertices.getu(pos));
         neighborhoods.setu(pos, view);
         neighborhoodsToReclaim.add(view);
         return;
      }

      /**
       * Ensure dependencies are satisfied
       */
      PatternExplorationPlan explorationPlan = pattern.explorationPlan();
      IntArrayList intersections = explorationPlan.intersection(pos);
      for (int i = 0; i < intersections.size(); ++i) {
         ensureNeighborhoods(graph, pattern, mcvcSize, intersections.getu(i));
      }

      /**
       * In case intersections is single element, just get reference
       */
      if (intersections.size() == 1) {
         int dep = intersections.getu(0);
         neighborhoods.setu(pos, neighborhoods.getu(dep));
         return;
      }

      /**
       * In case intersections have two elements
       */
      if (intersections.size() == 2) {
         IntArrayList neighborhood1 = neighborhoods.getu(intersections.getu(0));
         IntArrayList neighborhood2 = neighborhoods.getu(intersections.getu(1));
         IntArrayList result = IntArrayListPool.instance().createObject();
         Utils.sintersect(neighborhood1, neighborhood2, 0, neighborhood1.size(), 0, neighborhood2.size(), result);
         neighborhoods.setu(pos, result);
         neighborhoodsToReclaim.add(result);
         return;
      }

      /**
       * In case intersections have more than two elements
       */
      IntArrayList result = IntArrayListPool.instance().createObject();
      IntArrayList previous = IntArrayListPool.instance().createObject();
      previous.clear();
      IntArrayList neighborhood1 = neighborhoods.getu(intersections.getu(0));
      IntArrayList neighborhood2 = neighborhoods.getu(intersections.getu(1));
      Utils.sintersect(neighborhood1, neighborhood2, 0, neighborhood1.size(), 0, neighborhood2.size(), result);

      for (int i = 2; i < intersections.size(); ++i) {
         IntArrayList aux = result;
         result = previous;
         previous = aux;
         neighborhood1 = neighborhoods.getu(intersections.getu(i));
         result.clear();
         Utils.sintersect(previous, neighborhood1, 0, previous.size(), 0,
                 neighborhood1.size(), result);
      }

      neighborhoods.setu(pos, result);
      neighborhoodsToReclaim.add(result);
      IntArrayListPool.instance().reclaimObject(previous);
   }

   public long completeMatch(Computation computation, Pattern pattern,
                             SubgraphCallback callback) {
      PatternExplorationPlan explorationPlan = pattern.explorationPlan();
      int numOrderings = explorationPlan.numOrderings();

      if (numOrderings > 1) {
         originalVertices.clear();
         originalVertices.addAll(vertices);
      }

      long validSubgraphs = completeMatch(computation, pattern,
              callback, explorationPlan,
              numOrderings, 0);

      if (numOrderings > 1) {
         vertices.clear();
         vertices.addAll(originalVertices);
      }

      computation.getExecutionEngine().addValidSubgraphs(validSubgraphs);

      return validSubgraphs;
   }

   private long completeMatch(Computation computation, Pattern pattern,
                              SubgraphCallback callback,
                              PatternExplorationPlan explorationPlan,
                              int numOrderings, int nextOrdering) {
      int numVertices = pattern.getNumberOfVertices();
      int nextVertexPos = getNumVertices();
      MainGraph graph = pattern.getConfig().getMainGraph();

      /**
       * Maybe change the minimum cover match order to reflect the new ordering
       */
      if (nextOrdering - 1 >= 0) {
         verticesBackup.clear();
         verticesBackup.addAll(vertices);
         IntArrayList previousOrdering = explorationPlan.ordering(nextOrdering - 1);
         IntArrayList ordering = explorationPlan.ordering(nextOrdering);
         for (int j = 0; j < ordering.size(); ++j) {
            vertices.setu(previousOrdering.getu(j), verticesBackup.getu(ordering.getu(j)));
         }
      }

      /**
       * Here we guarantee that the neighborhoods has proper size and all elements equal to null.
       * Also, we return existing arrays to pool
       */
      neighborhoods.clear();
      for (int i = 0; i < numVertices; ++i) {
         neighborhoods.add(null);
      }

      /**
       * Clear and maybe reclaim temporary neighborhoods
       */
      for (int i = 0; i < neighborhoodsToReclaim.size(); ++i) {
         neighborhoodsToReclaim.getu(i).reclaim();
      }
      neighborhoodsToReclaim.clear();

      /**
       * Make sure the neighborhoods that will be queried exists
       */
      for (int pos = nextVertexPos; pos < numVertices; ++pos) {
         ensureNeighborhoods(graph, pattern, nextVertexPos, pos);
      }

      /**
       * Effectively match this ordering
       */
      long validSubgraphs = completeMatchRec(computation, pattern, callback);

      if (nextOrdering >= numOrderings - 1) { // reached last ordering
         return validSubgraphs;
      } else { // accumulate with other orderings
         return validSubgraphs + completeMatch(computation, pattern,
                 callback,
                 explorationPlan,
                 numOrderings,
                 nextOrdering + 1);
      }
   }

   private long completeMatchRec(Computation computation, Pattern pattern,
                                 SubgraphCallback callback) {
      long validSubgraphs = 0;
      int numVertices = getNumVertices();
      IntArrayList validExtensions = neighborhoods.getu(numVertices);
      PatternExplorationPlan explorationPlan = pattern.explorationPlan();
      IntArrayList differences = explorationPlan.difference(numVertices);
      VertexPredicate vertexPredicate = explorationPlan
              .vertexPredicate(numVertices);

      if (differences.size() > 0) {
         MainGraph graph = computation.getConfig().getMainGraph();
         IntArrayList result = IntArrayListPool.instance().createObject();
         IntArrayList previous = IntArrayListPool.instance().createObject();
         IntArrayListView nextNeighborhood = graph
                 .neighborhoodVertices(vertices.getu(differences.getu(0)));
         Utils.sdifference(validExtensions, nextNeighborhood,
                 0, validExtensions.size(), 0, nextNeighborhood.size(),
                 result);

         for (int i = 1; i < differences.size(); ++i) {
            IntArrayList aux = result;
            result = previous;
            previous = aux;
            graph.neighborhoodVertices(vertices.getu(differences.getu(i)),
                    nextNeighborhood);
            result.clear();
            Utils.sdifference(previous, nextNeighborhood, 0, previous.size(),
                    0, nextNeighborhood.size(), result);
         }

         nextNeighborhood.reclaim();
         previous.reclaim();
         validExtensions = result;
      }

      int lowerBound = pattern.sbLowerBound(this, getNumVertices());
      int startIdx = validExtensions.binarySearch(lowerBound);
      startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx + 1;
      if (getNumVertices() < pattern.getNumberOfVertices() - 1) {
         for (; startIdx < validExtensions.size(); ++startIdx) {
            int u = validExtensions.getu(startIdx);
            if (!vertexPredicate.test(u) || vertices.contains(u)) continue;
            addWord(u);
            validSubgraphs += completeMatchRec(computation, pattern, callback);
            removeLastWord();
         }
      } else {
         for (; startIdx < validExtensions.size(); ++startIdx) {
            int u = validExtensions.getu(startIdx);
            if (!vertexPredicate.test(u) || vertices.contains(u)) continue;
            addWord(u);
            validSubgraphs += 1;
            callback.apply(this, computation);
            removeLastWord();
         }
      }

      if (differences.size() > 0) {
         validExtensions.reclaim();
      }

      return validSubgraphs;
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      reset();

      init(Configuration.get(in.readInt()));

      edges.readFields(in);

      int numEdges = edges.size();

      for (int i = 0; i < numEdges; ++i) {
         updateVertices(edges.getu(i));
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
         PatternEdge pedge = patternEdges.getu(i);
         int srcPos = pedge.getSrcPos();
         int destPos = pedge.getDestPos();
         if (destPos >= pos && destPos < numVertices) {
            int src = vertices.getu(srcPos);
            int dest = vertices.getu(destPos);
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
         PatternEdge pedge = patternEdges.getu(i);
         int srcPos = pedge.getSrcPos();
         int destPos = pedge.getDestPos();
         if (destPos <= pos && destPos < numVertices) {
            int src = vertices.getu(srcPos);
            int dest = vertices.getu(destPos);
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
