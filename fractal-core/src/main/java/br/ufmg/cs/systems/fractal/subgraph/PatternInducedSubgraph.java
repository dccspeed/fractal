package br.ufmg.cs.systems.fractal.subgraph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.Edge;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.pattern.PatternEdge;
import br.ufmg.cs.systems.fractal.pattern.PatternEdgeArrayList;
import br.ufmg.cs.systems.fractal.util.EdgePredicate;
import br.ufmg.cs.systems.fractal.util.EdgePredicates;
import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import com.koloboke.collect.set.hash.HashIntSet;

import java.io.DataInput;
import java.io.IOException;
import java.io.ObjectInput;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

public class PatternInducedSubgraph extends BasicSubgraph {
   // Consumers and Predicates {{
   private UpdateEdgesConsumer updateEdgesConsumer;
   private EdgeTaggerConsumer edgeTagger;
   private UpdateExtensionsConsumer updateExtensionsConsumer;
   private EdgePredicates edgePredicates;
   private VertexPredicate vertexPredicate;
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

   @Override
   protected boolean areWordsNeighbours(int wordId1, int wordId2) {
      return configuration.getMainGraph().areEdgesNeighbors(wordId1, wordId2);
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
         configuration.getMainGraph().forEachEdgeId(existingVertexId,
                 newVertexId, updateEdgesConsumer);
         addedEdges += updateEdgesConsumer.getNumAdded();
      }

      numEdgesAddedWithWord.add(addedEdges);
   }

   @Override
   public void removeLastWord() {
      if (getNumWords() == 0) {
         return;
      }

      vertices.removeLast();
      super.removeLastWord();
   }

   @Override
   public boolean isCanonicalSubgraphWithWord(int wordId) {
      return true;
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
         if (computation.filter(this, i)) {
            int vertexLabel = configuration.getMainGraph().vertexLabel(i);
            if (vertexLabel == targetLabel) {
               extensionWordIds().add(i);
            }
         }
      }

      computation.getExecutionEngine().aggregate(
            Configuration.NEIGHBORHOOD_LOOKUPS(getNumWords()),
            (endMyWordRange - startMyWordRange));
   }

   @Override
   protected void updateExtensions(Computation computation) {
      MainGraph graph = getConfig().getMainGraph();
      Pattern pattern = computation.getPattern();
      IntArrayList intersection = IntArrayListPool.instance().createObject();
      IntArrayList difference = IntArrayListPool.instance().createObject();
      int numVertices = getNumVertices();
      int vertexLabel = 1;

      // symmetry breaking condition
      int lowerBound = pattern.sbLowerBound(this, numVertices);

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
            edgePredicate.set(graph, pedge.getLabel());
            vertexLabel = pedge.getDestLabel();
         }
      }

      for (int i = 0; i < numVertices; ++i) {
         int u = vertices.getUnchecked(i);
         if (!intersection.contains(u)) difference.add(u);
      }

      HashIntSet extensionWordIds = extensionWordIds();
      extensionWordIds.clear();
      updateExtensionsConsumer.set(extensionWordIds);
      vertexPredicate.set(graph, vertexLabel);
      getConfig().getMainGraph().neighborhoodTraversal(
              intersection,
              difference,
              lowerBound,
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
   public void readExternal(ObjectInput objInput)
           throws IOException, ClassNotFoundException {
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

            configuration.getMainGraph().getEdgeIds(src, dest).
               forEach(edgeTagger);
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

            configuration.getMainGraph().getEdgeIds(src, dest).
               forEach(edgeTagger);
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

   private static class VertexPredicate implements IntPredicate {
      private MainGraph graph;
      private int vertexLabel;

      public void set(MainGraph graph, int vertexLabel) {
         this.graph = graph;
         this.vertexLabel = vertexLabel;
      }

      @Override
      public boolean test(int u) {
         return graph.vertexLabel(u) == vertexLabel;
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
