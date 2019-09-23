package br.ufmg.cs.systems.fractal.subgraph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.Edge;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.graph.VertexNeighbourhood;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.pattern.PatternEdge;
import br.ufmg.cs.systems.fractal.pattern.PatternEdgeArrayList;
import br.ufmg.cs.systems.fractal.util.Utils;
import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.set.hash.HashIntSet;

import java.util.function.IntConsumer;

import java.io.DataInput;
import java.io.IOException;
import java.io.ObjectInput;
import java.util.function.IntPredicate;

public class PatternInducedSubgraph extends BasicSubgraph {
   // Consumers and Predicates {{
   private UpdateEdgesConsumer updateEdgesConsumer;
   private EdgeTaggerConsumer edgeTagger;
   private ValidNeighborhoodPredicate validNeighborhoodPredicate;
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
   }

   @Override
   public void init(Configuration config) {
      super.init(config);
      if (config.isGraphEdgeLabelled()) {
         validNeighborhoodPredicate = new ValidNeighborhoodPredicate();
      } else {
         validNeighborhoodPredicate = new TrueNeighborhoodPredicate();
      }
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
            int vertexLabel = configuration.getMainGraph().
               getVertex(i).getVertexLabel();
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
      Pattern pattern = computation.getPattern();

      // vertices info and symmetry breaking conditions
      int numVertices = getNumVertices();
      HashIntSet extensionWordIds = extensionWordIds();
      int targetLabel = -1;
      int lowerBound = pattern.sbLowerBound(this, numVertices);

      // clear any remaining extension
      if (extensionWordIds.size() > 0) extensionWordIds.clear();

      // find out the pattern edges that should connect the next vertex
      IntArrayList neighborhoodSources =
         IntArrayListPool.instance().createObject();
      IntArrayList neighborhoodEdgeLabels =
         IntArrayListPool.instance().createObject();
      neighborhoodSources.clear();
      neighborhoodEdgeLabels.clear();
      PatternEdgeArrayList patternEdges = pattern.getEdges();
      int numPatternEdges = patternEdges.size();
      for (int i = 0; i < numPatternEdges; ++i) {
         PatternEdge pedge = patternEdges.getUnchecked(i);
         int srcPos = pedge.getSrcPos();
         int destPos = pedge.getDestPos();
         if (destPos == numVertices && srcPos < numVertices) {
            int destLabel = pedge.getDestLabel();
            if (targetLabel == -1) {
               targetLabel = destLabel;
            } else if (targetLabel != destLabel) {
               throw new RuntimeException(
                     "Invalid pattern, destination label differ" +
                     " pattern=" + pattern);
            }
            neighborhoodSources.add(srcPos);
            neighborhoodEdgeLabels.add(pedge.getLabel());
         }
      }

      // number of neighborhoods to perform a multi-way intersection
      int numNeighborhoods = neighborhoodSources.size();
      VertexNeighbourhood neighborhood = null;

      // if we are extending through a single neighborhood, we do not need to
      // perform any intersection at all
      if (numNeighborhoods == 1) {
         int srcPos = neighborhoodSources.getUnchecked(0);
         neighborhood = configuration.getMainGraph().getVertexNeighbourhood(
                  vertices.getUnchecked(srcPos));
         if (neighborhood != null) {
            IntArrayList orderedVertices = neighborhood.getOrderedVertices();
            int numOrderedVertices = orderedVertices.size();

            // (symmetry breaking): discard words less or equal to the lower
            // bound.
            int i = 0;
            for (; i < numOrderedVertices &&
                  orderedVertices.getUnchecked(i) <= lowerBound; ++i);

            // include remaining as extensions
            for (; i < numOrderedVertices; ++i) {
               int vertexId = orderedVertices.getUnchecked(i);
               int vertexLabel = configuration.getMainGraph().
                  getVertex(vertexId).getVertexLabel();

               if (vertexLabel == targetLabel) {
                  // set edge label predicate
                  validNeighborhoodPredicate.set(configuration.getMainGraph(),
                        neighborhoodEdgeLabels.getUnchecked(0),
                        vertices.getUnchecked(srcPos));

                  // edge label passes the test
                  if (validNeighborhoodPredicate.test(vertexId)) {
                     extensionWordIds.add(vertexId);
                  }
               }
            }
         }

         // clean-up and return
         IntArrayListPool.instance().reclaimObject(neighborhoodSources);
         IntArrayListPool.instance().reclaimObject(neighborhoodEdgeLabels);

         return;
      }

      // auxiliary structures for intersection
      IntArrayList orderedVertices = null;
      int i1, i2, size1, size2;
      IntArrayList intersect1 = IntArrayListPool.instance().createObject();
      IntArrayList intersect2 = IntArrayListPool.instance().createObject();

      // ni tracks the list we are intersecting
      // we start by just copying the first one
      int ni = 0;
      int srcPos = neighborhoodSources.getUnchecked(ni);
      neighborhood = configuration.getMainGraph().getVertexNeighbourhood(
            vertices.getUnchecked(srcPos));
      orderedVertices = neighborhood.getOrderedVertices();
      i1 = 0;
      size1 = orderedVertices.size();
      for (; i1 < size1 &&
            orderedVertices.getUnchecked(i1) <= lowerBound; ++i1);
      for (; i1 < size1; ++i1) {
         int vertexId = orderedVertices.getUnchecked(i1);
         int vertexLabel = configuration.getMainGraph().
            getVertex(vertexId).getVertexLabel();
         if (vertexLabel == targetLabel) {
            // set edge label predicate
            validNeighborhoodPredicate.set(configuration.getMainGraph(),
                  neighborhoodEdgeLabels.getUnchecked(ni),
                  vertices.getUnchecked(srcPos));

            // edge label passes the test
            if (validNeighborhoodPredicate.test(vertexId)) {
               intersect1.add(vertexId);
            }
         }
      }
      //intersect1.transferFrom(orderedVertices, i1, 0, size1 - i1);
      ++ni;

      // intersect the current with a new neighborhood until the last but one
      for (; ni < numNeighborhoods - 1; ++ni) {
         srcPos = neighborhoodSources.getUnchecked(ni);
         i1 = 0;
         size1 = intersect1.size();

         neighborhood = configuration.getMainGraph().getVertexNeighbourhood(
               vertices.getUnchecked(srcPos));
         orderedVertices = neighborhood.getOrderedVertices();
         i2 = 0;
         size2 = orderedVertices.size();

         for (; i2 < size2 &&
               orderedVertices.getUnchecked(i2) <= lowerBound; ++i2);

         intersect2.clear();

         // set edge label predicate
         validNeighborhoodPredicate.set(configuration.getMainGraph(),
               neighborhoodEdgeLabels.getUnchecked(ni),
               vertices.getUnchecked(srcPos));

         Utils.sintersect(intersect1, orderedVertices,
               i1, size1, i2, size2,
               intersect2,
               validNeighborhoodPredicate);

         IntArrayList tmp = intersect1;
         intersect1 = intersect2;
         intersect2 = tmp;
      }

      // the last intersection we write directly to the output set
      i1 = 0;
      size1 = intersect1.size();
      srcPos = neighborhoodSources.getUnchecked(ni);

      neighborhood = configuration.getMainGraph().getVertexNeighbourhood(
            vertices.getUnchecked(srcPos));
      orderedVertices = neighborhood.getOrderedVertices();
      i2 = 0;
      size2 = orderedVertices.size();

      for (; i2 < size2 &&
            orderedVertices.getUnchecked(i2) <= lowerBound; ++i2);
         
      // set edge label predicate
      validNeighborhoodPredicate.set(configuration.getMainGraph(),
            neighborhoodEdgeLabels.getUnchecked(ni),
            vertices.getUnchecked(srcPos));

      Utils.sintersect(intersect1, orderedVertices,
            i1, size1, i2, size2,
            extensionWordIds,
            validNeighborhoodPredicate);

      IntArrayListPool.instance().reclaimObject(neighborhoodSources);
      IntArrayListPool.instance().reclaimObject(neighborhoodEdgeLabels);
      IntArrayListPool.instance().reclaimObject(intersect1);
      IntArrayListPool.instance().reclaimObject(intersect2);

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

   private class EdgeLabelPredicate implements IntPredicate {

      private MainGraph mainGraph;
      private int targetLabel;
      private int mod = 1;
      
      public EdgeLabelPredicate setModifier(int mod) {
         this.mod = mod;
         return this;
      }

      public EdgeLabelPredicate set(MainGraph mainGraph, int targetLabel) {
         this.mainGraph = mainGraph;
         this.targetLabel = targetLabel;
         return this;
      }

      @Override
      public boolean test(int edgeId) {
         return !mainGraph.getEdge(edgeId).validateLabel(mod * targetLabel);
      }
   }

   private class TrueNeighborhoodPredicate extends ValidNeighborhoodPredicate {
      @Override
      public boolean test(int dst) {
         return true;
      }
   }

   private class ValidNeighborhoodPredicate implements IntPredicate {

      private MainGraph mainGraph;
      private int src;
      private EdgeLabelPredicate edgeLabelPredicate = new EdgeLabelPredicate();

      public ValidNeighborhoodPredicate set(MainGraph mainGraph,
            int targetLabel, int src) {
         this.mainGraph = mainGraph;
         this.src = src;
         this.edgeLabelPredicate.set(mainGraph, targetLabel);
         return this;
      }

      @Override
      public boolean test(int dst) {
         IntCollection edgeIds = mainGraph.getEdgeIds(src, dst);
         return !edgeIds.forEachWhile(
               edgeLabelPredicate.setModifier(src < dst ? 1 : -1)
               );
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
