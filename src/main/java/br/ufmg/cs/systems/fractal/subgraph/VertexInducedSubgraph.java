package br.ufmg.cs.systems.fractal.subgraph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.VertexNeighbourhood;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.set.hash.HashIntSet;
import java.util.function.IntConsumer;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.io.ObjectInput;

public class VertexInducedSubgraph extends BasicSubgraph {
   private static final Logger LOG = Logger.getLogger(VertexInducedSubgraph.class);
   // Consumers {{
   private UpdateEdgesConsumer updateEdgesConsumer;
   // }}

   // Edge tracking for incremental modifications {{
   private IntArrayList numEdgesAddedWithWord;
   // }}

   private ValidWordIdAdder extensionWordIdsAdder = new ValidWordIdAdder();
   
   private ValidWordIdAdderLast lastExtensionWordIdsAdder =
      new ValidWordIdAdderLast();
   
   private ValidWordIdAdderPrevious previousExtensionWordIdsAdder =
      new ValidWordIdAdderPrevious();

   private IntWritable reusableInt = new IntWritable();

   private int lastPositionAdded = -1;

   public VertexInducedSubgraph() {
      super();
      updateEdgesConsumer = new UpdateEdgesConsumer();
      numEdgesAddedWithWord = new IntArrayList();
   }

   @Override
   public void init(Configuration config) {
      super.init(config);
   }

   @Override
   public void reset() {
      super.reset();
      numEdgesAddedWithWord.clear();
   }

   @Override
   public IntArrayList getWords() {
      return getVertices();
   }

   @Override
   public int getLastWord() {
      return vertices.getLast();
   }

   @Override
   public int getNumWords() {
      return getNumVertices();
   }
   
   @Override
   public Pattern getPattern() {
      ensureEdges();
      return super.getPattern();
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
   public int getNumVerticesAddedWithExpansion() {
      if (vertices.isEmpty()) {
         return 0;
      }

      return 1;
   }

   @Override
   public int getNumEdgesAddedWithExpansion() {
      ensureEdges();
      return numEdgesAddedWithWord.getLastOrDefault(0);
   }

   private void removeExtraEdges() {
      while (lastPositionAdded >= vertices.size()) {
         int numEdgesToRemove = numEdgesAddedWithWord.pop();
         edges.removeLast(numEdgesToRemove);
         lastPositionAdded--;
      }
   }

   private void ensureEdges() {
      while (lastPositionAdded + 1 < vertices.size()) {
         updateEdges(vertices.get(lastPositionAdded + 1),
               lastPositionAdded + 1);
      }
   }

   protected IntCollection getValidNeighboursForExpansion(int vertexId) {
      return configuration.getMainGraph().getVertexNeighbours(vertexId);
   }

   @Override
   protected boolean areWordsNeighbours(int wordId1, int wordId2) {
      return configuration.getMainGraph().isNeighborVertex(wordId1, wordId2);
   }

   @Override
   public void addWord(int word) {
      super.addWord(word);
      vertices.addUnchecked(word);
      neighborhoodCuts.add(-1);
   }

   @Override
   public void removeLastWord() {
      if (getNumVertices() == 0) {
         return;
      }

      vertices.removeLast();
      neighborhoodCuts.removeLast();

      removeExtraEdges();

      super.removeLastWord();
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      reset();

      init(Configuration.get(in.readInt()));

      vertices.readFields(in);

      int numVertices = vertices.size();

      for (int i = 0; i < numVertices; ++i) {
         updateEdges(vertices.getUnchecked(i), i);
      }
   }

   @Override
   public void readExternal(ObjectInput objInput)
           throws IOException, ClassNotFoundException {
      readFields(objInput);
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

      lastPositionAdded = positionAdded;
   }

   @Override
   public boolean isCanonicalSubgraphWithWord(int wordId) {
      return true;
   }

   //@Override
   //protected void updateExtensibleWordIdsSimple(Computation computation) {
   //   IntArrayList vertices = getVertices();
   //   int numVertices = getNumVertices();

   //   extensionWordIds().clear();

   //   int wordId;
   //   int lowerBound = vertices.getUnchecked(0);
   //   IntCollection neighbourhood = null;

   //   for (int i = numVertices - 1; i >= 0; --i) {
   //      wordId = vertices.getUnchecked(i);
   //      neighbourhood = getValidNeighboursForExpansion(wordId);

   //      if (neighbourhood != null) {
   //         neighbourhood.forEach(extensionWordIdsAdder.setBound(lowerBound));
   //      }

   //      lowerBound = Math.max(wordId, lowerBound);
   //   }
   //}

   @Override
   protected void updateExtensibleWordIdsSimple(Computation computation) {
      IntArrayList vertices = getVertices();
      int numVertices = getNumVertices();
      HashIntSet extensionWordIds = extensionWordIds();
      long neighborhoodLookups = 0;

      extensionWordIds.clear();

      int wordId;
      int lowerBound = vertices.getUnchecked(0);
      IntArrayList orderedVertices = null;
      VertexNeighbourhood neighbourhood = null;

      for (int i = numVertices - 1; i >= 0; --i) {
         wordId = vertices.getUnchecked(i);
         neighbourhood = configuration.getMainGraph().
            getVertexNeighbourhood(wordId);

         if (neighbourhood == null) {
            continue;
         }

         orderedVertices = neighbourhood.getOrderedVertices();
         int numOrderedVertices = orderedVertices.size();
         int fromIdx = neighborhoodCuts.getUnchecked(i);
         if (fromIdx < 0) {
            fromIdx = orderedVertices.binarySearch(vertices.getUnchecked(0));
            fromIdx = (fromIdx < 0) ? (-fromIdx - 1) : fromIdx;
            neighborhoodCuts.setUnchecked(i, fromIdx);
         }

         for (int j = fromIdx; j < numOrderedVertices; ++j) {
            int w = orderedVertices.getUnchecked(j);
            if (w > lowerBound) {
               extensionWordIds.add(w);
            } else {
               extensionWordIds.removeInt(w);
            }
         }

         neighborhoodLookups += (numOrderedVertices - fromIdx);

         lowerBound = Math.max(wordId, lowerBound);
      }

      computation.getExecutionEngine().aggregate(
            Configuration.NEIGHBORHOOD_LOOKUPS(getNumWords()),
            neighborhoodLookups);
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

   private class ValidWordIdAdderLast implements IntConsumer {
      private int lowerBound;

      public ValidWordIdAdderLast setBound(int lowerBound) {
         this.lowerBound = lowerBound;
         return this;
      }
      
      @Override
      public void accept(int w) {
         if (w > lowerBound) {
            extensionWordIds().add(w);
         }
      }
   }

   private class ValidWordIdAdderPrevious implements IntConsumer {
      private int lowerBound;

      public ValidWordIdAdderPrevious setBound(int lowerBound) {
         this.lowerBound = lowerBound;
         return this;
      }
      
      @Override
      public void accept(int w) {
         if (w > lowerBound) {
            extensionWordIds().add(w);
         } else {
            extensionWordIds().removeInt(w);
         }
      }
   }

   private class ValidWordIdAdder implements IntConsumer {
      private int lowerBound;

      public ValidWordIdAdder setBound(int lowerBound) {
         this.lowerBound = lowerBound;
         return this;
      }

      @Override
      public void accept(int w) {
         if (w > lowerBound) {
            extensionWordIds().add(w);
         } else {
            extensionWordIds().removeInt(w);
         }
      }
   }

   @Override
   public void applyTagFrom(Computation computation,
         AtomicBitSetArray vtag, AtomicBitSetArray etag, int pos) {

      int numVertices = vertices.size();
      int numEdges = edges.size();
      int upperIdx = numEdges - 1;

      for (int i = numVertices - 1; i >= pos; --i) {
         // tag vertex
         vtag.insert(vertices.getUnchecked(i));

         int lowerIdx = upperIdx - numEdgesAddedWithWord.getUnchecked(i);

         for (int j = upperIdx; j > lowerIdx; --j) {
            etag.insert(edges.getUnchecked(j));
         }

         upperIdx = lowerIdx;
      }
   }

   @Override
   public void applyTagTo(Computation computation,
         AtomicBitSetArray vtag, AtomicBitSetArray etag, int pos) {

      int lowerIdx = 0;

      for (int i = 0; i <= pos; ++i) {
         // tag vertex
         vtag.insert(vertices.getUnchecked(i));

         int upperIdx = lowerIdx + numEdgesAddedWithWord.getUnchecked(i);

         for (int j = lowerIdx; j < upperIdx; ++j) {
            etag.insert(edges.getUnchecked(j));
         }

         lowerIdx = upperIdx;
      }
   }
}
