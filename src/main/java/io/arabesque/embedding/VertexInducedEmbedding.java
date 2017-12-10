package io.arabesque.embedding;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.function.IntConsumer;
import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.computation.Computation;
import io.arabesque.computation.BasicComputation;
import io.arabesque.computation.SparkFromScratchMasterEngine;
import io.arabesque.conf.Configuration;
import io.arabesque.graph.VertexNeighbourhood;
import io.arabesque.utils.collection.AtomicBitSetArray;
import io.arabesque.utils.collection.RoaringBitSet;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.ObjArrayList;
import io.arabesque.utils.collection.IntSet;

import java.io.DataInput;
import java.io.IOException;
import java.io.ObjectInput;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.hadoop.io.IntWritable;
import org.roaringbitmap.RoaringBitmap;

public class VertexInducedEmbedding extends BasicEmbedding {
   private static final Logger LOG = Logger.getLogger(VertexInducedEmbedding.class);
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

   private ObjArrayList<RoaringBitmap> bitmaps;
   
   public VertexInducedEmbedding() {
      super();
      updateEdgesConsumer = new UpdateEdgesConsumer();
      numEdgesAddedWithWord = new IntArrayList();
      bitmaps = new ObjArrayList<>();
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
      return numEdgesAddedWithWord.getLastOrDefault(0);
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
      vertices.add(word);
      neighborhoodCuts.add(-1);
      updateEdges(word, vertices.size() - 1);
   }

   @Override
   public void removeLastWord() {
      if (getNumVertices() == 0) {
         return;
      }

      int numEdgesToRemove = numEdgesAddedWithWord.pop();
      edges.removeLast(numEdgesToRemove);
      vertices.removeLast();
      neighborhoodCuts.removeLast();

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
    * Updates the list of edges of this embedding based on the addition of a
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
   public boolean isCanonicalEmbeddingWithWord(int wordId) {
      return true;
   }

   //@Override
   //protected void updateExtensibleWordIdsSimple(Computation computation) {
   //   IntArrayList vertices = getVertices();
   //   int numVertices = getNumVertices();

   //   neighborhoodWordIds().clear();

   //   int firstWordId = vertices.getUnchecked(0);
   //   int lastWordId = vertices.getUnchecked(numVertices - 1);

   //   //if (numVertices > 1) {
   //   //   if (unvisitedNeighborhood != null) {
   //   //      unvisitedNeighborhood.forEach(previousExtensionWordIdsAdder);
   //   //   }
   //   //}

   //   IntCollection neighbourhood = getValidNeighboursForExpansion(lastWordId);
   //   if (neighbourhood != null) {
   //      neighbourhood.forEach(
   //            lastExtensionWordIdsAdder.setBound(firstWordId));
   //   }

   //}

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

   //@Override
   //protected void updateExtensibleWordIdsSimple(Computation computation) {
   //   IntArrayList vertices = getVertices();
   //   int numVertices = getNumVertices();
   //   HashIntSet extensionWordIds = extensionWordIds();
   //   long neighborhoodLookups = 0;

   //   extensionWordIds.clear();

   //   int wordId;
   //   int lowerBound = vertices.getUnchecked(0);
   //   int[] orderedVertices = null;
   //   VertexNeighbourhood neighbourhood = null;

   //   for (int i = numVertices - 1; i >= 0; --i) {
   //      wordId = vertices.getUnchecked(i);
   //      neighbourhood = configuration.getMainGraph().
   //         getVertexNeighbourhood(wordId);

   //      if (neighbourhood == null) {
   //         continue;
   //      }

   //      orderedVertices = neighbourhood.getOrderedVertices();
   //      int fromIdx = neighborhoodCuts.getUnchecked(i);
   //      if (fromIdx < 0) {
   //         fromIdx = Arrays.binarySearch(orderedVertices,
   //                 vertices.getUnchecked(0));
   //         fromIdx = (fromIdx < 0) ? (-fromIdx - 1) : fromIdx;
   //         neighborhoodCuts.setUnchecked(i, fromIdx);
   //      }

   //      for (int j = fromIdx; j < orderedVertices.length; ++j) {
   //         int w = orderedVertices[j];
   //         if (w > lowerBound) {
   //            extensionWordIds.add(w);
   //         } else {
   //            extensionWordIds.removeInt(w);
   //         }
   //      }

   //      neighborhoodLookups += (orderedVertices.length - fromIdx);

   //      lowerBound = Math.max(wordId, lowerBound);
   //   }

   //   computation.getExecutionEngine().aggregate(
   //         SparkFromScratchMasterEngine.NEIGHBORHOOD_LOOKUPS(getNumWords()),
   //         neighborhoodLookups);
   //}
   //

   @Override
   protected void updateExtensibleWordIdsSimple(Computation computation) {
      IntArrayList vertices = getVertices();
      int numVertices = getNumVertices();
      int totalNumWords = computation.getInitialNumWords();

      VertexNeighbourhood neighbourhood = null;

      wordBitmaps().clear();
      cummulativeWordBitmaps().clear();

      if (numVertices == 1) {
         int firstWord = vertices.getUnchecked(0);

         neighbourhood = configuration.getMainGraph().
            getVertexNeighbourhood(firstWord);

         if (neighbourhood == null) return;

         wordBitmaps().mutableUnion(neighbourhood.getVerticesBitmap());
         wordBitmaps().mutableRemove(0, firstWord + 1);
         
         cummulativeWordBitmaps().mutableUnion(wordBitmaps());

      } else {
         int firstWord = vertices.getUnchecked(0);
         int lastWord = vertices.getLast();
         
         neighbourhood = configuration.getMainGraph().
            getVertexNeighbourhood(lastWord);
         
         cummulativeWordBitmaps().mutableUnion(neighbourhood.getVerticesBitmap());
         cummulativeWordBitmaps().mutableRemove(0, firstWord + 1);

         if (previousCummulativeWordBitmaps().isEmpty()) {
            for (int i = 0; i < numVertices - 1; ++i) {
               neighbourhood = configuration.getMainGraph().
                  getVertexNeighbourhood(vertices.getUnchecked(i));
               previousCummulativeWordBitmaps().mutableUnion(
                     neighbourhood.getVerticesBitmap());
            }
            previousCummulativeWordBitmaps().mutableRemove(0, firstWord + 1);
         }

         cummulativeWordBitmaps().mutableUnion(
               previousCummulativeWordBitmaps());

         int lastLastWord = getLastLastWord();

         if (lastLastWord == -1) {
            // incremental information is empty
            invalidWordBitmaps().transferFrom(
                  previousCummulativeWordBitmaps().
                  iremove(lastWord, totalNumWords));
         } else {
            // build invalids incrementally
            invalidWordBitmaps().mutableRemove(lastWord, lastLastWord);
         }
            
         setLastLastWord(lastWord);

         wordBitmaps().mutableUnion(cummulativeWordBitmaps());
         wordBitmaps().mutableDifference(invalidWordBitmaps());

         IntArrayList words = getWords();
         int numWords = getNumWords();

         // Clean the words that are already in the embedding
         for (int i = 0; i < numWords; ++i) {
            int wId = words.getUnchecked(i);
            wordBitmaps().remove(wId);
         }
      }

      wordBitmaps().shrink();
   }
   
   //@Override
   //protected void updateExtensibleWordIdsSimple(Computation computation) {
   //   IntArrayList vertices = getVertices();
   //   int numVertices = getNumVertices();
   //   int totalNumWords = computation.getInitialNumWords();

   //   VertexNeighbourhood neighbourhood = null;

   //   wordBitmaps().clear();
   //   cummulativeWordBitmaps().clear();

   //   if (numVertices == 1) {
   //      int firstWord = vertices.getUnchecked(0);

   //      neighbourhood = configuration.getMainGraph().
   //         getVertexNeighbourhood(firstWord);

   //      if (neighbourhood == null) return;

   //      bitmaps.clear();
   //      bitmaps.add(neighbourhood.getVerticesBitmap().getInternalBitmap());
   //      wordBitmaps().setInternalBitmap(RoaringBitmap.or(
   //               bitmaps.iterator(),
   //               firstWord + 1,
   //               totalNumWords));
   //      
   //      cummulativeWordBitmaps().setInternalBitmap(RoaringBitmap.or(
   //               bitmaps.iterator(),
   //               firstWord + 1,
   //               totalNumWords));

   //   } else {
   //      int firstWord = vertices.getUnchecked(0);
   //      int lastWord = vertices.getLast();
   //      
   //      if (previousCummulativeWordBitmaps().isEmpty()) {
   //         bitmaps.clear();
   //         for (int i = 0; i < numVertices - 1; ++i) {
   //            neighbourhood = configuration.getMainGraph().
   //               getVertexNeighbourhood(vertices.getUnchecked(i));
   //            bitmaps.add(
   //                  neighbourhood.getVerticesBitmap().getInternalBitmap());
   //         }
   //         previousCummulativeWordBitmaps().setInternalBitmap(
   //               RoaringBitmap.or(bitmaps.iterator(),
   //                  firstWord + 1,
   //                  totalNumWords)
   //               );
   //         previousCummulativeWordBitmaps().runOptimize();
   //      }

   //      neighbourhood = configuration.getMainGraph().
   //         getVertexNeighbourhood(lastWord);

   //      bitmaps.clear();
   //      bitmaps.add(neighbourhood.getVerticesBitmap().getInternalBitmap());
   //      bitmaps.add(previousCummulativeWordBitmaps().getInternalBitmap());
   //      cummulativeWordBitmaps().setInternalBitmap(RoaringBitmap.or(
   //               bitmaps.iterator(),
   //               firstWord + 1,
   //               totalNumWords));

   //      int lastLastWord = getLastLastWord();

   //      if (lastLastWord == -1) {
   //         // incremental information is empty
   //         invalidWordBitmaps().setInternalBitmap(
   //               RoaringBitmap.remove(
   //                  previousCummulativeWordBitmaps().getInternalBitmap(),
   //                  lastWord,
   //                  totalNumWords)
   //               );
   //      } else {
   //         // build invalids incrementally
   //         invalidWordBitmaps().mutableRemove(lastWord, lastLastWord);
   //      }

   //      cummulativeWordBitmaps().runOptimize();
   //      invalidWordBitmaps().runOptimize();
   //         
   //      setLastLastWord(lastWord);

   //      wordBitmaps().setInternalBitmap(
   //            RoaringBitmap.andNot(
   //               cummulativeWordBitmaps().getInternalBitmap(),
   //               invalidWordBitmaps().getInternalBitmap()
   //               )
   //            );

   //      IntArrayList words = getWords();
   //      int numWords = getNumWords();

   //      // Clean the words that are already in the embedding
   //      for (int i = 0; i < numWords; ++i) {
   //         int wId = words.getUnchecked(i);
   //         wordBitmaps().remove(wId);
   //      }

   //      wordBitmaps().runOptimize();
   //   }
   //}

   @Override
   public IntCollection getExtensibleWordIds(Computation computation) {
      IntCollection extensions = null;
      // If we have to recompute the extensionVertexIds set
      if (dirtyExtensionWordIds) {
         if (getNumWords() > 0) {
            updateExtensibleWordIdsSimple(computation);
            extensions = wordBitmaps();
         } else {
            updateInitExtensibleWordIds(computation);
            extensions = extensionWordIds();
         }
      }

      return extensions;
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
   public void applyTagFrom(AtomicBitSetArray vtag, AtomicBitSetArray etag,
         int pos) {

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
   public void applyTagTo(AtomicBitSetArray vtag, AtomicBitSetArray etag,
         int pos) {

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
