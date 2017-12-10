package io.arabesque.embedding;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.function.IntConsumer;
import io.arabesque.computation.Computation;
import io.arabesque.computation.SparkFromScratchMasterEngine;
import io.arabesque.conf.Configuration;
import io.arabesque.pattern.Pattern;
import io.arabesque.utils.collection.AtomicBitSetArray;
import io.arabesque.utils.collection.RoaringBitSet;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.IntCollectionAddConsumer;
import io.arabesque.utils.collection.ObjArrayList;
import io.arabesque.utils.pool.HashIntSetPool;
import io.arabesque.utils.pool.RoaringBitSetPool;

import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectOutput;
import java.util.Objects;

public abstract class BasicEmbedding implements Embedding {
   protected Configuration configuration;

   // Basic structure {{
   protected IntArrayList vertices;
   protected IntArrayList edges;

   // Extension helper structures {{
   protected HashIntSet extensionWordIds;
   protected boolean dirtyExtensionWordIds;

   // Active extensions
   protected ObjArrayList<RoaringBitSet> wordBitmaps;
   protected ObjArrayList<RoaringBitSet> cummulativeWordBitmaps;
   protected ObjArrayList<RoaringBitSet> invalidWordBitmaps;
   protected ObjArrayList<HashIntSet> extensionLevels;
   protected IntArrayList neighborhoodCuts;
   protected IntArrayList lastWords;

   protected IntConsumer extensionWordIdsAdder = new IntConsumer() {
      @Override
      public void accept(int i) {
         extensionWordIds().add(i);
      }
   };

   protected IntCollectionAddConsumer intAddConsumer =
      new IntCollectionAddConsumer();

   // }}

   // Pattern {{
   /**
    * Pattern associated with this embedding.
    * <p>
    * Whether the current value actually represents the current embedding
    * depends on the value of the {@link #dirtyPattern} variable.
    */
   private Pattern pattern;
   /**
    * Whether the variable referred to in {@link #pattern} is up to date
    * with the structure of the embedding.
    */
   private boolean dirtyPattern;
   // }}

   // Incremental Stuff {{
   // }}

   public BasicEmbedding() {
      vertices = new IntArrayList();
      edges = new IntArrayList();
      extensionWordIds = HashIntSetPool.instance().createObject();
      extensionLevels = new ObjArrayList<HashIntSet>();
      wordBitmaps = new ObjArrayList<RoaringBitSet>();
      cummulativeWordBitmaps = new ObjArrayList<RoaringBitSet>();
      invalidWordBitmaps = new ObjArrayList<RoaringBitSet>();
      neighborhoodCuts = new IntArrayList();
      lastWords = new IntArrayList();
      nextExtensionLevel();
   }

   @Override
   public void init(Configuration config) {
      configuration = config;
      reset();
   }

   public Configuration getConfig() {
      return configuration;
   }

   public void reset() {
      vertices.clear();
      edges.clear();
      for (int i = 0; i < extensionLevels.size() - 1; ++i) {
         previousExtensionLevel();
      }
      setDirty();
   }

   protected void setDirty() {
      dirtyPattern = true;
      dirtyExtensionWordIds = true;
   }

   protected RoaringBitSet invalidWordBitmaps() {
      return invalidWordBitmaps.getLast();
   }

   protected int getLastLastWord() {
      return lastWords.getLast();
   }
   
   protected void setLastLastWord(int lastLastWord) {
      lastWords.setUnchecked(lastWords.size() - 1, lastLastWord);
   }

   protected RoaringBitSet wordBitmaps() {
      return wordBitmaps.getLast();
   }
   
   protected RoaringBitSet wordBitmaps(int level) {
      return wordBitmaps.get(level);
   }

   protected RoaringBitSet cummulativeWordBitmaps() {
      return cummulativeWordBitmaps.getLast();
   }
   
   protected RoaringBitSet cummulativeWordBitmaps(int level) {
      return cummulativeWordBitmaps.get(level);
   }
   
   protected RoaringBitSet previousCummulativeWordBitmaps() {
      return cummulativeWordBitmaps.get(cummulativeWordBitmaps.size() - 2);
   }

   protected HashIntSet extensionWordIds() {
      return extensionLevels.getLast();
   }

   protected HashIntSet extensionWordIds(int level) {
      return extensionLevels.get(level);
   }

   protected HashIntSet previousExtensionWordIds() {
      return extensionLevels.get(extensionLevels.size() - 2);
   }

   @Override
   public void nextExtensionLevel() {
      extensionLevels.add(HashIntSetPool.instance().createObject());
      wordBitmaps.add(RoaringBitSetPool.instance().createObject());
      cummulativeWordBitmaps.add(RoaringBitSetPool.instance().createObject());
      invalidWordBitmaps.add(RoaringBitSetPool.instance().createObject());
      lastWords.add(-1);
   }

   @Override
   public void previousExtensionLevel() {
      HashIntSetPool.instance().reclaimObject(
            extensionLevels.remove(extensionLevels.size() - 1));
      RoaringBitSetPool.instance().reclaimObject(
            wordBitmaps.remove(wordBitmaps.size() - 1));
      RoaringBitSetPool.instance().reclaimObject(
            cummulativeWordBitmaps.remove(cummulativeWordBitmaps.size() - 1));
      RoaringBitSetPool.instance().reclaimObject(
            invalidWordBitmaps.remove(invalidWordBitmaps.size() - 1));
      lastWords.remove(lastWords.size() - 1);
   }

   @Override
   public void nextExtensionLevel(Embedding other) {
      BasicEmbedding basicOther = (BasicEmbedding) other;
      int level = extensionLevels.size();

      //extensionLevels.add(
      //      basicOther.extensionLevels.getUnchecked(level));
      //wordBitmaps.add(
      //      basicOther.wordBitmaps.getUnchecked(level));
      extensionLevels.add(HashIntSetPool.instance().createObject());
      wordBitmaps.add(RoaringBitSetPool.instance().createObject());
      cummulativeWordBitmaps.add(
            basicOther.cummulativeWordBitmaps.getUnchecked(level).clone());
      invalidWordBitmaps.add(
            basicOther.invalidWordBitmaps.getUnchecked(level).clone());
      lastWords.add(
            basicOther.lastWords.getUnchecked(level));
   }

   @Override
   public IntArrayList getVertices() {
      return vertices;
   }

   @Override
   public int getNumVertices() {
      return vertices.size();
   }

   @Override
   public IntArrayList getEdges() {
      return edges;
   }

   @Override
   public int getNumEdges() {
      return edges.size();
   }

   @Override
   public void write(DataOutput out) throws IOException {
      out.writeInt(configuration.getId());
      getWords().write(out);
   }

   @Override
   public void writeExternal(ObjectOutput objOutput) throws IOException {
      write(objOutput);
   }

   @Override
   public IntCollection getExtensibleWordIds(Computation computation) {
      // If we have to recompute the extensionVertexIds set
      if (dirtyExtensionWordIds) {
         if (getNumWords() > 0) {
            updateExtensibleWordIdsSimple(computation);
         } else {
            updateInitExtensibleWordIds(computation);
         }
      }

      return extensionWordIds();
   }

   protected void updateInitExtensibleWordIds(Computation computation) {
      int totalNumWords = computation.getInitialNumWords();
      int numPartitions = computation.getNumberPartitions();
      int myPartitionId = computation.getPartitionId();
      int numWordsPerPartition = Math.max(totalNumWords / numPartitions, 1);
      int startMyWordRange = myPartitionId * numWordsPerPartition;
      int endMyWordRange = startMyWordRange + numWordsPerPartition;

      // If we are the last partition or our range end goes over the total
      // number of vertices, set the range end to the total number of vertices
      if (myPartitionId == numPartitions - 1 ||
            endMyWordRange > totalNumWords) {
         endMyWordRange = totalNumWords;
      }

      for (int i = startMyWordRange; i < endMyWordRange; ++i) {
         extensionWordIds().add(i);
      }

      computation.getExecutionEngine().aggregate(
            SparkFromScratchMasterEngine.NEIGHBORHOOD_LOOKUPS(getNumWords()),
            (endMyWordRange - startMyWordRange));
   }

   protected void updateExtensibleWordIdsSimple(Computation computation) {
      IntArrayList vertices = getVertices();
      int numVertices = getNumVertices();

      extensionWordIds().clear();

      for (int i = 0; i < numVertices; ++i) {
         IntCollection neighbourhood = getValidNeighboursForExpansion(
                 vertices.getUnchecked(i));

         if (neighbourhood != null) {
            neighbourhood.forEach(extensionWordIdsAdder);
         }
      }

      IntArrayList words = getWords();
      int numWords = getNumWords();

      // Clean the words that are already in the embedding
      for (int i = 0; i < numWords; ++i) {
         int wId = words.getUnchecked(i);
         extensionWordIds().removeInt(wId);
      }
   }

   @Override
   public boolean isCanonicalEmbeddingWithWord(int wordId) {
      IntArrayList words = getWords();
      int numWords = words.size();

      if (numWords == 0) return true;
      if (wordId < words.getUnchecked(0)) return false;

      int i;

      // find the first neighbor
      for (i = 0; i < numWords; ++i) {
         if (areWordsNeighbours(wordId, words.getUnchecked(i))) {
            break;
         }
      }

      // if we didn't find any neighbor
      if (i == numWords) {
         // not canonical because it's disconnected
         return false;
      }

      // If we found the first neighbour, all following words should have
      // lower ids than the one we are trying to add
      i++;
      for (; i < numWords; ++i) {
         // If one of those ids is higher or equal, not canonical
         if (words.getUnchecked(i) >= wordId) {
            return false;
         }
      }

      return true;
   }

   protected abstract boolean areWordsNeighbours(int wordId1, int wordId2);

   protected abstract IntCollection getValidNeighboursForExpansion(int vId);

   @Override
   public void addWord(int word) {
      setDirty();
   }

   @Override
   public void removeLastWord() {
      setDirty();
   }

   @Override
   public String toString() {
      return "Embedding{" +
              "vertices=" + vertices + ", " +
              "edges=" + edges +
              "} " + super.toString();
   }

   @Override
   public Pattern getPattern() {
      if (dirtyPattern) {
         if (pattern == null) {
            pattern = configuration.createPattern();
         }

         pattern.setEmbedding(this);
         dirtyPattern = false;
      }

      return pattern;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BasicEmbedding that = (BasicEmbedding) o;
      return Objects.equals(vertices, that.vertices) &&
              Objects.equals(edges, that.edges);
   }

   @Override
   public int hashCode() {
      return Objects.hash(vertices, edges);
   }

   @Override
   public void applyTagFrom(AtomicBitSetArray vtag, AtomicBitSetArray etag, int pos) {
   }
   
   @Override
   public void applyTagTo(AtomicBitSetArray vtag, AtomicBitSetArray etag, int pos) {
   }
}
