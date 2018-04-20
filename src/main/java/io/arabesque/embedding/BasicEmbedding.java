package io.arabesque.embedding;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.function.IntConsumer;
import io.arabesque.computation.Computation;
import io.arabesque.computation.SparkFromScratchMasterEngine;
import io.arabesque.conf.Configuration;
import io.arabesque.graph.Vertex;
import io.arabesque.graph.Edge;
import io.arabesque.graph.LabelledEdge;
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
      extensionLevels = new ObjArrayList<HashIntSet>();
      neighborhoodCuts = new IntArrayList();
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
   }

   @Override
   public void previousExtensionLevel() {
      HashIntSetPool.instance().reclaimObject(
            extensionLevels.remove(extensionLevels.size() - 1));
   }

   @Override
   public void nextExtensionLevel(Embedding other) {
      extensionLevels.add(HashIntSetPool.instance().createObject());
   }

   @Override
   public IntArrayList getVertices() {
      return vertices;
   }

   @Override
   public <V> Vertex<V> vertex(int vertexId) {
      return (Vertex<V>) configuration.getMainGraph().getVertex(vertexId);
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
   public <E> Edge<E> edge(int edgeId) {
      return (Edge<E>) configuration.getMainGraph().getEdge(edgeId);
   }
   
   @Override
   public <E> LabelledEdge<E> labelledEdge(int edgeId) {
      return (LabelledEdge<E>) configuration.getMainGraph().getEdge(edgeId);
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
         if (computation.filter(this, i)) {
            extensionWordIds().add(i);
         }
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
