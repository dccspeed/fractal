package br.ufmg.cs.systems.fractal.subgraph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.gmlib.motif.GtrieExtender;
import br.ufmg.cs.systems.fractal.graph.Edge;
import br.ufmg.cs.systems.fractal.graph.LabelledEdge;
import br.ufmg.cs.systems.fractal.graph.Vertex;
import br.ufmg.cs.systems.fractal.optimization.CliqueInducedSubgraphs;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.pool.HashIntSetPool;
import br.ufmg.cs.systems.fractal.util.pool.IntIntMapPool;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.koloboke.collect.set.hash.HashIntSet;
import java.util.function.IntConsumer;
import com.koloboke.function.IntIntConsumer;

import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectOutput;
import java.util.Objects;

public abstract class BasicSubgraph implements Subgraph {
   protected Configuration configuration;

   // Basic structure {{
   protected IntArrayList vertices;
   protected IntArrayList edges;

   // Extension helper structures {{
   protected HashIntSet extensionWordIds;
   protected IntIntMap extensionWordMaps;
   protected boolean dirtyExtensionWordIds;

   // Active extensions
   protected ObjArrayList<HashIntSet> extensionLevels;
   protected ObjArrayList<IntArrayList> extensionArrays;
   protected IntArrayList neighborhoodCuts;
   protected IntArrayList lastWords;

   protected HashIntObjMap cacheStore;

   // state
   protected CliqueInducedSubgraphs state;
   protected GtrieExtender extender;

   protected IntConsumer extensionWordIdsAdder = new IntConsumer() {
      @Override
      public void accept(int i) {
         extensionWordIds().add(i);
      }
   };

   protected BoundedWordIdAdder boundedExtensionsAdder =
      new BoundedWordIdAdder();
   
   protected BoundedWordMapAdder boundedExtensionsMapAdder =
      new BoundedWordMapAdder();
   
   protected BoundedWordMapAdderFixedSrc boundedExtensionsMapAdderFixedSrc =
      new BoundedWordMapAdderFixedSrc();

   // }}

   // Pattern {{
   /**
    * Pattern associated with this subgraph.
    * <p>
    * Whether the current value actually represents the current subgraph
    * depends on the value of the {@link #dirtyPattern} variable.
    */
   private Pattern pattern;
   /**
    * Whether the variable referred to in {@link #pattern} is up to date
    * with the structure of the subgraph.
    */
   private boolean dirtyPattern;
   // }}

   // Incremental Stuff {{
   // }}

   public BasicSubgraph() {
      vertices = new IntArrayList();
      edges = new IntArrayList();
      extensionWordMaps = IntIntMapPool.instance().createObject();
      extensionLevels = new ObjArrayList<HashIntSet>();
      extensionArrays = new ObjArrayList<IntArrayList>();
      neighborhoodCuts = new IntArrayList();
      cacheStore = HashIntObjMaps.newMutableMap();
      nextExtensionLevel();
   }

   @Override
   public void init(Configuration config) {
      configuration = config;
      reset();
   }

   @Override
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

   @Override
   public HashIntObjMap cacheStore() {
      return cacheStore;
   }

   @Override
   public GtrieExtender getExtender() {
      return extender;
   }

   @Override
   public void setExtender(GtrieExtender extender) {
      this.extender = extender;
   }

   @Override
   public CliqueInducedSubgraphs getState() {
      return state;
   }

   @Override
   public void setState(CliqueInducedSubgraphs state) {
      this.state = state;
   }

   @Override
   public IntCollection extensions() {
      return extensionWordIds();
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
   public void nextExtensionLevel(Subgraph other) {
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

   @Override
   public IntCollection extensions(Computation computation) {
      if (dirtyExtensionWordIds) {
         if (getNumWords() > 0) {
            updateExtensions(computation);
         } else {
            updateInitExtensibleWordIds(computation);
         }
      }

      return extensionWordIds();
   }
   
   @Override
   public IntCollection extensions(Computation computation, Pattern pattern) {
      if (dirtyExtensionWordIds) {
         if (getNumWords() == 0) {
            updateExtensionsInit(computation, pattern);
         } else {
            updateExtensions(computation, pattern);
         }

         int numVertices = getNumVertices();
         for (int i = 0; i < numVertices; ++i) {
            extensionWordIds().removeInt(vertices.getUnchecked(i));
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
         if (computation.containsWord(i) && computation.filter(this, i)) {
            extensionWordIds().add(i);
         }
      }

      computation.getExecutionEngine().aggregate(
            Configuration.NEIGHBORHOOD_LOOKUPS(getNumWords()),
            (endMyWordRange - startMyWordRange));
   }

   protected void updateExtensionsInit(Computation computation, Pattern pattern) {
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
         if (!computation.filter(this, i)) {
            continue;
         }

         int word = i;
         addWord(word);
         if (pattern.testSymmetryBreakerPos(this, 1)) {
            extensionWordIds().add(word);
         }
         removeLastWord();

         word += computation.getInitialNumWords();
         addWord(word);
         if (pattern.testSymmetryBreakerPos(this, 1)) {
            extensionWordIds().add(word);
         }
         removeLastWord();
      }

      computation.getExecutionEngine().aggregate(
            Configuration.NEIGHBORHOOD_LOOKUPS(getNumWords()),
            (endMyWordRange - startMyWordRange));
   }

   protected abstract void updateExtensibleWordIdsSimple(Computation computation);
   
   protected void updateExtensions(Computation computation, Pattern pattern) {
      updateExtensions(computation);
   }

   protected void updateExtensions(Computation computation) {
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

      // Clean the words that are already in the subgraph
      for (int i = 0; i < numWords; ++i) {
         int wId = words.getUnchecked(i);
         extensionWordIds().removeInt(wId);
      }
   }

   @Override
   public boolean isCanonicalSubgraphWithWord(int wordId) {
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
      return "Subgraph{" +
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

         pattern.setSubgraph(this);
         dirtyPattern = false;
      }

      return pattern;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BasicSubgraph that = (BasicSubgraph) o;
      return Objects.equals(vertices, that.vertices) &&
              Objects.equals(edges, that.edges);
   }

   @Override
   public int hashCode() {
      return Objects.hash(vertices, edges);
   }

   private class BoundedWordIdAdder implements IntConsumer {
      private int lowerBound = Integer.MIN_VALUE;
      private int upperBound = Integer.MAX_VALUE;

      public BoundedWordIdAdder setBounds(int lowerBound, int upperBound) {
         this.lowerBound = lowerBound;
         this.upperBound = upperBound;
         return this;
      }

      @Override
      public void accept(int i) {
         if (i > lowerBound && i < upperBound) {
            extensionWordIds().add(i);
         }
      }
   }

   private class BoundedWordMapAdder implements IntIntConsumer {
      private int lowerBound = Integer.MIN_VALUE;
      private int upperBound = Integer.MAX_VALUE;
      private Pattern pattern;

      public BoundedWordMapAdder setBounds(int lowerBound, int upperBound) {
         this.lowerBound = lowerBound;
         this.upperBound = upperBound;
         return this;
      }

      public BoundedWordMapAdder setPattern(Pattern pattern) {
         this.pattern = pattern;
         return this;
      }

      @Override
      public void accept(int k, int word) {
         extensionWordMaps.put(k, word);
         extensionWordIds().add(word);
      }
   }

   private class BoundedWordMapAdderFixedSrc implements IntConsumer {
      private int lowerBound = Integer.MIN_VALUE;
      private int upperBound = Integer.MAX_VALUE;
      private int k;

      public BoundedWordMapAdderFixedSrc setBounds(
            int lowerBound, int upperBound) {
         this.lowerBound = lowerBound;
         this.upperBound = upperBound;
         return this;
      }

      public BoundedWordMapAdderFixedSrc setK(int k) {
         this.k = k;
         return this;
      }

      @Override
      public void accept(int word) {
         extensionWordIds().add(word);
      }
   }
}
