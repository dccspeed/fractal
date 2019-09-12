package br.ufmg.cs.systems.fractal.subgraph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.Edge;
import br.ufmg.cs.systems.fractal.graph.LabelledEdge;
import br.ufmg.cs.systems.fractal.graph.Vertex;
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

import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectOutput;
import java.util.Objects;

public abstract class BasicSubgraph implements Subgraph {
   protected Configuration configuration;

   // Basic structure {{
   protected IntArrayList vertices;
   protected IntArrayList edges;

   protected IntIntMap extensionWordMaps;
   protected boolean dirtyExtensionWordIds;

   // Active extensions
   protected ObjArrayList<HashIntSet> extensionLevels;
   protected ObjArrayList<IntArrayList> extensionArrays;
   protected IntArrayList neighborhoodCuts;

   protected HashIntObjMap cacheStore;

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
   public IntCollection extensions() {
      return extensionWordIds();
   }

   protected HashIntSet extensionWordIds() {
      return extensionLevels.getLast();
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
   public IntCollection computeExtensions(Computation computation) {
      // If we have to recompute the extensionVertexIds set
      if (dirtyExtensionWordIds) {
         if (getNumWords() > 0) {
            updateExtensions(computation);
         } else {
            updateInitExtensions(computation);
         }

         int numWords = getNumWords();
         IntArrayList words = getWords();
         for (int i = 0; i < numWords; ++i) {
            extensionWordIds().removeInt(words.getUnchecked(i));
         }
      }

      return extensionWordIds();
   }

  protected void updateInitExtensions(Computation computation) {
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

   protected abstract void updateExtensions(Computation computation);

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
      return "subgraph{" +
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

}
