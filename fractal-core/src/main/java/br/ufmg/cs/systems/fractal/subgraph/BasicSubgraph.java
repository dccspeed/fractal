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
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.set.hash.HashIntSet;
import org.apache.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectOutput;
import java.util.Objects;

public abstract class BasicSubgraph implements Subgraph {
   private static final Logger LOG = Logger.getLogger(BasicSubgraph.class);
   protected Configuration configuration;

   // Basic structure {{
   protected IntArrayList vertices;
   protected IntArrayList edges;

   protected boolean dirtyExtensionWordIds;

   // Active extensions
   private ObjArrayList<HashIntSet> extensionLevels;
   // }}

   // Pattern {{
   /**
    * Pattern associated with this subgraph.
    * <p>
    * Whether the current value actually represents the current subgraph
    * depends on the value of the {@link #dirtyPattern} variable.
    */
   protected Pattern pattern;
   /**
    * Whether the variable referred to in {@link #pattern} is up to date
    * with the structure of the subgraph.
    */
   protected boolean dirtyPattern;
   // }}

   // Incremental Stuff {{
   // }}

   public BasicSubgraph() {
      vertices = new IntArrayList();
      edges = new IntArrayList();
      extensionLevels = new ObjArrayList<HashIntSet>();
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

   @Override
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
            extensionWordIds().removeInt(words.getu(i));
         }
      }

      return extensionWordIds();
   }

   protected void updateInitExtensions(Computation computation) {
      int totalNumWords = computation.getInitialNumWords();
      int numPartitions = computation.getNumberPartitions();
      int myPartitionId = computation.getPartitionId();

      for (int u = myPartitionId; u < totalNumWords; u += numPartitions) {
         if (computation.containsWord(u)) {
            extensionWordIds().add(u);
         }
      }

      computation.getExecutionEngine().aggregate(
              Configuration.NEIGHBORHOOD_LOOKUPS(getNumWords()),
              extensionWordIds().size());
   }

   protected abstract void updateExtensions(Computation computation);

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
              "extensionLevels=" + extensionLevels +
              "} " + super.toString();
   }

   @Override
   public Pattern quickPattern() {
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
