package br.ufmg.cs.systems.fractal.subgraph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.Edge;
import br.ufmg.cs.systems.fractal.graph.LabelledEdge;
import br.ufmg.cs.systems.fractal.graph.Vertex;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.IntCollection;
import org.apache.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectOutput;
import java.util.Objects;

public abstract class BasicSubgraph implements Subgraph {
   private static final Logger LOG = Logger.getLogger(BasicSubgraph.class);
   protected Configuration configuration;

   protected IntArrayList vertices;
   protected IntArrayList edges;
   protected boolean dirtyExtensionWordIds;
   protected Pattern pattern;
   protected boolean dirtyPattern;

   public BasicSubgraph() {
      vertices = new IntArrayList();
      edges = new IntArrayList();
   }

   @Override
   public int hashCode() {
      return Objects.hash(vertices, edges);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BasicSubgraph that = (BasicSubgraph) o;
      return Objects.equals(vertices, that.vertices) && Objects.equals(edges,
              that.edges);
   }

   @Override
   public String toString() {
      return "subgraph{" + "vertices=" + vertices + "," + "edges=" + edges +
              "}";
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
   public int numVerticesAdded(int wordIdx) {
      return numVerticesAdded();
   }

   @Override
   public void addWord(int word) {
      setDirty();
   }

   @Override
   public void setWordAndTruncate(int word, int index) {
      setDirty();
   }

   @Override
   public void removeLastWord() {
      setDirty();
   }

   @Override
   public abstract IntCollection computeExtensions(Computation computation);

   @Override
   public void reset() {
      vertices.clear();
      edges.clear();
      setDirty();
   }

   protected void setDirty() {
      dirtyPattern = true;
      dirtyExtensionWordIds = true;
   }

   @Override
   public void writeExternal(ObjectOutput objOutput) throws IOException {
      write(objOutput);
   }

   @Override
   public void write(DataOutput out) throws IOException {
      out.writeInt(configuration.getId());
      getWords().write(out);
   }

   @Override
   public long computeExtensionCost(IntArrayList extensionCandidates) {
      long cost = 0;
      for (int i = 0; i < extensionCandidates.size(); ++i) {
         cost += extensionCandidates.getu(i);
      }
      return cost;
   }

}
