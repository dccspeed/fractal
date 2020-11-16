package br.ufmg.cs.systems.fractal.subgraph;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import org.apache.log4j.Logger;

import java.util.Objects;

public abstract class BasicSubgraph implements Subgraph {
   private static final Logger LOG = Logger.getLogger(BasicSubgraph.class);
   protected Configuration configuration;

   protected IntArrayList vertices;
   protected IntArrayList edges;
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
      return "subgraph{" + "vertices=" + vertices + "," + "edges=" + edges + "}";
   }

   @Override
   public void init(Configuration configuration) {
      this.configuration = configuration;
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
   public void removeLastWord() {
      setDirty();
   }

   @Override
   public void reset() {
      vertices.clear();
      edges.clear();
      setDirty();
   }

   protected void setDirty() {
      dirtyPattern = true;
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
