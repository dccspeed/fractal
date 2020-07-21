package br.ufmg.cs.systems.fractal.subgraph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.function.IntIntConsumer;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.io.ObjectInput;
import java.util.function.IntConsumer;

public class VertexInducedSubgraph extends BasicSubgraph {
   private static final Logger LOG = Logger.getLogger(VertexInducedSubgraph.class);
   // Consumers {{
   private UpdateEdgesConsumer updateEdgesConsumer;
   private UpdateExtensionsConsumer updateExtensionsConsumer;
   // }}

   // Edge tracking for incremental modifications {{
   private IntArrayList numEdgesAddedWithWord;
   // }}

   private int lastPositionAdded;

   public VertexInducedSubgraph() {
      super();
      updateEdgesConsumer = new UpdateEdgesConsumer();
      updateExtensionsConsumer = new UpdateExtensionsConsumer();
      numEdgesAddedWithWord = new IntArrayList();
      lastPositionAdded = -1;
   }

   @Override
   public void init(Configuration config) {
      super.init(config);
   }

   @Override
   public void reset() {
      super.reset();
      numEdgesAddedWithWord.clear();
      lastPositionAdded = -1;
   }

   @Override
   public IntArrayList getWords() {
      return getVertices();
   }

   @Override
   public int getNumWords() {
      return getNumVertices();
   }
   
   @Override
   public Pattern quickPattern() {
      ensureEdges();
      return super.quickPattern();
   }

   @Override
   public IntArrayList getEdges() {
      ensureEdges();
      return edges;
   }

   @Override
   public String toOutputString() {
      StringBuilder sb = new StringBuilder();

      IntArrayList vertices = getVertices();

      for (int i = 0; i < vertices.size(); ++i) {
         sb.append(vertices.getu(i));
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

   @Override
   public void addWord(int word) {
      super.addWord(word);
      vertices.add(word);
   }

   @Override
   public void removeLastWord() {
      if (getNumVertices() == 0) {
         return;
      }

      vertices.removeLast();

      removeExtraEdges();

      super.removeLastWord();
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      reset();

      //init(Configuration.get(in.readInt()));

      vertices.readFields(in);

      int numVertices = vertices.size();

      for (int i = 0; i < numVertices; ++i) {
         updateEdges(vertices.getu(i), i);
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
         int existingVertexId = vertices.getu(i);

         updateEdgesConsumer.reset();
         configuration.getMainGraph().forEachEdge(existingVertexId,
                 newVertexId, updateEdgesConsumer);
         addedEdges += updateEdgesConsumer.getNumAdded();
      }

      numEdgesAddedWithWord.add(addedEdges);

      lastPositionAdded = positionAdded;
   }

   @Override
   protected void updateExtensions(Computation computation) {
      IntArrayList vertices = getVertices();
      int numVertices = getNumVertices();
      HashIntSet extensionWordIds = extensionWordIds();
      long neighborhoodLookups = 0;

      extensionWordIds.clear();

      int wordId;
      int lowerBound = vertices.getu(0);

      MainGraph graph = configuration.getMainGraph();

      for (int i = numVertices - 1; i >= 0; --i) {
         wordId = vertices.getu(i);
         updateExtensionsConsumer.setLowerBound(lowerBound);
         graph.neighborhoodTraversalVertexRange(wordId, vertices.getu(0), updateExtensionsConsumer);
         lowerBound = Math.max(wordId, lowerBound);
      }

      computation.getExecutionEngine().aggregate(
            Configuration.NEIGHBORHOOD_LOOKUPS(getNumWords()),
            neighborhoodLookups);
   }

   private class UpdateExtensionsConsumer implements IntIntConsumer {
      private int lowerBound;
      private HashIntSet extensionsWordIds;

      public void setLowerBound(int lowerBound) {
         this.lowerBound = lowerBound;
         this.extensionsWordIds = VertexInducedSubgraph.this.extensionWordIds();
      }

      @Override
      public void accept(int u, int e) {
         if (u > lowerBound) {
            extensionsWordIds.add(u);
         } else {
            extensionsWordIds.removeInt(u);
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

   @Override
   public void applyTagFrom(Computation computation,
         AtomicBitSetArray vtag, AtomicBitSetArray etag, int pos) {

      int numVertices = vertices.size();
      int numEdges = edges.size();
      int upperIdx = numEdges - 1;

      for (int i = numVertices - 1; i >= pos; --i) {
         // tag vertex
         vtag.insert(vertices.getu(i));

         int lowerIdx = upperIdx - numEdgesAddedWithWord.getu(i);

         for (int j = upperIdx; j > lowerIdx; --j) {
            etag.insert(edges.getu(j));
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
         vtag.insert(vertices.getu(i));

         int upperIdx = lowerIdx + numEdgesAddedWithWord.getu(i);

         for (int j = lowerIdx; j < upperIdx; ++j) {
            etag.insert(edges.getu(j));
         }

         lowerIdx = upperIdx;
      }
   }
}
