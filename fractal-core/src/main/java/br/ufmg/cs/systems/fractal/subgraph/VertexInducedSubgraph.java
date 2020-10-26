package br.ufmg.cs.systems.fractal.subgraph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import com.koloboke.function.IntIntConsumer;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.io.ObjectInput;
import java.util.function.IntConsumer;

public class VertexInducedSubgraph extends BasicSubgraph {
   private static final Logger LOG =
           Logger.getLogger(VertexInducedSubgraph.class);

   private UpdateEdgesConsumer updateEdgesConsumer;
   private UpdateExtensionsConsumer updateExtensionsConsumer;
   private IntArrayList numEdgesAddedWithWord;
   private IntSet extensionsSet;
   private int lastPositionAdded;

   public VertexInducedSubgraph() {
      super();
      updateEdgesConsumer = new UpdateEdgesConsumer();
      updateExtensionsConsumer = new UpdateExtensionsConsumer();
      numEdgesAddedWithWord = new IntArrayList();
      lastPositionAdded = -1;
      extensionsSet = HashIntSets.newMutableSet();
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
   public int numVerticesAdded() {
      if (vertices.isEmpty()) {
         return 0;
      }

      return 1;
   }

   @Override
   public int numEdgesAdded() {
      //ensureEdges();
      return numEdgesAddedWithWord.getLastOrDefault(0);
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
   public void init(Configuration config) {
      super.init(config);
   }

   @Override
   public void reset() {
      super.reset();
      extensionsSet.clear();
      numEdgesAddedWithWord.clear();
      lastPositionAdded = -1;
   }

   @Override
   public IntArrayList getEdges() {
      //ensureEdges();
      return edges;
   }

   private void updateInitExtensions(Computation computation) {
      int totalNumWords = computation.getInitialNumWords();
      int numPartitions = computation.getNumberPartitions();
      int myPartitionId = computation.getPartitionId();

      extensionsSet.clear();
      for (int u = myPartitionId; u < totalNumWords; u += numPartitions) {
         if (computation.containsWord(u)) extensionsSet.add(u);
      }
   }

   private void updateExtensions(Computation computation) {
      extensionsSet.clear();
      getConfig().getMainGraph().validExtensionsVertexInduced(
              computation, this, extensionsSet);
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
            extensionsSet.removeInt(words.getu(i));
         }
      }

      return extensionsSet;
   }

   @Override
   public void addWord(int word) {
      super.addWord(word);
      updateEdges(word, vertices.size());
      vertices.add(word);
      //ensureEdges();
   }

   @Override
   public void setWordAndTruncate(int word, int index) {
      super.setWordAndTruncate(word, index);
      updateEdges(word, vertices.size());
      vertices.setAndTruncate(index, word);
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
   public Pattern quickPattern() {
      //ensureEdges();
      return super.quickPattern();
   }

   private void ensureEdges() {
      while (lastPositionAdded + 1 < vertices.size()) {
         updateEdges(vertices.get(lastPositionAdded + 1),
                 lastPositionAdded + 1);
      }
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

   private void removeExtraEdges() {
      while (lastPositionAdded >= vertices.size()) {
         int numEdgesToRemove = numEdgesAddedWithWord.pop();
         edges.removeLast(numEdgesToRemove);
         lastPositionAdded--;
      }
   }

   @Override
   public void readExternal(ObjectInput objInput) throws IOException,
           ClassNotFoundException {
      readFields(objInput);
   }

   public void readFields(DataInput in) throws IOException {
      reset();
      vertices.readFields(in);
      int numVertices = vertices.size();
      for (int i = 0; i < numVertices; ++i) {
         updateEdges(vertices.getu(i), i);
      }
   }

   private class UpdateExtensionsConsumer implements IntIntConsumer {
      private int lowerBound;
      private IntSet extensionsWordIds;

      @Override
      public void accept(int u, int e) {
         if (u > lowerBound) {
            extensionsWordIds.add(u);
         } else {
            extensionsWordIds.removeInt(u);
         }
      }

      public void setLowerBound(int lowerBound) {
         this.lowerBound = lowerBound;
         this.extensionsWordIds = VertexInducedSubgraph.this.extensionsSet;
      }
   }

   private class UpdateEdgesConsumer implements IntConsumer {
      private int numAdded;

      @Override
      public void accept(int i) {
         edges.add(i);
         ++numAdded;
      }

      public int getNumAdded() {
         return numAdded;
      }

      public void reset() {
         numAdded = 0;
      }
   }

   @Override
   public String toString() {
      return "v" + super.toString();
   }

}
