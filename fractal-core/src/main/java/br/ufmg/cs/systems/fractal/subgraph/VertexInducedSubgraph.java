package br.ufmg.cs.systems.fractal.subgraph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import org.apache.log4j.Logger;

import java.util.function.IntConsumer;

public class VertexInducedSubgraph extends BasicSubgraph {
   private static final Logger LOG =
           Logger.getLogger(VertexInducedSubgraph.class);

   private UpdateEdgesConsumer updateEdgesConsumer;
   private IntArrayList numEdgesAddedWithWord;
   private HashIntSet extensionsSet;

   public VertexInducedSubgraph() {
      super();
      updateEdgesConsumer = new UpdateEdgesConsumer();
      numEdgesAddedWithWord = new IntArrayList();
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
      return numEdgesAddedWithWord.getLastOrDefault(0);
   }

   @Override
   public void init(Configuration configuration) {
      super.init(configuration);
   }

   @Override
   public void reset() {
      super.reset();
      extensionsSet.clear();
      numEdgesAddedWithWord.clear();
   }

   @Override
   public IntArrayList getEdges() {
      return edges;
   }

   @Override
   public void computeExtensions(Computation computation,
                                 IntArrayList extensions) {
      extensionsSet.clear();
      getConfig().getMainGraph().validExtensionsVertexInduced(
              computation, this, extensionsSet);

      int numWords = getNumWords();
      IntArrayList words = getWords();
      for (int i = 0; i < numWords; ++i) {
         extensionsSet.removeInt(words.getu(i));
      }

      extensions.setFrom(extensionsSet);
   }

   @Override
   public void computeFirstLevelExtensions(Computation computation,
                                           IntArrayList extensions) {
      int totalNumWords = computation.getInitialNumWords();
      int numPartitions = computation.getNumberPartitions();
      int myPartitionId = computation.getPartitionId();
      MainGraph graph = computation.getConfig().getMainGraph();

      computeFirstLevelExtensions(null, totalNumWords, numPartitions,
              myPartitionId, graph, extensions);
   }

   @Override
   public void computeFirstLevelExtensions(Pattern pattern, int totalNumWords,
                                           int numPartitions,
                                           int partitionId, MainGraph graph,
                                           IntArrayList extensions) {
      // round-robin
      for (int u = partitionId; u < totalNumWords; u += numPartitions) {
         if (graph.isVertexValid(u)) extensions.add(u);
      }
   }

   @Override
   public void addWord(int word) {
      super.addWord(word);
      updateEdges(word, vertices.size());
      vertices.add(word);
   }

   @Override
   public void removeLastWord() {
      vertices.removeLast();
      int numEdgesToRemove = numEdgesAddedWithWord.pop();
      edges.removeLast(numEdgesToRemove);

      super.removeLastWord();
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
