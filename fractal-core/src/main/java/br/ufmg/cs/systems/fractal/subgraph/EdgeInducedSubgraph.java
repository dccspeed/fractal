package br.ufmg.cs.systems.fractal.subgraph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import com.koloboke.function.IntIntConsumer;

public class EdgeInducedSubgraph extends BasicSubgraph {

   private IntArrayList numVerticesAddedWithWord;
   private UpdateExtensionsConsumer updateExtensionsConsumer;
   private IntSet extensionsSet;

   public EdgeInducedSubgraph() {
      super();
      updateExtensionsConsumer = new UpdateExtensionsConsumer();
      numVerticesAddedWithWord = new IntArrayList();
      extensionsSet = HashIntSets.newMutableSet();
   }

   protected void updateVertices(int word) {
      final int edgeSrc = configuration.getMainGraph().edgeSrc(word);
      final int edgeDst = configuration.getMainGraph().edgeDst(word);

      int numVerticesAdded = 0;

      boolean srcIsNew = false;
      boolean dstIsNew = false;

      if (!vertices.contains(edgeSrc)) {
         srcIsNew = true;
      }

      if (!vertices.contains(edgeDst)) {
         dstIsNew = true;
      }

      if (srcIsNew) {
         vertices.add(edgeSrc);
         ++numVerticesAdded;
      }

      if (dstIsNew) {
         vertices.add(edgeDst);
         ++numVerticesAdded;
      }

      numVerticesAddedWithWord.add(numVerticesAdded);
   }

   @Override
   public String toString() {
      return "e" + super.toString();
   }

   @Override
   public void init(Configuration configuration) {
      super.init(configuration);
   }

   @Override
   public int numVerticesAdded(int wordIdx) {
      return numVerticesAddedWithWord.getu(wordIdx);
   }

   /**
    * Add word and update the number of vertices in this subgraph.
    *
    * @param word
    */
   @Override
   public void addWord(int word) {
      super.addWord(word);
      edges.add(word);
      updateVertices(word);
   }

   @Override
   public void removeLastWord() {
      if (getNumEdges() == 0) {
         return;
      }

      int numVerticesToRemove = numVerticesAddedWithWord.pop();
      vertices.removeLast(numVerticesToRemove);
      edges.removeLast();

      super.removeLastWord();
   }

   @Override
   public void computeExtensions(Computation computation,
                                          IntArrayList extensions) {
      extensionsSet.clear();
      getConfig().getMainGraph()
              .validExtensionsEdgeInduced(computation, this, extensionsSet);

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

      for (int u = myPartitionId; u < totalNumWords; u += numPartitions) {
         extensions.add(u);
      }
   }

   @Override
   public void reset() {
      super.reset();
      numVerticesAddedWithWord.clear();
      extensionsSet.clear();
   }

   @Override
   public IntArrayList getWords() {
      return getEdges();
   }

   @Override
   public int getNumWords() {
      return getNumEdges();
   }

   @Override
   public int numVerticesAdded() {
      return numVerticesAddedWithWord.getLastOrDefault(0);
   }

   @Override
   public int numEdgesAdded() {
      if (edges.isEmpty()) {
         return 0;
      }

      return 1;
   }

   private class UpdateExtensionsConsumer implements IntIntConsumer {
      private int lowerBound;
      private IntSet extensionWordIds;

      @Override
      public void accept(int u, int e) {
         if (e > lowerBound) {
            extensionWordIds.add(e);
         } else {
            extensionWordIds.removeInt(e);
         }
      }

      public void setLowerBound(int lowerBound) {
         this.lowerBound = lowerBound;
         //this.extensionWordIds = EdgeInducedSubgraph.this.extensionsSet();
         this.extensionWordIds = EdgeInducedSubgraph.this.extensionsSet;
      }
   }

}
