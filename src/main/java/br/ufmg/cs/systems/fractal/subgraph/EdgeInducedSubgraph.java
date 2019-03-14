package br.ufmg.cs.systems.fractal.subgraph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.Edge;
import br.ufmg.cs.systems.fractal.graph.VertexNeighbourhood;
import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.set.hash.HashIntSet;

import java.io.DataInput;
import java.io.IOException;
import java.io.ObjectInput;

public class EdgeInducedSubgraph extends BasicSubgraph {
   private IntArrayList numVerticesAddedWithWord;

   public EdgeInducedSubgraph() {
      super();
      numVerticesAddedWithWord = new IntArrayList();
   }

   @Override
   public void init(Configuration config) {
      super.init(config);
   }

   @Override
   public void reset() {
      super.reset();
      numVerticesAddedWithWord.clear();
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
   public String toOutputString() {
      StringBuilder sb = new StringBuilder();

      int numEdges = getNumEdges();
      IntArrayList edges = getEdges();

      for (int i = 0; i < numEdges; ++i) {
         Edge edge = configuration.getMainGraph().
                 getEdge(edges.getUnchecked(i));
         sb.append(edge.getSourceId());
         sb.append("-");
         sb.append(edge.getDestinationId());
         sb.append(" ");
      }

      return sb.toString();
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

  @Override
   protected boolean areWordsNeighbours(int wordId1, int wordId2) {
      return configuration.getMainGraph().areEdgesNeighbors(wordId1, wordId2);
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

   protected void updateVertices(int word) {
      final Edge edge = configuration.getMainGraph().getEdge(word);

      int numVerticesAdded = 0;

      boolean srcIsNew = false;
      boolean dstIsNew = false;

      if (!vertices.contains(edge.getSourceId())) {
         srcIsNew = true;
      }

      if (!vertices.contains(edge.getDestinationId())) {
         dstIsNew = true;
      }

      if (srcIsNew) { 
         vertices.add(edge.getSourceId());
         neighborhoodCuts.add(-1);
         ++numVerticesAdded;
      }

      if (dstIsNew) {
         vertices.add(edge.getDestinationId());
         neighborhoodCuts.add(-1);
         ++numVerticesAdded;
      }

      numVerticesAddedWithWord.add(numVerticesAdded);
   }

   @Override
   public void removeLastWord() {
      if (getNumEdges() == 0) {
         return;
      }

      int numVerticesToRemove = numVerticesAddedWithWord.pop();
      vertices.removeLast(numVerticesToRemove);
      neighborhoodCuts.removeLast(numVerticesToRemove);
      edges.removeLast();

      super.removeLastWord();
   }

   @Override
   public boolean isCanonicalSubgraphWithWord(int wordId) {
      return true;
   }

   //@Override
   //protected void updateExtensions(Computation computation) {
   //   IntArrayList vertices = getVertices();
   //   IntArrayList edges = getEdges();
   //   int numVertices = getNumVertices();
   //   int numEdges = getNumEdges();
   //   long neighborhoodLookups = 0;

   //   extensionWordIds().clear();

   //   int lowerBound = edges.getUnchecked(0);
   //   int currVertice = numVertices - 1;

   //   for (int i = numEdges - 1; i >= 0; --i) {
   //      int wordId = edges.getUnchecked(i);
   //      extensionWordIdsAdder.setBound(lowerBound);

   //      int numVerticesAdded = numVerticesAddedWithWord.getUnchecked(i);
   //      for (int j = 0; j < numVerticesAdded; ++j) {
   //         int vertexId = vertices.getUnchecked(currVertice);
   //         IntCollection neighbourhood =
   //                 getValidNeighboursForExpansion(vertexId);

   //         if (neighbourhood != null) {
   //            neighbourhood.forEach(extensionWordIdsAdder);
   //            neighborhoodLookups += neighbourhood.size();
   //         }

   //         --currVertice;
   //      }

   //      lowerBound = Math.max(wordId, lowerBound);
   //   }
   //   
   //   computation.getExecutionEngine().aggregate(
   //         SparkFromScratchMasterEngine.NEIGHBORHOOD_LOOKUPS(getNumWords()),
   //         neighborhoodLookups);
   //}
   
   @Override
   protected void updateExtensions(Computation computation) {
      IntArrayList vertices = getVertices();
      IntArrayList edges = getEdges();
      int numVertices = getNumVertices();
      int numEdges = getNumEdges();
      long neighborhoodLookups = 0;
      HashIntSet extensionWordIds = extensionWordIds();

      extensionWordIds.clear();

      int currVertice = numVertices - 1;
      int wordId;
      int lowerBound = edges.getUnchecked(0);
      IntArrayList orderedEdges = null;
      VertexNeighbourhood neighbourhood = null;

      for (int i = numEdges - 1; i >= 0; --i) {
         wordId = edges.getUnchecked(i);

         int numVerticesAdded = numVerticesAddedWithWord.getUnchecked(i);
         for (int j = 0; j < numVerticesAdded; ++j) {
            int vertexId = vertices.getUnchecked(currVertice);
            neighbourhood = configuration.getMainGraph().
                 getVertexNeighbourhood(vertexId);

            if (neighbourhood == null) {
               continue;
            }

            orderedEdges = neighbourhood.getOrderedEdges();
            int numOrderedEdges = orderedEdges.size();
            int fromIdx = neighborhoodCuts.getUnchecked(currVertice);
            if (fromIdx < 0) {
               fromIdx = orderedEdges.binarySearch(edges.getUnchecked(0));
               fromIdx = (fromIdx < 0) ? (-fromIdx - 1) : fromIdx;
               neighborhoodCuts.setUnchecked(currVertice, fromIdx);
            }

            for (int k = fromIdx; k < numOrderedEdges; ++k) {
               int w = orderedEdges.getUnchecked(k);
               if (w > lowerBound) {
                  extensionWordIds.add(w);
               } else {
                  extensionWordIds.removeInt(w);
               }
            }

            neighborhoodLookups += (numOrderedEdges - fromIdx);

            --currVertice;
         }

         lowerBound = Math.max(wordId, lowerBound);
      }
      
      computation.getExecutionEngine().aggregate(
            Configuration.NEIGHBORHOOD_LOOKUPS(getNumWords()),
            neighborhoodLookups);
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      reset();

      init(Configuration.get(in.readInt()));

      edges.readFields(in);

      int numEdges = edges.size();

      for (int i = 0; i < numEdges; ++i) {
         updateVertices(edges.getUnchecked(i));
      }
   }

   @Override
   public void readExternal(ObjectInput objInput)
           throws IOException, ClassNotFoundException {
      readFields(objInput);
   }

   @Override
   public void applyTagFrom(Computation computation,
         AtomicBitSetArray vtag, AtomicBitSetArray etag, int pos) {
      int numEdges = edges.size();
      int numVertices = vertices.size();
      int upperIdx = numVertices - 1;

      for (int i = numEdges - 1; i >= pos; --i) {
         // tag edge
         etag.insert(edges.getUnchecked(i));

         int lowerIdx = upperIdx - numVerticesAddedWithWord.getUnchecked(i);

         for (int j = upperIdx; j > lowerIdx; --j) {
            vtag.insert(vertices.getUnchecked(j));
         }

         upperIdx = lowerIdx;
      }
   }
   
   @Override
   public void applyTagTo(Computation computation,
         AtomicBitSetArray vtag, AtomicBitSetArray etag, int pos) {
      int lowerIdx = 0;

      for (int i = 0; i <= pos; ++i) {
         // tag edge
         etag.insert(edges.getUnchecked(i));

         int upperIdx = lowerIdx + numVerticesAddedWithWord.getUnchecked(i);

         for (int j = lowerIdx; j < upperIdx; ++j) {
            vtag.insert(vertices.getUnchecked(j));
         }

         lowerIdx = upperIdx;
      }
   }
}
