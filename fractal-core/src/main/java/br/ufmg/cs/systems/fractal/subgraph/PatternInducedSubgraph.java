package br.ufmg.cs.systems.fractal.subgraph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.pattern.PatternEdge;
import br.ufmg.cs.systems.fractal.pattern.PatternEdgeArrayList;
import br.ufmg.cs.systems.fractal.pattern.PatternExplorationPlan;
import br.ufmg.cs.systems.fractal.util.EdgePredicates;
import br.ufmg.cs.systems.fractal.callback.SubgraphCallback;
import br.ufmg.cs.systems.fractal.util.Utils;
import br.ufmg.cs.systems.fractal.util.VertexPredicate;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayListView;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import org.apache.log4j.Logger;

import java.util.function.BiPredicate;
import java.util.function.IntConsumer;

public class PatternInducedSubgraph extends BasicSubgraph {
   private static final Logger LOG =
           Logger.getLogger(PatternInducedSubgraph.class);

   private static final BiPredicate<PatternInducedSubgraph,
           Computation<PatternInducedSubgraph>>
           truePredicate = (s, c) -> true;

   private IntSet extensionsSet;
   private UpdateEdgesConsumer updateEdgesConsumer;
   private EdgePredicates edgePredicates;
   private VertexPredicate vertexPredicate;
   private IntArrayList intersection;
   private IntArrayList difference;
   private IntArrayList starts;
   private IntArrayList ends;
   private IntArrayList originalVertices;
   private IntArrayList verticesBackup;
   private ObjArrayList<IntArrayList> neighborhoods;
   private ObjArrayList<IntArrayList> neighborhoodsToReclaim;

   public PatternInducedSubgraph() {
      super();
      updateEdgesConsumer = new UpdateEdgesConsumer();
      intersection = new IntArrayList();
      difference = new IntArrayList();
      neighborhoods = new ObjArrayList<>();
      neighborhoodsToReclaim = new ObjArrayList<>();
      originalVertices = new IntArrayList();
      verticesBackup = new IntArrayList();
      starts = new IntArrayList();
      ends = new IntArrayList();
      extensionsSet = HashIntSets.newMutableSet();
   }

   public Pattern applyLabels(Pattern unlabeledPattern) {
      if (dirtyPattern) {
         if (pattern == null) {
            pattern = unlabeledPattern.copy();
            pattern.init(configuration);
         }

         // fix vertex ids
         IntArrayList patternVertices = pattern.getVertices();
         for (int i = 0; i < vertices.size(); ++i) {
            patternVertices.set(i, vertices.getu(i));
         }

         // fix pattern edges, TODO: consider edge labels
         PatternEdgeArrayList patternEdges = pattern.getEdges();
         MainGraph graph = getConfig().getMainGraph();
         for (int i = 0; i < patternEdges.size(); ++i) {
            PatternEdge pedge = patternEdges.getu(i);
            pedge.setSrcLabel(
                    graph.firstVertexLabel(patternVertices.getu(pedge.getSrcPos())));
            pedge.setDestLabel(graph.firstVertexLabel(
                    patternVertices.getu(pedge.getDestPos())));
         }

         dirtyPattern = false;
      }

      return pattern;
   }

   public long completeMatch(Computation computation, Pattern pattern,
                             SubgraphCallback callback) {
      return completeMatch(computation, pattern, truePredicate, callback);
   }

   public long completeMatch(Computation computation, Pattern pattern,
                             BiPredicate<PatternInducedSubgraph,
                                     Computation<PatternInducedSubgraph>> predicate,
                             SubgraphCallback callback) {
      PatternExplorationPlan explorationPlan = pattern.explorationPlan();
      int numOrderings = explorationPlan.numOrderings();

      if (numOrderings > 1) {
         originalVertices.clear();
         originalVertices.addAll(vertices);
      }

      long validSubgraphs =
              completeMatch(computation, pattern, predicate, callback,
                      explorationPlan, numOrderings, 0);

      if (numOrderings > 1) {
         vertices.clear();
         vertices.addAll(originalVertices);
      }

      return validSubgraphs;
   }

   private long completeMatch(Computation computation, Pattern pattern,
                              BiPredicate<PatternInducedSubgraph,
                                      Computation<PatternInducedSubgraph>> predicate,
                              SubgraphCallback callback,
                              PatternExplorationPlan explorationPlan,
                              int numOrderings, int nextOrdering) {
      int numVertices = pattern.getNumberOfVertices();
      int nextVertexPos = getNumVertices();
      MainGraph graph = pattern.getConfig().getMainGraph();

      /**
       * Maybe change the minimum cover match order to reflect the new ordering
       */
      if (nextOrdering - 1 >= 0) {
         //verticesBackup.clear();
         //verticesBackup.addAll(vertices);
         IntArrayList previousOrdering =
                 explorationPlan.ordering(nextOrdering - 1);
         IntArrayList ordering = explorationPlan.ordering(nextOrdering);
         for (int j = 0; j < ordering.size(); ++j) {
            //vertices.setu(previousOrdering.getu(j),
            //        verticesBackup.getu(ordering.getu(j)));
            vertices.setu(previousOrdering.getu(j),
                    originalVertices.getu(ordering.getu(j)));
         }
      }

      /**
       * Here we guarantee that the neighborhoods has proper size and all
       * elements equal to null.
       * Also, we return existing arrays to pool
       */
      neighborhoods.clear();
      for (int i = 0; i < numVertices; ++i) {
         neighborhoods.add(null);
      }

      /**
       * Clear and maybe reclaim temporary neighborhoods
       */
      for (int i = 0; i < neighborhoodsToReclaim.size(); ++i) {
         neighborhoodsToReclaim.getu(i).reclaim();
      }
      neighborhoodsToReclaim.clear();

      /**
       * Make sure the neighborhoods that will be queried exists
       */
      for (int pos = nextVertexPos; pos < numVertices; ++pos) {
         ensureNeighborhoods(graph, pattern, nextVertexPos, pos);
      }

      /**
       * Effectively match this ordering
       */
      long validSubgraphs =
              completeMatchRec(computation, pattern, predicate, callback);

      if (nextOrdering >= numOrderings - 1) { // reached last ordering
         return validSubgraphs;
      } else { // accumulate with other orderings
         return validSubgraphs +
                 completeMatch(computation, pattern, predicate, callback,
                         explorationPlan, numOrderings, nextOrdering + 1);
      }
   }

   private long completeMatchRec(Computation computation, Pattern pattern,
                                 BiPredicate<PatternInducedSubgraph,
                                         Computation<PatternInducedSubgraph>> predicate,
                                 SubgraphCallback callback) {
      long validSubgraphs = 0;
      int numVertices = getNumVertices();
      IntArrayList validExtensions = neighborhoods.getu(numVertices);
      PatternExplorationPlan explorationPlan = pattern.explorationPlan();
      IntArrayList differences = explorationPlan.difference(numVertices);
      VertexPredicate vertexPredicate =
              explorationPlan.vertexPredicate(numVertices);

      if (differences.size() > 0) {
         MainGraph graph = computation.getConfig().getMainGraph();
         IntArrayList result = IntArrayListPool.instance().createObject();
         IntArrayList previous = IntArrayListPool.instance().createObject();
         IntArrayListView nextNeighborhood =
                 graph.neighborhoodVertices(vertices.getu(differences.getu(0)));
         Utils.sdifference(validExtensions, nextNeighborhood, 0,
                 validExtensions.size(), 0, nextNeighborhood.size(), result);

         for (int i = 1; i < differences.size(); ++i) {
            IntArrayList aux = result;
            result = previous;
            previous = aux;
            graph.neighborhoodVertices(vertices.getu(differences.getu(i)),
                    nextNeighborhood);
            result.clear();
            Utils.sdifference(previous, nextNeighborhood, 0, previous.size(), 0,
                    nextNeighborhood.size(), result);
         }

         nextNeighborhood.reclaim();
         previous.reclaim();
         validExtensions = result;
      }

      int lowerBound = pattern.sbLowerBound(this, getNumVertices());
      int startIdx = validExtensions.binarySearch(lowerBound);
      startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx + 1;
      if (getNumVertices() < pattern.getNumberOfVertices() - 1) {
         for (; startIdx < validExtensions.size(); ++startIdx) {
            int u = validExtensions.getu(startIdx);
            if (!vertexPredicate.test(u) || vertices.contains(u)) continue;
            addWord(u);
            if (predicate.test(this, computation)) {
               validSubgraphs +=
                       completeMatchRec(computation, pattern, predicate,
                               callback);
            }
            removeLastWord();
         }
      } else {
         for (; startIdx < validExtensions.size(); ++startIdx) {
            int u = validExtensions.getu(startIdx);
            if (!vertexPredicate.test(u) || vertices.contains(u)) continue;
            addWord(u);
            validSubgraphs += 1;
            if (predicate.test(this, computation)) {
               callback.apply(this, computation);
            }
            removeLastWord();
         }
      }

      if (differences.size() > 0) {
         validExtensions.reclaim();
      }

      return validSubgraphs;
   }

   private void ensureNeighborhoods(MainGraph graph, Pattern pattern,
                                    int mcvcSize, int pos) {
      if (neighborhoods.getu(pos) != null) return;
      /**
       * Single vertices: neighborhood view
       */
      if (pos < mcvcSize) {
         IntArrayList view = graph.neighborhoodVertices(vertices.getu(pos));
         neighborhoods.setu(pos, view);
         neighborhoodsToReclaim.add(view);
         return;
      }

      /**
       * Ensure dependencies are satisfied
       */
      PatternExplorationPlan explorationPlan = pattern.explorationPlan();
      IntArrayList intersections = explorationPlan.intersection(pos);
      for (int i = 0; i < intersections.size(); ++i) {
         ensureNeighborhoods(graph, pattern, mcvcSize, intersections.getu(i));
      }

      /**
       * In case intersections is single element, just get reference
       */
      if (intersections.size() == 1) {
         int dep = intersections.getu(0);
         neighborhoods.setu(pos, neighborhoods.getu(dep));
         return;
      }

      /**
       * In case intersections have two elements
       */
      if (intersections.size() == 2) {
         IntArrayList neighborhood1 = neighborhoods.getu(intersections.getu(0));
         IntArrayList neighborhood2 = neighborhoods.getu(intersections.getu(1));
         IntArrayList result = IntArrayListPool.instance().createObject();
         Utils.sintersect(neighborhood1, neighborhood2, 0, neighborhood1.size(),
                 0, neighborhood2.size(), result);
         neighborhoods.setu(pos, result);
         neighborhoodsToReclaim.add(result);
         return;
      }

      /**
       * In case intersections have more than two elements
       */
      IntArrayList result = IntArrayListPool.instance().createObject();
      IntArrayList previous = IntArrayListPool.instance().createObject();
      previous.clear();
      IntArrayList neighborhood1 = neighborhoods.getu(intersections.getu(0));
      IntArrayList neighborhood2 = neighborhoods.getu(intersections.getu(1));
      Utils.sintersect(neighborhood1, neighborhood2, 0, neighborhood1.size(), 0,
              neighborhood2.size(), result);

      for (int i = 2; i < intersections.size(); ++i) {
         IntArrayList aux = result;
         result = previous;
         previous = aux;
         neighborhood1 = neighborhoods.getu(intersections.getu(i));
         result.clear();
         Utils.sintersect(previous, neighborhood1, 0, previous.size(), 0,
                 neighborhood1.size(), result);
      }

      neighborhoods.setu(pos, result);
      neighborhoodsToReclaim.add(result);
      IntArrayListPool.instance().reclaimObject(previous);
   }

   public IntArrayList getEdges(Pattern pattern) {
      ensureEdges(pattern);
      return edges;
   }

   private void ensureEdges(Pattern pattern) {
      int numVertices = getNumVertices();
      edges.clear();

      PatternExplorationPlan explorationPlan = pattern.explorationPlan();
      //int edgeIdx = 0;
      for (int u = 1; u < numVertices; ++u) {
         IntArrayList edgesIdxs = explorationPlan.vertexEdges(u);
         //edgeIdx += edgesIdxs.size();
         //if (edgeIdx > edges.size()) {
            // add edges
            for (int i = 0; i < edgesIdxs.size(); ++i) {
               PatternEdge pedge = pattern.getEdges().getu(edgesIdxs.getu(i));
               configuration.getMainGraph()
                       .forEachEdge(vertices.getu(pedge.getSrcPos()),
                               vertices.getu(pedge.getDestPos()),
                               updateEdgesConsumer);
            }
         //}
      }

      // remove old edges, if any
      //edges.removeLast(edges.size() - edgeIdx);
   }

   @Override
   public String toString() {
      return "p" + super.toString();
   }

   @Override
   public void init(Configuration configuration) {
      super.init(configuration);
   }

   @Override
   public IntArrayList getEdges() {
      throw new UnsupportedOperationException("Use getEdges(pattern) instead");
   }

   /**
    * Add word and update the number of vertices in this subgraph.
    *
    * @param word
    */
   @Override
   public void addWord(int word) {
      super.addWord(word);
      vertices.add(word);
   }

   @Override
   public void removeLastWord() {
      vertices.removeLast();
      super.removeLastWord();
   }

   @Override
   public void computeExtensions(Computation computation,
                                 IntArrayList extensions) {
      int numVertices = getNumVertices();
      Pattern pattern = computation.getPattern();
      PatternExplorationPlan explorationPlan = pattern.explorationPlan();
      IntArrayList intersectionIdx = explorationPlan.intersection(numVertices);
      IntArrayList differenceIdx = explorationPlan.difference(numVertices);

      /**
       * Find the lower bound on vertex ids concerning symmetry breaking
       * conditions
       */
      int lowerBound = pattern.sbLowerBound(this, numVertices);
      int upperBound = pattern.sbUpperBound(this, numVertices);

      /**
       * Neighborhood traversal: intersection and/or difference among
       * neighborhoods
       */
      if (!pattern.vertexLabeled()) {
         getConfig().getMainGraph()
                 .validExtensionsPatternInduced(computation, this,
                         intersectionIdx, differenceIdx, starts, ends,
                         lowerBound, upperBound, extensionsSet);
      } else {
         vertexPredicate = explorationPlan.vertexPredicate(numVertices);
         edgePredicates = explorationPlan.edgePredicates(numVertices);
         getConfig().getMainGraph()
                 .validExtensionsPatternInducedLabeled(computation, this,
                         intersectionIdx, differenceIdx, starts, ends,
                         lowerBound, upperBound, vertexPredicate,
                         edgePredicates, extensionsSet);
      }

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
      Pattern pattern = computation.getPattern();
      int totalNumWords = computation.getInitialNumWords();
      int numPartitions = computation.getNumberPartitions();
      int myPartitionId = computation.getPartitionId();

      VertexPredicate vertexPredicate =
              pattern.explorationPlan().vertexPredicate(0);
      boolean vertexLabeled = pattern.vertexLabeled();

      for (int u = myPartitionId; u < totalNumWords; u += numPartitions) {
         if (!vertexLabeled || vertexPredicate.test(u)) extensions.add(u);
      }
   }

   @Override
   public void reset() {
      super.reset();
      intersection.clear();
      difference.clear();
      neighborhoods.clear();
      for (int i = 0; i < neighborhoodsToReclaim.size(); ++i) {
         neighborhoodsToReclaim.getu(i).reclaim();
      }
      neighborhoodsToReclaim.clear();
      originalVertices.clear();
      verticesBackup.clear();
      extensionsSet.clear();
   }

   @Override
   public long computeExtensionCost(IntArrayList extensionCandidates) {
      long cost = Long.MAX_VALUE, numEdges = 0;
      for (int i = 0; i < extensionCandidates.size(); ++i) {
         int numExtensions = extensionCandidates.getu(i);
         if (numExtensions >= 0) {
            cost = Math.min(cost, extensionCandidates.getu(i));
            numEdges++;
         }
      }
      return cost * numEdges;
   }

   @Override
   public IntArrayList getWords() {
      return vertices;
   }

   @Override
   public int getNumWords() {
      return vertices.size();
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
      // TODO: get this information via pattern edges
      throw new UnsupportedOperationException();
   }

   private class UpdateEdgesConsumer implements IntConsumer {
      @Override
      public void accept(int i) {
         edges.add(i);
      }
   }
}
