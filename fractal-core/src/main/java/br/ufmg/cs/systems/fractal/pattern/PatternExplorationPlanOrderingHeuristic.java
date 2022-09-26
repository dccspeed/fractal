package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import com.koloboke.collect.ObjCursor;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.ObjObjCursor;
import com.koloboke.collect.map.ObjObjMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.ObjSet;
import com.koloboke.collect.set.hash.HashIntSets;
import com.koloboke.collect.set.hash.HashObjSets;
import org.apache.log4j.Logger;

import java.util.function.BiConsumer;

public class PatternExplorationPlanOrderingHeuristic extends PatternExplorationPlan {
   private static final Logger LOG = Logger.getLogger(
           PatternExplorationPlanOrderingHeuristic.class);

   private final FindMaximumCost findMaximumCost = new FindMaximumCost();

   private static int compareCosts(IntArrayList c1, IntArrayList c2) {
      for (int i = 0; i < c1.size(); ++i) {
         int e1 = c1.get(i);
         int e2 = c2.get(i);
         if (e1 < e2) return -1;
         else if (e1 > e2) return 1;
      }
      return 0;
   }

   public void updateWithHeuristicPlan(Pattern pattern,
                                       IntIntMap vertexLabelFrequencies) {
      int numVertices = pattern.getNumberOfVertices();
      PatternEdgeArrayList edges = pattern.getEdges();

      pattern.updateSymmetryBreaker();
      ObjArrayList<IntArrayList> sbLowerBound =
              pattern.vsymmetryBreakerLowerBound();
      ObjArrayList<IntArrayList> sbUpperBound =
              pattern.vsymmetryBreakerUpperBound();

      // 2. create adjacency lists for the pattern
      ObjArrayList<IntArrayList> adjLists = new ObjArrayList<>();
      IntArrayList vertexLabelMap = new IntArrayList(numVertices);

      for (int i = 0; i < numVertices; ++i) {
         adjLists.add(new IntArrayList());
         vertexLabelMap.add(-1);
      }

      for (PatternEdge pedge : edges) {
         int src = pedge.getSrcPos();
         int dst = pedge.getDestPos();
         int srcLabel = pedge.getSrcLabel();
         int dstLabel = pedge.getDestLabel();
         adjLists.get(src).add(dst);
         adjLists.get(dst).add(src);
         vertexLabelMap.set(src, srcLabel);
         vertexLabelMap.set(dst, dstLabel);
      }

      // 3. initialize cost map
      ObjObjMap<IntArrayList,IntArrayList> costMap =
              HashObjObjMaps.newMutableMap();
      ObjObjMap<IntArrayList,IntArrayList> newCostMap =
              HashObjObjMaps.newMutableMap();
      ObjObjCursor<IntArrayList,IntArrayList> mapCursor;

      for (int u = 0; u < numVertices; ++u) {
         // key
         IntArrayList ordering = new IntArrayList(numVertices);
         ordering.add(u);

         // value
         int ulabel = vertexLabelMap.get(u);
         int f = vertexLabelFrequencies.get(ulabel);
         IntArrayList cost = new IntArrayList(3);
         cost.add(-f);
         cost.add(0);
         cost.add(0);

         costMap.put(ordering, cost);
      }

      // 4. remove entries that are not maximum
      int numEntriesBefore = costMap.size();
      findMaximumCost.reset();
      costMap.forEach(findMaximumCost);
      IntArrayList bestCost = findMaximumCost.getMaximumCost();
      mapCursor = costMap.cursor();
      while (mapCursor.moveNext()) {
         IntArrayList cost = mapCursor.value();
         if (compareCosts(cost, bestCost) < 0) mapCursor.remove();
      }
      int numEntriesAfter = costMap.size();

      LOG.debug(String.format("CostMap entriesBefore=%d entriesAfter=%d " +
                      "bestCost=%s", numEntriesBefore, numEntriesAfter, bestCost));

      // 5. repeat by growing the orderings
      IntSet orderingSet = HashIntSets.newUpdatableSet(numVertices);
      IntIntMap candidates = HashIntIntMaps.newMutableMap();
      for (int i = 1; i < numVertices; ++i) {
         ObjObjCursor<IntArrayList, IntArrayList> ordCostCur = costMap.cursor();
         newCostMap.clear();
         while (ordCostCur.moveNext()) {
            IntArrayList ordering = ordCostCur.key();
            IntArrayList cost = ordCostCur.value();
            orderingSet.clear();
            orderingSet.addAll(ordering);

            candidates.clear();
            for (int j = 0; j < ordering.size(); ++j) {
               int u = ordering.get(j);
               IntArrayList neighbors = adjLists.get(u);
               for (int k = 0; k < neighbors.size(); ++k) {
                  int v = neighbors.get(k);
                  candidates.put(v, candidates.getOrDefault(v, 0) + 1);
               }
            }

            for (int j = 0; j < ordering.size(); ++j) {
               candidates.remove(ordering.get(j));
            }

            IntIntCursor candidatesCur = candidates.cursor();
            while (candidatesCur.moveNext()) {
               int v = candidatesCur.key();
               int vEdges = candidatesCur.value();

               // label frequency
               int vlabel = vertexLabelMap.get(v);
               int f = vertexLabelFrequencies.get(vlabel);

               // partial orderings
               int numPartialOrderings = 0;
               IntArrayList sbConditions = sbLowerBound.get(v);
               for (int j = 0; j < sbConditions.size(); ++j) {
                  if (orderingSet.contains(sbConditions.get(j))) ++numPartialOrderings;
               }

               sbConditions = sbUpperBound.get(v);
               for (int j = 0; j < sbConditions.size(); ++j) {
                  if (orderingSet.contains(sbConditions.get(j))) ++numPartialOrderings;
               }

               // density
               int density = cost.get(2) + vEdges;

               // key
               IntArrayList newOrdering = new IntArrayList(ordering.size() + 1);
               newOrdering.addAll(ordering);
               newOrdering.add(v);

               // value
               IntArrayList newCost = new IntArrayList(3);
               newCost.add(-f);
               newCost.add(numPartialOrderings);
               newCost.add(density);

               newCostMap.put(newOrdering, newCost);
            }
         }

         numEntriesBefore = newCostMap.size();
         findMaximumCost.reset();
         newCostMap.forEach(findMaximumCost);
         bestCost = findMaximumCost.getMaximumCost();
         mapCursor = newCostMap.cursor();
         while (mapCursor.moveNext()) {
            IntArrayList cost = mapCursor.value();
            if (compareCosts(cost, bestCost) < 0) mapCursor.remove();
         }
         numEntriesAfter = newCostMap.size();

         LOG.debug(String.format("CostMap entriesBefore=%d entriesAfter=%d " +
                         "bestCost=%s", numEntriesBefore, numEntriesAfter, bestCost));

         ObjObjMap aux = costMap;
         costMap = newCostMap;
         newCostMap = aux;
      }

      ObjCursor<IntArrayList> cur = costMap.keySet().cursor();
      cur.moveNext();
      IntArrayList chosenOrdering = cur.elem();

      IntIntMap relabeling = HashIntIntMaps.newUpdatableMap();
      for (int u = 0; u < numVertices; ++u) {
         relabeling.put(chosenOrdering.getu(u), u);
      }

      LOG.debug("before relabeling " + pattern + " " + pattern.vsymmetryBreakerLowerBound() + " " +
              pattern.vsymmetryBreakerUpperBound());
      pattern.relabel(relabeling);
      LOG.debug("after relabeling " + pattern);
      pattern.getEdges().sort();
      LOG.debug("after increasing positions " + pattern + " " + pattern.vsymmetryBreakerLowerBound() + " " + pattern.vsymmetryBreakerUpperBound());

      // fill exploration plan according to this ordering
      super.updateWithNaivePlan(pattern);
   }

   public static ObjArrayList<Pattern> apply(Pattern pattern,
                                             IntIntMap vertexLabelFrequencies) {
      PatternExplorationPlanOrderingHeuristic
              explorationPlan = new PatternExplorationPlanOrderingHeuristic();
      Pattern newPattern = pattern.copy();
      PatternUtils.increasingPositions(newPattern);
      explorationPlan.updateWithHeuristicPlan(newPattern, vertexLabelFrequencies);
      newPattern.setExplorationPlan(explorationPlan);
      ObjArrayList<Pattern> patterns = new ObjArrayList<>();
      patterns.add(newPattern);
      return patterns;
   }

   @Override
   public String toString() {
      return "heuristic{intersections=" + intersectionIdxs +
              ", differences=" + differenceIdxs +
              ", vertexPredicates=" + vertexPredicates +
              ", edgePredicates=" + edgePredicates + "}";
   }

   private class FindMaximumCost
           implements BiConsumer<IntArrayList,IntArrayList> {

      private IntArrayList maximumCost;

      public void reset() {
         this.maximumCost = null;
      }

      public IntArrayList getMaximumCost() {
         return maximumCost;
      }

      @Override
      public void accept(IntArrayList ordering, IntArrayList cost) {
         if (maximumCost == null || compareCosts(cost, maximumCost) > 0) {
            maximumCost = cost;
         }
      }
   }
}
