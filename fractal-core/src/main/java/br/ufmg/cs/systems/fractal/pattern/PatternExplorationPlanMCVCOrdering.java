package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.util.EdgePredicate;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntIntMapPool;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.ObjObjMap;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

public class PatternExplorationPlanMCVCOrdering extends PatternExplorationPlan {
   private static final Logger LOG = Logger.getLogger(PatternExplorationPlanMCVCOrdering.class);

   protected ObjArrayList<IntArrayList> vgroupOrderings;

   public PatternExplorationPlanMCVCOrdering() {
      super();
      vgroupOrderings = new ObjArrayList<>();
   }

   @Override
   public void init(Pattern pattern) {
      super.init(pattern);
   }

   @Override
   protected void reset(Pattern pattern) {
      super.reset(pattern);
      vgroupOrderings.clear();
   }

   @Override
   public int mcvcSize() {
      return vgroupOrderings.get(0).size();
   }

   @Override
   public int numOrderings() {
      return vgroupOrderings.size();
   }

   @Override
   public IntArrayList ordering(int i) {
      return vgroupOrderings.getu(i);
   }

   private static void relabelMCVC(Pattern pattern, IntArrayList mcvc) {
      PatternEdgeArrayList patternEdges = pattern.getEdges();
      int i;
      IntIntMap labeling = IntIntMapPool.instance().createObject();

      for (i = 0; i < mcvc.size(); ++i) {
         labeling.put(mcvc.get(i), i);
      }

      for (PatternEdge pedge : patternEdges) {
         labeling.putIfAbsent(pedge.getSrcPos(), labeling.size());
         labeling.putIfAbsent(pedge.getDestPos(), labeling.size());
      }

      // sanity check
      for (i = 0; i < mcvc.size(); ++i) {
         if (labeling.get(mcvc.getu(i)) != i) {
            throw new RuntimeException("Labeling does not match vertex cover order");
         }
      }

      pattern.relabel(labeling);
      patternEdges.sort();

      IntIntMapPool.instance().reclaimObject(labeling);
   }

   private static int updateInitialPlan(Pattern pattern, IntArrayList mcvc) {
      int numVertices = pattern.getNumberOfVertices();
      PatternExplorationPlan explorationPlan = pattern.explorationPlan();
      PatternEdgeArrayList patternEdges = pattern.getEdges();
      int i;
      int numCoverEdges = 0;

      relabelMCVC(pattern, mcvc);

      explorationPlan.reset(pattern);

      boolean edgeLabeled = pattern.edgeLabeled();

      /**
       * Create initial plan
       */
      for (i = 0; i < patternEdges.size(); ++i) {
         PatternEdge pedge = patternEdges.get(i);
         explorationPlan.intersectionIdxs.get(pedge.getDestPos()).add(pedge.getSrcPos());
         explorationPlan.vertexPredicates.get(pedge.getDestPos()).setLabel(pedge.getDestLabel());
         EdgePredicate edgePredicate;
         if (edgeLabeled) {
            edgePredicate = new EdgePredicate();
            edgePredicate.setLabel(pedge.getLabel());
         } else {
            edgePredicate = EdgePredicate.trueEdgePredicate;
         }
         explorationPlan.edgePredicates.get(pedge.getDestPos()).add(edgePredicate);
         if (pedge.getSrcPos() < mcvc.size() && pedge.getDestPos() < mcvc.size()) {
            ++numCoverEdges;
         }
      }

      if (pattern.induced()) {
         for (int dst = 1; dst < numVertices; ++dst) {
            for (int src = 0; src < dst; ++src) {
               if (!explorationPlan.intersectionIdxs.get(dst).contains(src)) {
                  explorationPlan.differenceIdxs.get(dst).add(src);
               }
            }
         }
      }

      /**
       * Improve plan for vertices not in the MCVC
       */
      for (int intersectionSize = 1; intersectionSize < mcvc.size(); ++intersectionSize) {
         for (int dst = mcvc.size(); dst < numVertices; ++dst) {
            IntArrayList intersection1 = explorationPlan.intersectionIdxs.get(dst);
            for (int dst2 = mcvc.size(); dst2 < numVertices; ++dst2) {
               IntArrayList intersection2 = explorationPlan.intersectionIdxs.get(dst2);
               if (dst == dst2 || intersection2.size() > intersection1.size()) continue;

               // this means we may reuse the intersection
               if (intersection1.containsAll(intersection2)) {
                  intersection1.removeAll(intersection2);
                  intersection1.add(dst2);
               }
            }
         }
      }

      return numCoverEdges;
   }

   /**
    * Generates a minimum connected vertex cover for the pattern given. Note that this is a brute-force algorithm,
    * but ok for using with small patterns
    * @param pattern existing pattern
    * @return an array with a minimum connected vertex cover
    */
   public static ObjArrayList<IntArrayList> minimumConnectedVertexCover(Pattern pattern) {
      ObjArrayList<IntArrayList> minCovers = new ObjArrayList<>();
      for (int numCoverVertices = 1; numCoverVertices < pattern.getNumberOfVertices(); numCoverVertices++) {
         Iterator<IntArrayList> covers = pattern.getVertices().combinations(numCoverVertices);
         while (covers.hasNext()) {
            IntArrayList coverCandidate = covers.next();

            // check if this is a valid connected cover
            boolean validCover = true;
            for (PatternEdge pedge : pattern.getEdges()) {
               if (!coverCandidate.contains(pedge.getSrcPos()) && !coverCandidate.contains(pedge.getDestPos())) {
                  validCover = false;
                  break;
               }
            }

            if (validCover && pattern.connectedValidOrdering(coverCandidate)) {
               minCovers.add(new IntArrayList(coverCandidate));
            }
         }
         if (minCovers.size() > 0) {
            break;
         }
      }
      return minCovers;
   }

   public static ObjArrayList<Pattern> worstExecutions(Pattern pattern) {
      ObjArrayList<Pattern> worstPlan = null;
      for (ObjArrayList<Pattern> plan : allExecutions(pattern)) {
         if (worstPlan == null || worstPlan.size() < plan.size()) {
            worstPlan = plan;
         }
      }

      return worstPlan;
   }

   public static ObjArrayList<Pattern> apply(Pattern pattern) {
      ObjArrayList<Pattern> bestPlan = null;
      for (ObjArrayList<Pattern> plan : allExecutions(pattern)) {
         if (bestPlan == null || bestPlan.size() > plan.size()) {
            bestPlan = plan;
         }
      }

      return bestPlan;
   }

   public static ObjArrayList<ObjArrayList<Pattern>> allExecutions(Pattern pattern) {
      ObjArrayList<ObjArrayList<Pattern>> executions = new ObjArrayList<>();
      ObjArrayList<IntArrayList> mcvcs = minimumConnectedVertexCover(pattern);
      for (int i = 0; i < mcvcs.size(); ++i) {
         executions.addAll(allExecutionsWithCover(pattern, mcvcs.get(i)));
      }

      return executions;
   }

   private static ObjArrayList<ObjArrayList<Pattern>> allExecutionsWithCover(Pattern pattern, IntArrayList mcvc) {
      ObjArrayList<ObjArrayList<Pattern>> executions = new ObjArrayList<>();
      Iterator<IntArrayList> mcvcIterator = mcvc.permutations();

      while (mcvcIterator.hasNext()) {
         Pattern newPattern = pattern.copy();
         IntArrayList nextMcvc = mcvcIterator.next();
         executions.add(executions(newPattern, nextMcvc));
      }

      return executions;
   }

   private static ObjArrayList<Pattern> executions(Pattern pattern, IntArrayList mcvc) {
      pattern.setExplorationPlan(new PatternExplorationPlanMCVCOrdering());
      int numCoverEdges = updateInitialPlan(pattern, mcvc);

      ObjObjMap<Pattern, ObjArrayList<IntArrayList>> vgroupSequences = HashObjObjMaps.newMutableMap();

      for (int i = 0; i < mcvc.size(); ++i) mcvc.set(i, i);
      Iterator<IntArrayList> vertexOrderings = mcvc.permutations();
      while (vertexOrderings.hasNext()) {
         IntArrayList ordering = vertexOrderings.next();
         if (pattern.sbValidOrdering(ordering)) {
            Pattern newPattern = pattern.copy();
            relabelMCVC(newPattern, ordering);
            newPattern.removeLastNEdges(newPattern.getNumberOfEdges() - numCoverEdges);

            ObjArrayList<IntArrayList> orderings = vgroupSequences.getOrDefault(newPattern, new ObjArrayList<>());
            orderings.add(new IntArrayList(ordering));
            vgroupSequences.putIfAbsent(newPattern, orderings);
         }
      }

      ObjArrayList<Pattern> newPatterns = new ObjArrayList<>(vgroupSequences.size());

      for (ObjArrayList<IntArrayList> vgroupOrderings : vgroupSequences.values()) {
         for (IntArrayList ordering : vgroupOrderings) {
            Pattern newPattern = pattern.copy();
            PatternExplorationPlanMCVCOrdering explorationPlanMCVC = new PatternExplorationPlanMCVCOrdering();
            newPattern.setExplorationPlan(explorationPlanMCVC);
            updateInitialPlan(newPattern, mcvc);
            explorationPlanMCVC.vgroupOrderings.clear();
            explorationPlanMCVC.vgroupOrderings.add(ordering);
            newPattern.updateSymmetryBreaker(ordering);
            newPatterns.add(newPattern);
         }
      }

      return newPatterns;
   }

   @Override
   public String toString() {
      return "mcvc-orderings{" + super.toString() + " orderings=" + vgroupOrderings + "}";
   }

   @Override
   public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeInt(vgroupOrderings.size());
      for (int i = 0; i < vgroupOrderings.size(); ++i) {
         IntArrayList ordering = vgroupOrderings.getu(i);
         ordering.write(out);
      }
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      int size = in.readInt();
      for (int i = 0; i < size; ++i) {
         IntArrayList ordering = new IntArrayList();
         ordering.readFields(in);
         vgroupOrderings.add(ordering);
      }
   }
}
