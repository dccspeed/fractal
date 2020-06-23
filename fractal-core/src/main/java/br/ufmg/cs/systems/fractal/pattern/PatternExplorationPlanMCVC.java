package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.util.EdgePredicate;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntIntMapPool;
import com.koloboke.collect.map.IntIntMap;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Iterator;

public class PatternExplorationPlanMCVC extends PatternExplorationPlan {
   private static final Logger LOG = Logger.getLogger(PatternExplorationPlanMCVC.class);

   protected int mcvcSize;

   @Override
   public int mcvcSize() {
      return mcvcSize;
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

   private static int updateInitalPlan(Pattern pattern, IntArrayList mcvc) {
      int numVertices = pattern.getNumberOfVertices();
      PatternExplorationPlan explorationPlan = pattern.explorationPlan();
      PatternEdgeArrayList patternEdges = pattern.getEdges();
      int i;
      int numCoverEdges = 0;

      relabelMCVC(pattern, mcvc);

      explorationPlan.reset(pattern);

      /**
       * Create initial plan
       */
      for (i = 0; i < patternEdges.size(); ++i) {
         PatternEdge pedge = patternEdges.get(i);
         explorationPlan.intersectionIdxs.get(pedge.getDestPos()).add(pedge.getSrcPos());
         explorationPlan.vertexPredicates.get(pedge.getDestPos()).setLabel(pedge.getDestLabel());
         explorationPlan.vertexPredicates.get(pedge.getSrcPos()).setLabel(pedge.getSrcLabel());
         EdgePredicate edgePredicate = new EdgePredicate();
         edgePredicate.setLabel(pedge.getLabel());
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


      // set vertex to edges mapping
      PatternEdgeArrayList pedges = pattern.getEdges();
      for (int u = 1; u < numVertices; ++u) {
         for (i = 0; i < pedges.size(); ++i) {
            PatternEdge pedge = pedges.get(i);
            if (pedge.getDestPos() == u) {
               explorationPlan.vertexToEdges.get(u).add(i);
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
            Iterator<IntArrayList> iter = coverCandidate.permutations();

            while (iter.hasNext()) {
               IntArrayList coverCandidatePer = iter.next();
               // check if this is a valid connected cover
               boolean validCover = true;
               for (PatternEdge pedge : pattern.getEdges()) {
                  if (!coverCandidatePer.contains(pedge.getSrcPos())
                          && !coverCandidatePer.contains(pedge.getDestPos())) {
                     validCover = false;
                     break;
                  }
               }

               if (validCover
                       && pattern.connectedValidOrdering(coverCandidatePer)) {
                  minCovers.add(new IntArrayList(coverCandidatePer));
               }
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
      Pattern reorderedPattern = pattern.copy();
      PatternUtils.increasingPositions(reorderedPattern);
      for (ObjArrayList<Pattern> plan : allExecutions(reorderedPattern)) {
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
         IntArrayList mcvc = mcvcs.get(i);
         if (pattern.connectedValidOrdering(mcvc)) {
            executions.addAll(allExecutionsWithCover(pattern, mcvc));
         }
      }

      return executions;
   }

   private static ObjArrayList<ObjArrayList<Pattern>> allExecutionsWithCover(Pattern pattern, IntArrayList mcvc) {
      ObjArrayList<ObjArrayList<Pattern>> executions = new ObjArrayList<>();
      ObjArrayList<Pattern> newPatterns = new ObjArrayList<>(1);
      Pattern newPattern = pattern.copy();
      PatternExplorationPlanMCVC explorationPlanMCVC = new PatternExplorationPlanMCVC();
      explorationPlanMCVC.mcvcSize = mcvc.size();
      newPattern.setExplorationPlan(explorationPlanMCVC);
      updateInitalPlan(newPattern, mcvc);
      newPattern.updateSymmetryBreaker();
      newPatterns.add(newPattern);
      executions.add(newPatterns);

      return executions;
   }

   @Override
   public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeInt(mcvcSize);
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      mcvcSize = in.readInt();
   }

   @Override
   public void writeExternal(ObjectOutput objectOutput) throws IOException {
      write(objectOutput);
   }

   @Override
   public void readExternal(ObjectInput objectInput) throws IOException {
      readFields(objectInput);
   }

   @Override
   public String toString() {
      return "mcvc{" + super.toString() + ", mcvcSize=" + mcvcSize + "}";
   }

}
