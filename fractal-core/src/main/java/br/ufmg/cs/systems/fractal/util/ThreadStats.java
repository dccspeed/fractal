package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.computation.ExecutionEngine;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import com.koloboke.collect.map.IntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.koloboke.collect.set.ObjSet;
import com.koloboke.collect.set.hash.HashObjSets;

import java.io.*;
import java.util.Arrays;

public class ThreadStats implements Serializable {

   private static final IntObjMap<ObjArrayList<ThreadStats>> threadStatuses =
           HashIntObjMaps.newMutableMap();

   public static void add(ThreadStats threadStatus) {
      int stageId = threadStatus.stage;
      ObjArrayList<ThreadStats> stageThreadStatuses = threadStatuses.get(stageId);
      if (stageThreadStatuses == null) {
         stageThreadStatuses = new ObjArrayList<>();
         synchronized (threadStatuses) {
            threadStatuses.put(stageId, stageThreadStatuses);
         }
      }

      stageThreadStatuses.add(threadStatus);
   }

   public static ObjArrayList<ThreadStats> get(int stageId) {
      return threadStatuses.get(stageId);
   }

   public static void remove(int stageId) {
      synchronized (threadStatuses) {
         threadStatuses.remove(stageId);
      }
   }

   // identification
   public final long step;
   public final int stage;
   public final int threadId;

   // timeline
   public final long computationTimeStart;
   public final long computationTimeEnd;
   public final long initTimeStart;
   public final long initTimeEnd;
   public final long computationWorkStealingTimeStart;
   public final long computationWorkStealingTimeEnd;

   // extension counts
   public final long[] numValidExtensions;
   public final long[] numCanonicalExtensions;

   // work stealing counts
   public final long[] numInternalWorkSteals;
   public final long[] numExternalWorkSteals;


   public ThreadStats(Computation computation) {
      ExecutionEngine engine = computation.getExecutionEngine();
      // identification
      step = computation.getStep();
      stage = engine.getStageId();
      threadId = computation.getPartitionId();

      // timeline
      computationTimeStart = engine.getComputationTimeStart();
      computationTimeEnd = engine.getComputationTimeEnd();
      initTimeStart = engine.getInitTimeStart();
      initTimeEnd = engine.getInitTimeEnd();
      computationWorkStealingTimeStart =
              engine.getComputationWorkStealingTimeStart();
      computationWorkStealingTimeEnd =
              engine.getComputationWorkStealingTimeEnd();

      // extension counts and work stealing
      int numComputations = computation.lastComputation().getDepth() + 1;
      numValidExtensions = new long[numComputations];
      numCanonicalExtensions = new long[numComputations];
      numInternalWorkSteals = new long[numComputations];
      numExternalWorkSteals = new long[numComputations];
      Computation currComp = computation;
      for (int i = 0; i < numComputations; ++i) {
         numValidExtensions[i] = currComp.getNumValidExtensions();
         numCanonicalExtensions[i] = currComp.getNumCanonicalExtensions();
         numInternalWorkSteals[i] = currComp.getInternalWorkSteals();
         numExternalWorkSteals[i] = currComp.getExternalWorkSteals();
         currComp = currComp.nextComputation();
      }
   }

   public double initTimeMs() {
      return (initTimeEnd - initTimeStart) * 1e-6;
   }

   public double computationTimeMs() {
      return (computationTimeEnd - computationTimeStart) * 1e-6;
   }

   public double computationWorkStealingTimeMs() {
      return (computationWorkStealingTimeEnd - computationWorkStealingTimeStart)
              * 1e-6;
   }

   @Override
   public String toString() {
      StringBuffer sb = new StringBuffer("ThreadStats(");
      sb.append("step=").append(step);
      sb.append(",stage=").append(stage);
      sb.append(",id=").append(threadId);
      sb.append(",initTimeMs=");
      sb.append(initTimeMs());
      sb.append(",computationTimeMs=");
      sb.append(computationTimeMs());
      sb.append(",computationWorkStealingTimeMs=");
      sb.append(computationWorkStealingTimeMs());
      sb.append(",numValidExtensions=");
      sb.append(Arrays.toString(numValidExtensions));
      sb.append(",numCanonicalExtensions=");
      sb.append(Arrays.toString(numCanonicalExtensions));
      sb.append(",numInternalWorkSteals=");
      sb.append(Arrays.toString(numInternalWorkSteals));
      sb.append(",numExternalWorkSteals=");
      sb.append(Arrays.toString(numExternalWorkSteals));
      sb.append(")");
      return sb.toString();
   }
}
