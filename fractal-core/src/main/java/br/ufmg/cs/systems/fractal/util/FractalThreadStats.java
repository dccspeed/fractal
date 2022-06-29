package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.computation.ExecutionEngine;
import com.koloboke.collect.map.ObjLongMap;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class FractalThreadStats implements Serializable {

   // identification
   public final int step;
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

   // forced termination
   public final boolean forcedTermination;

   public FractalThreadStats(Computation computation) {
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

      forcedTermination = computation.getForcedTermination();
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
   public int hashCode() {
      int h = 1;
      h += 31 * h + step;
      h += 31 * h + stage;
      h += 31 * h + threadId;
      return h;
   }

   @Override
   public boolean equals(Object other) {
      if (!(other instanceof FractalThreadStats)) return false;
      FractalThreadStats otherStats = (FractalThreadStats) other;
      return step == otherStats.step && stage == otherStats.stage && threadId == otherStats.threadId;
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
