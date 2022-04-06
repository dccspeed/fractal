package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.Fractoid;
import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.profiler.ProfilingResult;
import br.ufmg.cs.systems.fractal.profiler.StackTrace;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import com.koloboke.collect.map.IntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;

import java.io.Serializable;
import java.text.NumberFormat;
import java.util.*;

public class FractalPerformanceDiagnose implements Serializable {
   public String[] primitives;
   public long[] validSubgraphs;
   public long[] totalValidSubgraphs;
   public double elapsedTime;
   public double totalThreadsTime, totalThreadsTimePerc;
   public double Eperc, Fperc, Aperc, Nperc;
   public double Etime, Ftime, Atime, Ntime;

   public FractalPerformanceDiagnose(int maxNumPrimitives) {
      this.validSubgraphs = new long[maxNumPrimitives];
      this.totalValidSubgraphs = new long[maxNumPrimitives];
   }

   public <S extends Subgraph> void addThreadStatuses(Fractoid<S> fractoid,
                                                      FractalThreadStats[] threadStatuses) {

      List<String> primitiveStrs = new ArrayList<>();
      IntArrayList numPrimitivesInFractoids = new IntArrayList();
      IntArrayList offsets = new IntArrayList();

      // primitives header
      Fractoid frac = fractoid;
      while (frac != null) {
         List<String> primitivesAux = new ArrayList<>();
         Computation computation = frac.computationContainer();
         while (computation != null) {
            primitivesAux.add(computation.asPrimitiveString());
            computation = computation.nextComputation();
         }
         numPrimitivesInFractoids.add(primitivesAux.size());
         primitivesAux.addAll(primitiveStrs);
         primitiveStrs = primitivesAux;
         frac = frac.parent();
      }

      offsets.add(0);
      for (int i = 1; i < numPrimitivesInFractoids.size(); ++i) {
         int j = numPrimitivesInFractoids.size() - i;
         offsets.add(offsets.get(i-1) + numPrimitivesInFractoids.get(j));
      }

      String[] newPrimitives = new String[primitiveStrs.size()];
      primitiveStrs.toArray(newPrimitives);
      if (this.primitives == null) {
         this.primitives = newPrimitives;
      } else if (!Arrays.equals(newPrimitives, this.primitives)) {
         //throw new RuntimeException("Cannot aggregate different programs." +
         //        Arrays.toString(newPrimitives) + " " + Arrays.toString(primitives));
      }

      if (this.validSubgraphs == null) {
         this.validSubgraphs = new long[primitives.length];
      }

      if (this.totalValidSubgraphs == null) {
         this.totalValidSubgraphs = new long[primitives.length];
      }

      IntSet steps = HashIntSets.newUpdatableSet();
      for (FractalThreadStats threadStats : threadStatuses) {
         steps.add(threadStats.step);
      }

      int[] stepsArray = steps.toIntArray();
      Arrays.sort(stepsArray);

      IntObjMap<ObjArrayList<FractalThreadStats>> threadStatusesPerStep =
              HashIntObjMaps.newUpdatableMap();

      for (FractalThreadStats threadStats : threadStatuses) {
         int step = threadStats.step;
         ObjArrayList<FractalThreadStats> stepThreadStatuses =
                 threadStatusesPerStep.get(step);
         if (stepThreadStatuses == null) {
            stepThreadStatuses = new ObjArrayList<>();
            threadStatusesPerStep.put(step, stepThreadStatuses);
         }

         stepThreadStatuses.add(threadStats);
      }

      for (int i = 0; i < stepsArray.length; ++i) {
         int step = stepsArray[i];
         int offset = offsets.get(i);
         ObjArrayList<FractalThreadStats> stepThreadStatuses = threadStatusesPerStep.get(step);
         for (int j = 0; j < stepThreadStatuses.size(); ++j) {
            FractalThreadStats threadStats = stepThreadStatuses.get(j);
            int numPrimitives = threadStats.numValidExtensions.length;
            for (int k = 0; k < numPrimitives; ++k) {
               validSubgraphs[offset + k] += threadStats.numValidExtensions[k];
            }
            if (i == stepsArray.length - 1) {
               totalValidSubgraphs[offset + numPrimitives - 1] +=
                       threadStats.numValidExtensions[numPrimitives - 1];
            }
         }
      }
   }

   public void addProfilingResult(ProfilingResult profilingResult) {

      long totalTime = profilingResult.getTimeTotal();
      double totalTimePerc = profilingResult.getPercentageTotal();
      double Eperc, Fperc, Aperc, Nperc;
      long Etime, Ftime, Atime, Ntime;
      Eperc = Fperc = Aperc = Nperc = 0.0;
      Etime = Ftime = Atime = Ntime = 0;

      for (StackTrace t : profilingResult.getTraces()) {
         if (t.getPrimitive() == Primitive.E) {
            Eperc += t.getPercentage();
            Etime += t.getTime();
         } else if (t.getPrimitive() == Primitive.F) {
            Fperc += t.getPercentage();
            Ftime += t.getTime();
         } else if (t.getPrimitive() == Primitive.A) {
            Aperc += t.getPercentage();
            Atime += t.getTime();
         } else {
            Nperc += t.getPercentage();
            Ntime += t.getTime();
         }
      }

      this.totalThreadsTime = totalTime * 1e-6;
      this.Etime = Etime * 1e-6;
      this.Ftime = Ftime * 1e-6;
      this.Atime = Atime * 1e-6;
      this.Ntime = Ntime * 1e-6;

      this.totalThreadsTimePerc = totalTimePerc;
      this.Eperc = Eperc;
      this.Fperc = Fperc;
      this.Aperc = Aperc;
      this.Nperc = Nperc;

      this.elapsedTime = profilingResult.elapsedTime() * 1e-6;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < primitives.length; ++i) {
         sb.append(String.format("%s", primitives[i]));
         sb.append(";");
         sb.append(String.format("%s;%s",
                 NumberFormat.getInstance().format(validSubgraphs[i]),
                 NumberFormat.getInstance().format(totalValidSubgraphs[i])));
         sb.append("\n");
      }

      sb.append("\n");
      sb.append(String.format("%s;%.1f;%.1f%%\n",
              "ExtendMs", Etime, Eperc));
      sb.append(String.format("%s;%.1f;%.1f%%\n",
              "FilterMs", Ftime, Fperc));
      sb.append(String.format("%s;%.1f;%.1f%%\n",
              "AggregationMs", Atime, Aperc));
      sb.append(String.format("%s;%.1f;%.1f%%\n",
              "None", Ntime, Nperc));
      sb.append(String.format("%s;%.1f;%.1f%%\n",
              "TotalThreadsTimeMs", totalThreadsTime, totalThreadsTimePerc));
      sb.append(String.format("%s;%.1f\n",
              "ElapsedTimeNoSetupOverheadMs", elapsedTime));

      return sb.toString();
   }
}
