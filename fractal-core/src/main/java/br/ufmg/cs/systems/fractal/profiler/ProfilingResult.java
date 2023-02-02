package br.ufmg.cs.systems.fractal.profiler;

import java.util.Arrays;

public class ProfilingResult {

   private StackTrace[] traces;
   private long numSamplesTotal;
   private double percentageTotal;
   private long timeTotal;
   private long startTime;
   private long finishTime;

   public StackTrace[] getTraces() {
      return traces;
   }

   public long getNumSamplesTotal() {
      return numSamplesTotal;
   }

   public double getPercentageTotal() {
      return percentageTotal;
   }

   public long elapsedTime() {
      return finishTime - startTime;
   }

   public long getTimeTotal() {
      return timeTotal;
   }

   public ProfilingResult(String tracesStr, long startTime, long finishTime) {
      this.startTime = startTime;
      this.finishTime = finishTime;

      final String splitRegex = "--- ";

      numSamplesTotal = 0;
      percentageTotal = 0.0;
      timeTotal = 0;

      String[] traceStrs = tracesStr.split(splitRegex);
      traces = new StackTrace[traceStrs.length - 2];

      for (int i = 2; i < traceStrs.length; ++i) {
         StackTrace st = new StackTrace(traceStrs[i]);
         numSamplesTotal += st.getSamples();
         percentageTotal += st.getPercentage();
         timeTotal += st.getTime();
         traces[i - 2] = st;
      }
   }

   @Override
   public String toString() {
      return Arrays.deepToString(traces);
   }

}
