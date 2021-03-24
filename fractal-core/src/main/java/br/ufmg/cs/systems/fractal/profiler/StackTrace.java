package br.ufmg.cs.systems.fractal.profiler;

import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import org.apache.log4j.Logger;

public class StackTrace {
   private static final Logger LOG = Logger.getLogger(StackTrace.class);

   private long time;
   private double percentage;
   private long samples;
   private String[] stack;

   public long getTime() {
      return time;
   }

   public double getPercentage() {
      return percentage;
   }

   public long getSamples() {
      return samples;
   }

   public String[] getStack() {
      return stack;
   }

   public Primitive getPrimitive() {
      return primitive;
   }

   private Primitive primitive;

   public StackTrace(String traceStr) {
      String[] lines = traceStr.trim().split("\n");

      stack = new String[lines.length - 1];

      // first line
      String[] tokens = lines[0].split(" ");
      time = Long.parseLong(tokens[0]);
      percentage = Double.parseDouble(
              tokens[2].substring(1, tokens[2].length() - 3));
      samples = Long.parseLong(tokens[3]);

      primitive = null;

      for (int i = 1; i < lines.length; ++i) {
         tokens = lines[i].trim().split("] ");
         stack[i - 1] = tokens[1].trim();

         if (primitive != null) continue;

         if (stack[i - 1].contains("EXTENSION_PRIMITIVE")) {
            primitive = Primitive.E;
         } else if (stack[i - 1].contains("AGGREGATION_PRIMITIVE")) {
            primitive = Primitive.A;
         } else if (stack[i - 1].contains("FILTERING_PRIMITIVE")) {
            primitive = Primitive.F;
         }
      }

      if (primitive == null) {
         primitive = Primitive.None;
      }
   }

   @Override
   public String toString() {
      return "StackTrace(time=" + time + ",percentage=" + percentage +
              ",samples=" + samples + ",size=" + stack.length +
              ",primitive=" + primitive +
              ",topStack=" + stack[0] + ")";
   }
}
