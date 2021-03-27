package br.ufmg.cs.systems.fractal.profiler;

import br.ufmg.cs.systems.fractal.util.FractalNativeUtils;
import one.profiler.AsyncProfiler;

import java.io.IOException;

public class FractalProfiler {
   private static String nativeLibPath;

   public static synchronized void start(String event, long interval) {
      if (nativeLibPath == null) {
         try {
            nativeLibPath = FractalNativeUtils.getTempFileFromJar("libasyncProfiler.so");
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }

      AsyncProfiler.getInstance(nativeLibPath).start(event, interval);
   }

   public static synchronized ProfilingResult stop() {
      return stop(Integer.MAX_VALUE);
   }

   public static synchronized ProfilingResult stop(int maxtraces) {
      if (nativeLibPath == null) {
         throw new RuntimeException("Start profiler first.");
      }

      AsyncProfiler prof = AsyncProfiler.getInstance(nativeLibPath);
      String tracesStr = prof.dumpTraces(maxtraces);

      return new ProfilingResult(tracesStr);
   }
}
