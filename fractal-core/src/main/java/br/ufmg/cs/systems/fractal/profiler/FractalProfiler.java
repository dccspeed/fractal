package br.ufmg.cs.systems.fractal.profiler;

import br.ufmg.cs.systems.fractal.computation.BasicComputation;
import br.ufmg.cs.systems.fractal.util.FractalNativeUtils;
import one.profiler.AsyncProfiler;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Wrapper used for built-in profiling.
 */
public class FractalProfiler {
   private static final Logger LOG = Logger.getLogger(FractalProfiler.class);
   private static String nativeLibPath;
   private static AsyncProfiler profiler;

   /**
    * Start profiling current JVM
    * @param event events supported depend on the JVM, see
    *              [[one.profiler.Events]] for a list with most common ones
    * @param interval determines the interval between data collection
    */
   public static synchronized void start(String event, long interval) {
      if (profiler == null) {
         try {
            nativeLibPath = FractalNativeUtils.getTempFileFromJar("libasyncProfiler.so");
         } catch (IOException e) {
            throw new RuntimeException(e);
         }

         profiler = AsyncProfiler.getInstance(nativeLibPath);
         LOG.info("Loaded AsyncProfiler v" + profiler.getVersion());
      }

      profiler.start(event, interval);
   }

   /**
    * Stop profiling current JVM
    * @return result object containing collected data
    */
   public static synchronized ProfilingResult stop() {
      return stop(Integer.MAX_VALUE);
   }

   /**
    * Stop profiling current JVM
    * @param maxtraces limit the number of collected traces to consider
    * @return result object containing collected data
    */
   public static synchronized ProfilingResult stop(int maxtraces) {
      if (nativeLibPath == null) {
         throw new RuntimeException("Start profiler first.");
      }

      AsyncProfiler prof = AsyncProfiler.getInstance(nativeLibPath);
      String tracesStr = prof.dumpTraces(maxtraces);

      return new ProfilingResult(tracesStr);
   }
}
