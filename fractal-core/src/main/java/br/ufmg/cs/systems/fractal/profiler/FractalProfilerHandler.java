package br.ufmg.cs.systems.fractal.profiler;

import br.ufmg.cs.systems.fractal.util.FractalNativeUtils;
import one.profiler.AsyncProfiler;
import org.apache.log4j.Logger;
import org.spark_project.jetty.server.HttpChannelState;

import java.io.IOException;

/**
 * Wrapper used for built-in profiling.
 */
public class FractalProfilerHandler {
   private static final Logger LOG = Logger.getLogger(FractalProfilerHandler.class);

   private final AsyncProfiler profiler;
   private final long startTime;
   private long finishTime;

   public FractalProfilerHandler(AsyncProfiler profiler) {
      this.profiler = profiler;
      this.startTime = System.nanoTime();
   }

   /**
    * Stop profiling current JVM
    * @return result object containing collected data
    */
   public synchronized ProfilingResult stop() {
      return stop(Integer.MAX_VALUE);
   }

   /**
    * Stop profiling current JVM
    * @param maxtraces limit the number of collected traces to consider
    * @return result object containing collected data
    */
   public synchronized ProfilingResult stop(int maxtraces) {
      this.finishTime = System.nanoTime();
      String tracesStr = profiler.dumpTraces(maxtraces);
      return new ProfilingResult(tracesStr, startTime, finishTime);
   }
}
