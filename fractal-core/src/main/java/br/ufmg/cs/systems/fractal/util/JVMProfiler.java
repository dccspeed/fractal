package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class JVMProfiler {
   private static final Logger LOG = Logger.getLogger(JVMProfiler.class);

   private Process jvmProfiler;

   private boolean initialized = false;

   private static String getPID() {
      String processName =
         java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
      return processName.split("@")[0];
   }

   public void init(Configuration conf) throws IOException {
      if (initialized) return;

      String cmd = conf.getString(
            Configuration.CONF_JVMPROF_CMD,
            Configuration.CONF_JVMPROF_CMD_DEFAULT);

      if (cmd == null) {
         LOG.info("Profiler command not provided");
         initialized = true;
         return;
      }
      
      cmd = cmd.replace("{{pid}}", getPID());
      LOG.info("Running profiler: " + cmd);
      jvmProfiler = Runtime.getRuntime().exec(cmd);

      BufferedReader stdInput = new BufferedReader(new 
            InputStreamReader(jvmProfiler.getInputStream()));
      BufferedReader stdError = new BufferedReader(new 
            InputStreamReader(jvmProfiler.getErrorStream()));

      String s = null;

      while ((s = stdInput.readLine()) != null) {
         LOG.info("Stdout: " + s);
      }

      while ((s = stdError.readLine()) != null) {
         LOG.info("Stderr: " + s);
      }

      initialized = true;
   }

   private static class JVMProfilerHolder {
      public static final JVMProfiler INSTANCE = new JVMProfiler();
   }

   public static JVMProfiler instance() {
      return JVMProfilerHolder.INSTANCE;
   }
}
