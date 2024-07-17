package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.graph.VELabeledMainGraph;
import org.apache.log4j.Logger;

import java.io.*;

public class PythonFilterRunner {
   private static final Logger LOG =
           Logger.getLogger(PythonFilterRunner.class);
   private Process process;
   private BufferedReader inputStreamReader;
   private OutputStreamWriter outputStreamWriter;
   public PythonFilterRunner(String filterstr) {
      // start process
      String filterRunnerPath = System.getenv("PYFRACTAL_LIB") + "/filterrunner.py";
      ProcessBuilder processBuilder = new ProcessBuilder("python", filterRunnerPath, filterstr);
      processBuilder.redirectErrorStream(false);

      process = null;
      try {
         process = processBuilder.start();
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
      inputStreamReader =
              new BufferedReader(new InputStreamReader(process.getInputStream()));
      outputStreamWriter = new OutputStreamWriter(process.getOutputStream());
   }

   public boolean test(String subgraphstr) {
      // send subgraphstr to process
      int result;
      try {
         outputStreamWriter.write(subgraphstr + "\n");
         outputStreamWriter.flush();
         // read result from process
         result = inputStreamReader.read();
      } catch (IOException e) {
         throw new RuntimeException(e);
      }

      if (result == '1') {
         return true;
      } else {
         return false;
      }
   }

   @Override
   protected void finalize() throws Throwable {
      super.finalize();
      outputStreamWriter.write("CLOSE\n"); // force process to finish
      outputStreamWriter.flush();
   }
}
