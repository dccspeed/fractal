package br.ufmg.cs.systems.fractal.util;

import org.apache.log4j.Logger;

import java.io.*;

public class FractalNativeUtils {
   private static final Logger LOG = Logger.getLogger(FractalNativeUtils.class);

   public static void loadNativeLibFromJar(String libFileName)
           throws IOException {
      String tempPath = getTempFileFromJar(libFileName);
      LOG.info("Loading native library from temporary file " + tempPath);
      System.load(tempPath);
   }

   public static String getTempFileFromJar(String fileName) throws IOException {
      InputStream is = FractalNativeUtils.class.getResourceAsStream("/" + fileName);
      if (is == null) {
         throw new RuntimeException(fileName + " not found in jar.");
      }

      File temp = File.createTempFile(fileName, "fractal-resource-file");
      temp.deleteOnExit();

      if (!temp.exists()) {
         throw new RuntimeException("Unable to create temp file for lib.");
      }

      byte[] buffer = new byte[1024];
      int readBytes;

      // Open output stream and copy data between source file in JAR and the temporary file
      OutputStream os = new FileOutputStream(temp);
      try {
         while ((readBytes = is.read(buffer)) != -1) {
            os.write(buffer, 0, readBytes);
         }
      } finally {
         // If read/write fails, close streams safely before throwing an exception
         os.close();
         is.close();
      }

      return temp.getAbsolutePath();
   }
}
