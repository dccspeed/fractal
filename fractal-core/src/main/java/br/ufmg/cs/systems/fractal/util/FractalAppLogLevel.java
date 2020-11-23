package br.ufmg.cs.systems.fractal.util;

import org.apache.log4j.Level;

public class FractalAppLogLevel extends Level {
   private static final int APP_INT = OFF_INT - 1;

   public static final Level APP = new FractalAppLogLevel(APP_INT, "APP", 10);
   protected FractalAppLogLevel(int level, String levelStr,
                                int syslogEquivalent) {

      super(level, levelStr, syslogEquivalent);
   }
}
