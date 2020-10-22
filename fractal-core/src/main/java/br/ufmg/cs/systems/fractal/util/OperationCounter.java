package br.ufmg.cs.systems.fractal.util;

public class OperationCounter {
   public static final boolean ENABLED = true;
   private static final int MAX_DEPTH = 21;
   private static final int MAX_THREADS = 50;

   private static long[][] validSubgraphs =
           new long[MAX_THREADS][MAX_DEPTH];
   private static long[][] canonicalSubgraphs =
           new long[MAX_THREADS][MAX_DEPTH];
   private static long[][] extensionIterations =
           new long[MAX_THREADS][MAX_DEPTH];

   public static void clear(int threadId) {
      for (int i = 0; i < MAX_THREADS; ++i) {
         for (int j = 0; j < MAX_DEPTH; ++j) {
            validSubgraphs[i][j] = 0;
            canonicalSubgraphs[i][j] = 0;
            extensionIterations[i][j] = 0;
         }
      }
   }

   public static long getValidSubgraphs(int threadId, int depth) {
      return validSubgraphs[threadId][depth];
   }

   public static long getCanonicalSubgraphs(int threadId, int depth) {
      return canonicalSubgraphs[threadId][depth];
   }

   public static long getExtensionIterations(int threadId, int depth) {
      return extensionIterations[threadId][depth];
   }

   public static void addValidSubgraphs(int threadId, int depth, long n) {
      validSubgraphs[threadId][depth] += n;
   }

   public static void addCanonicalSubgraphs(int threadId, int depth, long n) {
      canonicalSubgraphs[threadId][depth] += n;
   }

   public static void addExtensionIterations(int threadId, int depth, long n) {
      extensionIterations[threadId][depth] += n;
   }

}
