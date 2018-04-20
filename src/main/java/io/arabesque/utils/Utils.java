package io.arabesque.utils;

import java.util.*;

import java.util.concurrent.ThreadLocalRandom;

public class Utils {
  // Implementing Fisher–Yates shuffle
  public static void shuffleArray(int[] ar) {
    Random rnd = ThreadLocalRandom.current();
    for (int i = ar.length - 1; i > 0; i--) {
      int index = rnd.nextInt(i + 1);
      // Simple swap
      int a = ar[index];
      ar[index] = ar[i];
      ar[i] = a;
    }
  }
}
