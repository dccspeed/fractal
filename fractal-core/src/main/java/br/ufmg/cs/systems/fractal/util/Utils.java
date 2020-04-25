package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.IntCollection;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

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

   public static int sintersect(IntArrayList arr1, IntArrayList arr2,
                                int i1, int size1, int i2, int size2, IntCollection target) {
      int cost = 0;
      while (i1 < size1 && i2 < size2) {
         int v1 = arr1.getUnchecked(i1);
         int v2 = arr2.getUnchecked(i2);
         if (v1 == v2) {
            target.add(v1);
            ++i1;
            ++i2;
         } else if (v1 < v2) {
            ++i1;
         } else {
            ++i2;
         }
         ++cost;
      }

      return cost;
   }

   public static void sintersectWithKeyPred(IntArrayList arr1, IntArrayList arr2,
                                           int i1, int size1, int i2, int size2, IntCollection target,
                                           IntArrayList arr2Keys, IntPredicate pred) {
      while (i1 < size1 && i2 < size2) {
         int v1 = arr1.getUnchecked(i1);
         int v2 = arr2.getUnchecked(i2);
         if (v1 == v2) {
            if (pred.test(arr2Keys.getUnchecked(i2))) {
               target.add(v1);
            }
            ++i1;
            ++i2;
         } else if (v1 < v2) {
            ++i1;
         } else {
            ++i2;
         }
      }
   }

   public static int sintersect(IntArrayList arr1, IntArrayList arr2,
                                int i1, int size1, int i2, int size2, IntCollection target, IntPredicate pred) {
      int cost = 0;
      while (i1 < size1 && i2 < size2) {
         int v1 = arr1.getUnchecked(i1);
         int v2 = arr2.getUnchecked(i2);
         if (v1 == v2) {
            if (pred.test(v1)) {
               target.add(v1);
            }
            ++i1;
            ++i2;
         } else if (v1 < v2) {
            ++i1;
         } else {
            ++i2;
         }
         ++cost;
      }
      return cost;
   }

   public static void sintersectConsumeWithKeyPred(IntArrayList arr1, IntArrayList arr2,
                                                   int i1, int size1, int i2, int size2, IntConsumer consumer,
                                                   IntArrayList arr2Keys, IntPredicate pred) {
      while (i1 < size1 && i2 < size2) {
         int v1 = arr1.getUnchecked(i1);
         int v2 = arr2.getUnchecked(i2);
         if (v1 == v2) {
            if (pred.test(arr2Keys.getUnchecked(i2))) {
               consumer.accept(v1);
            }
            ++i1;
            ++i2;
         } else if (v1 < v2) {
            ++i1;
         } else {
            ++i2;
         }
      }
   }

  public static void sintersectConsume(IntArrayList arr1, IntArrayList arr2,
                                       int i1, int size1, int i2, int size2, IntConsumer consumer) {
     while (i1 < size1 && i2 < size2) {
        int v1 = arr1.getUnchecked(i1);
        int v2 = arr2.getUnchecked(i2);
        if (v1 == v2) {
           consumer.accept(v1);
           ++i1;
           ++i2;
        } else if (v1 < v2) {
           ++i1;
        } else {
           ++i2;
        }
     }
  }

  public static void sdifferenceConsume(IntArrayList arr1, IntArrayList arr2,
                                        int i1, int size1, int i2, int size2, IntConsumer consumer) {
     while (i1 < size1 && i2 < size2) {
        int v1 = arr1.getUnchecked(i1);
        int v2 = arr2.getUnchecked(i2);
        if (v1 == v2) {
           ++i1;
           ++i2;
        } else if (v1 < v2) {
           consumer.accept(v1);
           ++i1;
        } else {
           ++i2;
        }
     }

     while (i1 < size1) {
        consumer.accept(arr1.getUnchecked(i1));
        ++i1;
     }
  }

  public static int sdifference(IntArrayList arr1, IntArrayList arr2,
                                int i1, int size1, int i2, int size2, IntCollection target) {
     int cost = 0;
     while (i1 < size1 && i2 < size2) {
        int v1 = arr1.getUnchecked(i1);
        int v2 = arr2.getUnchecked(i2);
        if (v1 == v2) {
           ++i1;
           ++i2;
        } else if (v1 < v2) {
           target.add(v1);
           ++i1;
        } else {
           ++i2;
        }
        ++cost;
     }

     while (i1 < size1) {
        target.add(arr1.getUnchecked(i1));
        ++i1;
     }

     return cost;
  }

  public static int sunion(IntArrayList arr1, IntArrayList arr2,
                           int i1, int size1, int i2, int size2, IntCollection target) {
     int cost = 0;
     while (i1 < size1 && i2 < size2) {
        int v1 = arr1.getUnchecked(i1);
        int v2 = arr2.getUnchecked(i2);
        if (v1 == v2) {
           target.add(v1);
           ++i1;
           ++i2;
        } else if (v1 < v2) {
           target.add(v1);
           ++i1;
        } else {
           target.add(v2);
           ++i2;
        }
        ++cost;
     }

     while (i1 < size1) {
        target.add(arr1.getUnchecked(i1));
        ++i1;
     }

     while (i2 < size2) {
        target.add(arr2.getUnchecked(i2));
        ++i2;
     }

     return cost;
  }
}
