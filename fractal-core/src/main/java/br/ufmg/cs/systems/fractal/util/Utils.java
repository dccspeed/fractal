package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.IntCollection;

import java.util.List;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

public class Utils {
   public static int sintersectSize(IntArrayList arr1, IntArrayList arr2,
                                    int startIdx1, int endIdx1, int startIdx2,
                                    int endIdx2) {
      int size = 0;
      while (startIdx1 < endIdx1 && startIdx2 < endIdx2) {
         int v1 = arr1.getu(startIdx1);
         int v2 = arr2.getu(startIdx2);
         if (v1 == v2) {
            ++size;
            ++startIdx1;
            ++startIdx2;
         } else if (v1 < v2) {
            ++startIdx1;
         } else {
            ++startIdx2;
         }
      }

      return size;
   }

   public static int sintersect(IntArrayList arr1, IntArrayList arr2,
                                int startIdx1, int endIdx1, int startIdx2,
                                int endIdx2, IntCollection target) {
      int cost = 0;
      while (startIdx1 < endIdx1 && startIdx2 < endIdx2) {
         int v1 = arr1.getu(startIdx1);
         int v2 = arr2.getu(startIdx2);
         if (v1 < v2) {
            ++startIdx1;
         } else if (v1 > v2) {
            ++startIdx2;
         } else {
            target.add(v1);
            ++startIdx1;
            ++startIdx2;
         }
         ++cost;
      }

      return cost;
   }

   public static int sintersectWithKeyPred(IntArrayList arr1,
                                           IntArrayList arr2, int startIdx1,
                                           int endIdx1, int startIdx2,
                                           int endIdx2, IntCollection target,
                                           IntArrayList arr2Keys,
                                           IntPredicate pred) {
      int cost = 0;
      while (startIdx1 < endIdx1 && startIdx2 < endIdx2) {
         int v1 = arr1.getu(startIdx1);
         int v2 = arr2.getu(startIdx2);
         if (v1 < v2) {
            ++startIdx1;
         } else if (v1 > v2) {
            ++startIdx2;
         } else {
            if (pred.test(arr2Keys.getu(startIdx2))) {
               target.add(v1);
            }
            ++startIdx1;
            ++startIdx2;
         }
         ++cost;
      }

      return cost;
   }

   public static int sintersectConsumeWithKeyPred(IntArrayList arr1,
                                                  IntArrayList arr2,
                                                  int startIdx1, int endIdx1,
                                                  int startIdx2, int endIdx2,
                                                  IntConsumer consumer,
                                                  IntArrayList arr2Keys,
                                                  IntPredicate pred) {
      int cost = 0;
      while (startIdx1 < endIdx1 && startIdx2 < endIdx2) {
         int v1 = arr1.getu(startIdx1);
         int v2 = arr2.getu(startIdx2);
         if (v1 < v2) {
            ++startIdx1;
         } else if (v1 > v2) {
            ++startIdx2;
         } else {
            if (pred.test(arr2Keys.getu(startIdx2))) {
               consumer.accept(v1);
            }
            ++startIdx1;
            ++startIdx2;
         }
         ++cost;
      }

      return cost;
   }

   public static int sintersectConsume(IntArrayList arr1, IntArrayList arr2,
                                       int startIdx1, int endIdx1,
                                       int startIdx2, int endIdx2,
                                       IntConsumer consumer) {
      int cost = 0;
      while (startIdx1 < endIdx1 && startIdx2 < endIdx2) {
         int v1 = arr1.getu(startIdx1);
         int v2 = arr2.getu(startIdx2);
         if (v1 < v2) {
            ++startIdx1;
         } else if (v1 > v2) {
            ++startIdx2;
         } else {
            consumer.accept(v1);
            ++startIdx1;
            ++startIdx2;
         }
         ++cost;
      }

      return cost;
   }

   public static int sdifferenceConsume(IntArrayList arr1, IntArrayList arr2,
                                        int startIdx1, int endIdx1,
                                        int startIdx2, int endIdx2,
                                        IntConsumer consumer) {
      int cost = 0;
      while (startIdx1 < endIdx1 && startIdx2 < endIdx2) {
         int v1 = arr1.getu(startIdx1);
         int v2 = arr2.getu(startIdx2);
         if (v1 < v2) {
            consumer.accept(v1);
            ++startIdx1;
         } else if (v1 > v2) {
            ++startIdx2;
         } else {
            ++startIdx1;
            ++startIdx2;
         }
         ++cost;
      }

      while (startIdx1 < endIdx1) consumer.accept(arr1.getu(startIdx1++));

      return cost;
   }

   public static int sdifference(IntArrayList arr1, IntArrayList arr2,
                                 int startIdx1, int endIdx1, int startIdx2,
                                 int endIdx2, IntArrayList target) {
      int cost = 0;
      while (startIdx1 < endIdx1 && startIdx2 < endIdx2) {
         int v1 = arr1.getu(startIdx1);
         int v2 = arr2.getu(startIdx2);
         if (v1 < v2) {
            target.add(v1);
            ++startIdx1;
         } else if (v1 > v2) {
            ++startIdx2;
         } else {
            ++startIdx1;
            ++startIdx2;
         }
         ++cost;
      }

      if (startIdx1 < endIdx1) {
         target.arrayCopy(arr1, startIdx1, target.size(), endIdx1 - startIdx1);
      }

      return cost;
   }

   public static int sunion(IntArrayList arr1, IntArrayList arr2,
                            int startIdx1, int endIdx1, int startIdx2,
                            int endIdx2, IntArrayList target) {
      int cost = 0;
      while (startIdx1 < endIdx1 && startIdx2 < endIdx2) {
         int v1 = arr1.getu(startIdx1);
         int v2 = arr2.getu(startIdx2);
         if (v1 < v2) {
            target.add(v1);
            ++startIdx1;
         } else if (v1 > v2) {
            target.add(v2);
            ++startIdx2;
         } else {
            target.add(v1);
            ++startIdx1;
            ++startIdx2;
         }
         ++cost;
      }

      if (startIdx1 < endIdx1) {
         target.arrayCopy(arr1, startIdx1, target.size(), endIdx1 - startIdx1);
      } else if (startIdx2 < endIdx2) {
         target.arrayCopy(arr2, startIdx2, target.size(), endIdx2 - startIdx2);
      }

      return cost;
   }

   public static void swap(IntArrayList arr1, int i1, int i2) {
      int tmp = arr1.getu(i1);
      arr1.setu(i1, arr1.getu(i2));
      arr1.setu(i2, tmp);
   }
}
