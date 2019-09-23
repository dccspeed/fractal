package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.util.collection.IntCollectionAddConsumer;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;

import java.util.Arrays;

public class EdgePositionEquivalences {
   private IntSet[] equivalences;
   private int numEdges;
   private IntCollectionAddConsumer intAddConsumer = new IntCollectionAddConsumer();
    
   public EdgePositionEquivalences() {
        this.equivalences = null;
        this.numEdges = -1;
    }

   public EdgePositionEquivalences(IntSet[] equivalences) {
      this.equivalences = equivalences;
      this.numEdges = equivalences.length;
   }

   public IntSet getEquivalences(int pos) {
      return equivalences[pos];
   }

   public void setNumEdges(int numEdges) {
      if (this.numEdges != numEdges) {
         ensureCapacity(numEdges);
         this.numEdges = numEdges;
      }
   }
    
   public void addEquivalence(int pos1, int pos2) {
      equivalences[pos1].add(pos2);
   }

   private void ensureCapacity(int n) {
      int numSetsToCreate = n;

      if (equivalences == null) {
         equivalences = new HashIntSet[n];
      }
      else if (equivalences.length < n) {
         numSetsToCreate -= equivalences.length;
         equivalences = Arrays.copyOf(equivalences, n);
      }

      int newSize = equivalences.length;
      int targetI = newSize - numSetsToCreate;

      for (int i = newSize - 1; i >= targetI; --i) {
         equivalences[i] = HashIntSets.newMutableSet();
      }
   }

   public void clear() {
      for (int i = 0; i < equivalences.length; ++i) {
         equivalences[i].clear();
      }
   }

   public void propagateEquivalences() {
      for (int i = 0; i < numEdges; ++i) {
         IntSet currentEdgeEquivalences = equivalences[i];

         if (currentEdgeEquivalences != null) {
            IntCursor cursor = currentEdgeEquivalences.cursor();

            while (cursor.moveNext()) {
               int equivalentPosition = cursor.elem();

               if (equivalentPosition == i) {
                  continue;
               }

               intAddConsumer.setCollection(equivalences[equivalentPosition]);
               currentEdgeEquivalences.forEach(intAddConsumer);
            }
         }
      }
   }

   public int getNumEdges() {
      return numEdges;
   }

   @Override
   public String toString() {
      return "EdgePositionEquivalences{" +
         "equivalences=" + Arrays.toString(equivalences) +
         ", numEdges=" + numEdges +
         '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      EdgePositionEquivalences that = (EdgePositionEquivalences) o;

      if (numEdges != that.numEdges) return false;

      for (int i = 0; i < numEdges; ++i) {
         // Only enter if one is null and the other isn't
         if ((equivalences[i] == null) ^ (that.equivalences[i] == null)) {
            return false;
         }

         if (equivalences[i] != null && !equivalences[i].equals(that.equivalences[i])) {
            return false;
         }
      }

      return true;
   }

   @Override
   public int hashCode() {
      int result = numEdges;

      for (int i = 0; i < numEdges; ++i) {
         result = 31 * result + (equivalences[i] != null ? equivalences[i].hashCode() : 0);
      }

      return result;
   }

   public boolean isEmpty() {
      return numEdges == 0;
   }
}
