package br.ufmg.cs.systems.fractal.util.collection;

import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListViewPool;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.IntIterator;

import javax.annotation.Nonnull;
import java.io.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

public class IntArrayList implements ReclaimableIntCollection, Externalizable {
   private static final int INITIAL_SIZE = 16;

   protected int[] backingArray;
   protected int numElements;
   private boolean preventReclaim = false;
   private IntConsumer intAdder = new IntAdderConsumer();

   public IntArrayList() {
      this(16);
   }

   public IntArrayList(int capacity) {
      ensureCapacity(capacity);
      this.numElements = 0;
   }

   public IntArrayList(boolean preventReclaim) {
      this();
      this.preventReclaim = preventReclaim;
   }

   public IntArrayList(IntArrayList intArrayList) {
      this(intArrayList.backingArray, intArrayList.numElements);
   }

   public IntArrayList(int[] intArray, int numElements) {
      set(intArray, numElements);
   }

   public IntArrayList(int[] intArray) {
      set(intArray);
   }

   public void set(IntArrayList intArrayList) {
      set(intArrayList.backingArray, intArrayList.numElements);
   }

   public void set(int[] intArray, int numElements) {
      this.numElements = numElements;
      backingArray = Arrays.copyOf(intArray, numElements);
   }

   public void set(int[] intArray) {
      this.numElements = intArray.length;
      backingArray = intArray;
   }

   public int getSize() {
      return numElements;
   }

   public int getCapacity() {
      return backingArray.length;
   }

   public int getRemaining() {
      return getCapacity() - getSize();
   }

   @Override
   public int size() {
      return numElements;
   }

   @Override
   public long sizeAsLong() {
      return numElements;
   }

   @Override
   public boolean ensureCapacity(long l) {
      if (l > Integer.MAX_VALUE) {
         throw new UnsupportedOperationException("IntArrayList does not support long sizes yet");
      }

      int minimumSize = (int) l;

      if (backingArray == null) {
         backingArray = new int[minimumSize];
      }
      else if (minimumSize > backingArray.length) {
         int targetLength = Math.max(backingArray.length, 1);

         while (targetLength < minimumSize) {
            targetLength = targetLength << 1;

            if (targetLength < 0) {
               targetLength = minimumSize;
               break;
            }
         }

         backingArray = Arrays.copyOf(backingArray, targetLength);
      }
      else {
         return false;
      }

      return true;
   }

   @Override
   public boolean shrink() {
      if (backingArray.length == numElements) {
         return false;
      }

      backingArray = Arrays.copyOf(backingArray, numElements);
      return true;
   }

   @Override
   public boolean isEmpty() {
      return size() == 0;
   }

   @Override
   public boolean contains(Object o) {
      return contains((int) o);
   }

   @Override
   public boolean contains(int element) {
      for (int i = 0; i < numElements; ++i) {
         if (backingArray[i] == element) {
            return true;
         }
      }

      return false;
   }

   @Nonnull
   @Override
   public Object[] toArray() {
      return toArray(new Integer[numElements]);
   }

   @Nonnull
   @Override
   public <T> T[] toArray(@Nonnull T[] ts) {
      if (ts.length < numElements) {
         ts = Arrays.copyOf(ts, numElements);
      }

      for (int i = 0; i < numElements; ++i) {
         ts[i] = (T) Integer.valueOf(backingArray[i]);
      }

      if (ts.length > numElements) {
         ts[numElements] = null;
      }

      return ts;
   }

   @Nonnull
   @Override
   public int[] toIntArray() {
      return toArray(new int[numElements]);
   }

   @Nonnull
   @Override
   public int[] toArray(@Nonnull int[] ints) {
      if (ints.length < numElements) {
         return Arrays.copyOf(backingArray, numElements);
      }

      System.arraycopy(backingArray, 0, ints, 0, numElements);

      return ints;
   }

   public int binarySearch(int value) {
      return Arrays.binarySearch(backingArray, 0, numElements, value);
   }

   public int binarySearch(int value, int from, int size) {
      return Arrays.binarySearch(backingArray, from, size, value);
   }

   public void setFrom(IntCollection collection) {
      ensureCapacity(collection.size());
      clear();
      collection.forEach(intAdder);
   }

   public void arrayCopy(IntArrayList src, int srcPos, int destPos, int length) {
      int finalSize = destPos + length;
      ensureCanAddNElements(finalSize);
      System.arraycopy(src.backingArray, src.getIdx(srcPos),
              backingArray, destPos, length);
      numElements = finalSize;
   }

   public int getIdx(int idx) {
      return idx;
   }

   public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeInt(numElements);

      for (int i = 0; i < numElements; ++i) {
         dataOutput.writeInt(backingArray[i]);
      }
   }

   @Override
   public void writeExternal(ObjectOutput objOutput) throws IOException {
      write (objOutput);
   }

   public void readFields(DataInput dataInput) throws IOException {
      clear();

      numElements = dataInput.readInt();

      ensureCanAddNElements(numElements);

      for (int i = 0; i < numElements; ++i) {
         backingArray[i] = dataInput.readInt();
      }
   }

   @Override
   public void readExternal(ObjectInput objInput) throws IOException, ClassNotFoundException {
      readFields(objInput);
   }

   @Override
   public void reclaim() {
      if (preventReclaim) {
         return;
      }

      IntArrayListPool.instance().reclaimObject(this);
   }

   private class IntArrayListCursor implements IntCursor {
      private int index;

      public IntArrayListCursor() {
         this.index = -1;
      }

      @Override
      public void forEachForward(@Nonnull IntConsumer intConsumer) {
         int localNumElements = numElements;

         for (int i = index; i < localNumElements; ++i) {
            intConsumer.accept(backingArray[i]);
         }

         if(localNumElements != numElements) {
            throw new ConcurrentModificationException();
         } else {
            this.index = numElements;
         }
      }

      @Override
      public int elem() {
         if (index < 0 || index >= numElements) {
            throw new IllegalStateException();
         }

         return backingArray[index];
      }

      @Override
      public boolean moveNext() {
         ++index;

         return index >= 0 && index < numElements;
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }
   }

   private class IntArrayListReverseCursor implements IntCursor {
      private int index;

      public IntArrayListReverseCursor() {
         this.index = numElements;
      }

      @Override
      public void forEachForward(@Nonnull IntConsumer intConsumer) {
         throw new UnsupportedOperationException();
      }

      @Override
      public int elem() {
         return backingArray[index];
      }

      @Override
      public boolean moveNext() {
         --index;
         return index >= 0;
      }

      @Override
      public void remove() {
      }
   }

   private class IntArrayListIterator implements IntIterator {
      private int index;

      public IntArrayListIterator() {
         this.index = -1;
      }

      @Override
      public int nextInt() {
         if (index >= numElements - 1) {
            throw new NoSuchElementException();
         }

         return backingArray[++index];
      }

      @Override
      public void forEachRemaining(@Nonnull IntConsumer intConsumer) {
         int localNumElements = numElements;

         for (int i = index + 1; i < localNumElements - 1; ++i) {
            intConsumer.accept(backingArray[i]);
         }

         if (localNumElements != numElements) {
            throw new ConcurrentModificationException();
         } else {
            index = numElements - 1;
         }
      }

      @Override
      public void forEachRemaining(@Nonnull Consumer<? super Integer> intConsumer) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean hasNext() {
         return index < numElements - 1;
      }

      @Override
      public Integer next() {
         return nextInt();
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }
   }

   @Nonnull
   @Override
   public IntCursor cursor() {
      return new IntArrayListCursor();
   }

   public IntCursor reverseCursor() {
      return new IntArrayListReverseCursor();
   }

   @Nonnull
   @Override
   public IntIterator iterator() {
      return new IntArrayListIterator();
   }

   @Override
   public void forEach(@Nonnull IntConsumer intConsumer) {
      for (int i = 0; i < numElements; ++i) {
         intConsumer.accept(backingArray[i]);
      }
   }

   @Override
   public void forEach(@Nonnull Consumer<? super Integer> intConsumer) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean forEachWhile(@Nonnull IntPredicate intPredicate) {
      for (int i = 0; i < numElements; ++i) {
         if (!intPredicate.test(backingArray[i])) {
            return false;
         }
      }

      return true;
   }

   @Override
   public boolean add(@Nonnull Integer integer) {
      return add((int) integer);
   }

   public void addUnchecked(int newValue) {
      backingArray[numElements++] = newValue;
   }

   public boolean add(int newValue) {
      ensureCanAddNewElement();
      backingArray[numElements++] = newValue;
      return true;
   }


   @Override
   public boolean remove(Object o) {
      return removeInt((int) o);
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      for (Object o : c) {
         if (!contains(o)) {
            return false;
         }
      }

      return true;
   }

   @Override
   public boolean addAll(Collection<? extends Integer> c) {
      if (c == null || c.size() == 0) {
         return false;
      }

      ensureCanAddNElements(c.size());

      for (int o : c) {
         add(o);
      }

      return true;
   }

   public boolean addAll(IntCollection c) {
      if (c == null || c.size() == 0) {
         return false;
      }

      ensureCanAddNElements(c.size());

      //if (intAdder == null) {
      //   intAdder = new IntConsumer() {
      //      @Override
      //      public void accept(int i) {
      //         add(i);
      //      }
      //   };
      //}

      c.forEach(intAdder);

      return true;
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      return removeBasedOnCollection(c, true);
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      return removeBasedOnCollection(c, false);
   }

   private boolean removeBasedOnCollection(Collection<?> c, boolean ifPresent) {
      boolean removedAtLeastOne = false;

      for (int i = numElements - 1; i >= 0; --i) {
         int e = backingArray[i];

         boolean collectionContainsE = c.contains(e);

         if (ifPresent == collectionContainsE) {
            remove(i);
            removedAtLeastOne = true;
         }
      }

      return removedAtLeastOne;
   }

   @Override
   public boolean removeInt(int targetValue) {
      for (int i = 0; i < numElements; ++i) {
         int e = backingArray[i];

         if (e == targetValue) {
            remove(i);
            return true;
         }
      }

      return false;
   }

   @Override
   public boolean removeIf(@Nonnull IntPredicate intPredicate) {
      boolean removedAtLeastOne = false;

      for (int i = 0; i < numElements; ++i) {
         if (intPredicate.test(backingArray[i])) {
            removedAtLeastOne = true;
            remove(i);
         }
      }

      return removedAtLeastOne;
   }

   @Override
   public boolean removeIf(@Nonnull Predicate<? super Integer> intPredicate) {
      throw new UnsupportedOperationException();
   }

   public int remove(int index) {
      if (index < 0 || index >= numElements) {
         throw new IllegalArgumentException("Argument: " + this);
      }

      int removedElement = backingArray[index];

      --numElements;

      if (index != numElements) {
         System.arraycopy(backingArray, index + 1, backingArray, index, numElements - index);
      }

      return removedElement;
   }

   public int get(int index) {
      checkIndex(index);
      return getu(index);
   }

   public int getu(int index) {
      return backingArray[index];
   }

   public void set(int index, int newValue) {
      checkIndex(index);
      setu(index, newValue);
   }

   public void increment(int index, int n) {
      backingArray[index]++;
   }

   public void setu(int index, int newValue) {
      backingArray[index] = newValue;
   }

   public void setAndTruncate(int index, int newValue) {
      backingArray[index] = newValue;
      numElements = index + 1;
   }

   public void incUnchecked(int index) {
      backingArray[index] += 1;
   }

   public void clear() {
      numElements = 0;
   }

   public int[] getBackingArray() {
      return backingArray;
   }

   public void sort() {
      Arrays.sort(backingArray, 0, numElements);
   }

   public boolean ensureCapacity(int targetCapacity) {
      return ensureCapacity((long) targetCapacity);
   }

   @Override
   public String toString() {
      StringBuilder strBuilder = new StringBuilder();

      strBuilder.append("[");

      boolean first = true;

      for (int i = 0; i < numElements; ++i) {
         if (!first) {
            strBuilder.append(",");
         }

         strBuilder.append(getu(i));

         first = false;
      }

      strBuilder.append("]");

      return strBuilder.toString();
   }

   protected void checkIndex(int index) {
      if (index < 0 || index >= numElements) {
         throw new ArrayIndexOutOfBoundsException(index);
      }
   }

   private void ensureCanAddNewElement() {
      ensureCanAddNElements(1);
   }

   private void ensureCanAddNElements(int numNewElements) {
      int newTargetSize;

      if (backingArray == null) {
         newTargetSize = Math.max(numNewElements, INITIAL_SIZE);
      } else if (backingArray.length < numElements + numNewElements) {
         newTargetSize = getSizeWithPaddingWithoutOverflow(numNewElements, numElements + numNewElements);
      } else {
         return;
      }

      ensureCapacity(newTargetSize);
   }

   private int getSizeWithPaddingWithoutOverflow(int targetSize, int currentSize) {
      if (currentSize > targetSize) {
         return currentSize;
      }

      int sizeWithPadding = Math.max(currentSize, 1);

      while (true) {
         int previousSizeWithPadding = sizeWithPadding;

         // Multiply by 2
         sizeWithPadding <<= 1;

         // If we saw an overflow, return simple targetSize
         if (previousSizeWithPadding > sizeWithPadding) {
            return targetSize;
         }

         if (sizeWithPadding >= targetSize) {
            return sizeWithPadding;
         }
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      IntArrayList integers = (IntArrayList) o;

      return equals(integers);
   }

   public boolean equals(IntArrayList intArrayList) {
      if (this == intArrayList) return true;
      if (intArrayList == null) return false;

      if (numElements != intArrayList.numElements) return false;

      for (int i = 0; i < numElements; ++i) {
         if (backingArray[i] != intArrayList.backingArray[i]) {
            return false;
         }
      }

      return true;
   }

   public boolean equalsCollection(Collection<Integer> intCollection) {
      if (this == intCollection) return true;
      if (intCollection == null) return false;

      if (numElements != intCollection.size()) return false;

      int i = 0;
      for (Integer e : intCollection) {
         if (backingArray[i] != e) {
            return false;
         }

         ++i;
      }

      return true;
   }

   public boolean equalsIntCollection(IntCollection intCollection) {
      if (this == intCollection) return true;
      if (intCollection == null) return false;

      if (numElements != intCollection.size()) return false;

      IntCursor intCursor = intCollection.cursor();

      int i = 0;
      while (intCursor.moveNext()) {
         if (backingArray[i] != intCursor.elem()) {
            return false;
         }

         ++i;
      }

      return true;
   }

   @Override
   public int hashCode() {
      int result = numElements;

      for (int i = 0; i < numElements; ++i) {
         result = 31 * result + backingArray[i];
      }

      return result;
   }

   public int pop() {
      return remove(numElements - 1);
   }

   public void removeLast() {
      removeLast(1);
   }

   public void removeLast(int n) {
      numElements = Math.max(0, numElements - n);
   }

   public int getLast() {
      int index = numElements - 1;

      if (index >= 0) {
         return backingArray[index];
      }
      else {
         throw new ArrayIndexOutOfBoundsException(index);
      }
   }

   public int getLastOrDefault(int def) {
      int index = numElements - 1;

      if (index >= 0) {
         return backingArray[index];
      }
      else {
         return def;
      }
   }

   public int findLargestCommonPrefixEnd(IntArrayList other) {
      if (other == null) {
         return 0;
      }

      int pos;
      int minPos = Math.min(size(), other.size());

      for (pos = 0; pos < minPos; ++pos) {
         if (getu(pos) != other.getu(pos)) {
            return pos;
         }
      }

      return pos;
   }

   public Iterator<IntArrayList> combinations(int k) {
      return new CombinationsIterator (this, k);
   }

   private class CombinationsIterator implements Iterator<IntArrayList> {
      private IntArrayList source;
      private IntArrayList target;
      private IntArrayList indices;
      private boolean hasNext;
      private int k;
      private int n;

      public CombinationsIterator(IntArrayList source, int k) {
         this.source = source;
         this.indices = new IntArrayList(k);
         this.target = new IntArrayList(k);
         this.k = k;
         this.n = source.size();
         for (int i = 0; i < k; ++i) {
            indices.add (i);
            target.add (i);
         }
         this.hasNext = true;
      }

      public boolean hasNext() {
         return hasNext;
      }

      private boolean loadNext() {
         int i = k - 1;
         indices.setu(i, indices.getu(i) + 1);
         while ((i >= 0) && (indices.getu(i) >= n - k + 1 + i)) {
            --i;
            if (i < 0) return false;
            indices.setu(i, indices.getu(i) + 1);
         }

         if (indices.getu(0) > n - k) {
            return false;
         }

         for (i = i + 1; i < k; ++i)
            indices.setu(i, indices.getu(i - 1) + 1);

         return true;
      }

      public IntArrayList next() {
         for (int i = 0; i < k; ++i)
            target.setu(i, source.getu(indices.getu(i)));
         hasNext = loadNext();
         return target;
      }

      public void remove() {
      }
   }

   public Iterator<IntArrayList> permutations() {
      return new PermutationsIterator(this);
   }

   private class PermutationsIterator implements Iterator<IntArrayList> {
      private IntArrayList source;
      private IntArrayList target;
      private IntArrayList indices;
      private IntArrayList targetIndexes;
      private boolean hasNext;
      private int n;
      private int i;

      public PermutationsIterator(IntArrayList source) {
         this.source = source;
         this.targetIndexes = new IntArrayList(n);
         this.indices = new IntArrayList(n);
         this.target = new IntArrayList(n);
         this.n = source.size();
         for (int i = 0; i < n; ++i) {
            indices.add(0);
            target.add(i);
            targetIndexes.add(i);
         }
         this.hasNext = true;
         this.i = 0;
      }

      public boolean hasNext() {
         return hasNext;
      }

      private boolean loadNext() {
         while (i < n) {
            if (indices.getu(i) < i) {
               int i1 = i % 2 == 0 ? 0 : indices.getu(i);
               int i2 = i;
               int aux = targetIndexes.getu(i1);
               targetIndexes.setu(i1, targetIndexes.getu(i2));
               targetIndexes.setu(i2, aux);
               indices.setu(i, indices.getu(i) + 1);
               i = 0;
               return true;
            } else {
               indices.setu(i, 0);
               ++i;
            }
         }

         return false;
      }

      public IntArrayList next() {
         for (int i = 0; i < n; ++i)
            target.setu(i, source.getu(targetIndexes.getu(i)));
         hasNext = loadNext();
         return target;
      }

      public void remove() {
      }
   }

   public IntArrayListView view(int from, int to) {
      IntArrayListView view = IntArrayListViewPool.instance().createObject();
      view.set(this, from, to);
      return view;
   }

   public void swap(int i, int j) {
      int elem = backingArray[i];
      backingArray[i] = backingArray[j];
      backingArray[j] = elem;
   }

   private class IntAdderConsumer implements IntConsumer {
      @Override

      public void accept(int elem) {
         IntArrayList.this.backingArray[IntArrayList.this.numElements++] = elem;
      }
   }
}
