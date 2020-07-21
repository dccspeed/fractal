package br.ufmg.cs.systems.fractal.util.collection;

import br.ufmg.cs.systems.fractal.util.pool.IntArrayListViewPool;
import com.koloboke.collect.IntCursor;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.function.IntConsumer;

public final class IntArrayListView extends IntArrayList{
   private int offset;

   public IntArrayListView() {
   }

   public IntArrayListView set(IntArrayList underlying, int from, int to) {
      this.backingArray = underlying.backingArray;
      this.numElements = to - from;
      this.offset = from;
      return this;
   }

   @Override
   public int get(int i) {
      checkIndex(i);
      return backingArray[offset + i];
   }

   @Override
   public int getu(int i) {
      return backingArray[offset + i];
   }

   @Override
   protected void checkIndex(int i) {
      if (i < offset || i >= offset + numElements) {
         throw new ArrayIndexOutOfBoundsException(i);
      }
   }

   @Override
   public void reclaim() {
      IntArrayListViewPool.instance().reclaimObject(this);
   }

   @Override
   public void forEach(@Nonnull IntConsumer intConsumer) {
      for (int i = offset; i < offset + numElements; ++i) {
         intConsumer.accept(backingArray[i]);
      }
   }

   @Nonnull
   @Override
   public IntCursor cursor() {
      return new IntArrayListViewCursor();
   }

   private class IntArrayListViewCursor implements IntCursor {
      private int index;

      public IntArrayListViewCursor() {
         this.index = offset - 1;
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
         checkIndex(index);
         return backingArray[index];
      }

      @Override
      public boolean moveNext() {
         ++index;
         return index >= offset && index < offset + numElements;
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }
   }

   @Override
   public int binarySearch(int value) {
      int idx = Arrays.binarySearch(backingArray, offset, offset + numElements,
              value);
      return (idx < 0) ? idx + offset: idx - offset;
   }

   @Override
   public int binarySearch(int value, int from, int size) {
      throw new UnsupportedOperationException();
   }

   @Override
   public String toString() {
      return "view" + super.toString();
   }
}
