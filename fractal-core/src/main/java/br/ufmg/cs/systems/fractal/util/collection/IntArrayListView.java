package br.ufmg.cs.systems.fractal.util.collection;

import br.ufmg.cs.systems.fractal.util.pool.IntArrayListViewPool;
import com.koloboke.collect.IntCursor;
import org.mortbay.log.Log;

import javax.annotation.Nonnull;
import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.function.IntConsumer;

public final class IntArrayListView extends IntArrayList{
   protected int offset;

   public IntArrayListView() {
   }

   @Override
   protected void init(int capacity) {
      backingArray = null;
      numElements = 0;
      offset = 0;
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
   public int getLast() {
      return getu(size() - 1);
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
   public void arrayCopy(IntArrayList src, int srcPos, int destPos, int length) {
      throw new UnsupportedOperationException();
   }

   @Override
   public int getIdx(int idx) {
      return idx + offset;
   }

   @Override
   public boolean contains(int elem) {
      for (int i = offset; i < offset + numElements; ++i) {
         if (backingArray[i] == elem) return true;
      }

      return false;
   }

   @Override
   public IntArrayListView view(int from, int to) {
      IntArrayListView view = IntArrayListViewPool.instance().createObject();
      view(view, from, to);
      return view;
   }

   @Override
   public void view(IntArrayListView view, int from, int to) {
      view.backingArray = backingArray;
      view.numElements = to - from;
      view.offset = offset + from;
   }

   @Override
   public String toString() {
      return String.format("view(offset=%d)", offset) + super.toString();
   }
}
