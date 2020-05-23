package br.ufmg.cs.systems.fractal.util.collection;

import br.ufmg.cs.systems.fractal.util.pool.IntArrayListViewPool;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Arrays;

public final class IntArrayListView extends IntArrayList{
   private int offset;

   public IntArrayListView() {
   }

   public void set(IntArrayList underlying, int from, int to) {
      this.backingArray = underlying.backingArray;
      this.numElements = to - from;
      this.offset = from;
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
