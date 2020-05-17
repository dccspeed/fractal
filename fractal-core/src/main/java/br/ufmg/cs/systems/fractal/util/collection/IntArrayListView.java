package br.ufmg.cs.systems.fractal.util.collection;

import br.ufmg.cs.systems.fractal.util.pool.IntArrayListViewPool;

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
   public int getUnchecked(int i) {
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

}
