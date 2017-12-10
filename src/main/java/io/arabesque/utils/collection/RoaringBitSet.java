package io.arabesque.utils.collection;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.IntIterator;
import com.koloboke.function.IntConsumer;
import com.koloboke.function.IntPredicate;

import io.arabesque.utils.pool.RoaringBitSetPool;

import javax.annotation.Nonnull;
import java.util.Collection;

import org.roaringbitmap.RoaringBitmap;

public class RoaringBitSet implements IntCollection {
   protected RoaringBitmap internalBitmap;

   private final RoaringConsumer roaringConsumer = new RoaringConsumer();

   public RoaringBitSet() {
      this(new RoaringBitmap());
   }
   
   public RoaringBitSet(RoaringBitmap internalBitmap) {
      this.internalBitmap = internalBitmap;
   }

   public RoaringBitmap getInternalBitmap() {
      return internalBitmap;
   }

   public void setInternalBitmap(RoaringBitmap internalBitmap) {
      this.internalBitmap = internalBitmap;
   }

   public void transferFrom(RoaringBitSet other) {
      this.internalBitmap = other.internalBitmap;
   }

   public void add(RoaringBitSet other, long rangeStart, long rangeEnd) {
      internalBitmap = RoaringBitmap.add(
            other.internalBitmap, rangeStart, rangeEnd);
   }

   public RoaringBitSet iremove(long rangeStart, long rangeEnd) {
      RoaringBitSet res = RoaringBitSetPool.instance().createObject();
      res.internalBitmap.or(internalBitmap);
      res.internalBitmap.remove(rangeStart, rangeEnd);
      return res;
   }
   
   public void mutableRemove(long rangeStart, long rangeEnd) {
      internalBitmap.remove(rangeStart, rangeEnd);
   }

   public RoaringBitSet immutableUnion(RoaringBitSet other) {
      return new RoaringBitSet(
            RoaringBitmap.or(internalBitmap, other.internalBitmap));
   }
   
   public void mutableUnion(RoaringBitSet other) {
      internalBitmap.or(other.internalBitmap);
   }

   public RoaringBitSet immutableDifference(RoaringBitSet other) {
      return new RoaringBitSet(
            RoaringBitmap.andNot(internalBitmap, other.internalBitmap));
   }
   
   public void mutableDifference(RoaringBitSet other) {
      internalBitmap.andNot(other.internalBitmap);
   }

   public RoaringBitSet clone() {
      return new RoaringBitSet(internalBitmap.clone());
   }

   @Override
   public void clear() {
      internalBitmap.clear();
   }

   public long sizeInBytes() {
      return internalBitmap.getLongSizeInBytes();
   }

   @Override
   public boolean add(@Nonnull Integer integer) {
      return add((int) integer);
   }

   @Override
   public boolean add(int newValue) {
      internalBitmap.add(newValue);
      return true;
   }

   @Override
   public boolean removeInt(int targetValue) {
      internalBitmap.remove(targetValue);
      return true;
   }

   @Override
   public boolean remove(Object o) {
      return removeInt((int) o);
   }

   @Override
   public boolean contains(Object o) {
      return contains((int) o);
   }

   @Override
   public boolean contains(int element) {
      return internalBitmap.contains(element);
   }

   @Nonnull
   @Override
   public IntIterator iterator() {
      throw new UnsupportedOperationException();
   }

   @Nonnull
   @Override
   public IntCursor cursor() {
      return new RoaringIntCursor();
   }

   @Override
   public int size() {
      return internalBitmap.getCardinality();
   }

   @Override
   public long sizeAsLong() {
      return internalBitmap.getCardinality();
   }

   @Override
   public boolean isEmpty() {
      return size() == 0;
   }

   @Override
   public boolean shrink() {
      return internalBitmap.runOptimize();
   }

   @Override
   public boolean ensureCapacity(long l) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean addAll(Collection<? extends Integer> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void forEach(@Nonnull IntConsumer intConsumer) {
      roaringConsumer.setConsumer(intConsumer);
      internalBitmap.forEach(roaringConsumer);
   }

   @Override
   public boolean removeIf(@Nonnull IntPredicate intPredicate) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean forEachWhile(@Nonnull IntPredicate intPredicate) {
      throw new UnsupportedOperationException();
   }

   @Nonnull
   @Override
   public Object[] toArray() {
      throw new UnsupportedOperationException();
   }

   @Nonnull
   @Override
   public <T> T[] toArray(@Nonnull T[] ts) {
      throw new UnsupportedOperationException();
   }

   @Nonnull
   @Override
   public int[] toIntArray() {
      return internalBitmap.toArray();
   }

   @Nonnull
   @Override
   public int[] toArray(@Nonnull int[] ints) {
      throw new UnsupportedOperationException();
   }

   private class RoaringConsumer implements org.roaringbitmap.IntConsumer {
      private IntConsumer kolobokeConsumer;

      public void setConsumer(IntConsumer kolobokeConsumer) {
         this.kolobokeConsumer = kolobokeConsumer;
      }

      public void accept(int value) {
         kolobokeConsumer.accept(value);
      }
   }

   private class RoaringIntCursor implements IntCursor {
      private org.roaringbitmap.IntIterator internalIterator;
      private int currElem;

      public RoaringIntCursor() {
         this.internalIterator = internalBitmap.getReverseIntIterator();
      }

      @Override
      public void forEachForward(@Nonnull IntConsumer intConsumer) {
         throw new UnsupportedOperationException();
      }

      @Override
      public int elem() {
         return currElem;
      }

      @Override
      public boolean moveNext() {
         if (internalIterator.hasNext()) {
            currElem = internalIterator.next();
            return true;
         } else {
            currElem = -1;
            return false;
         }
      }

      @Override
      public void remove() {
         // internalBitmap.remove(currElem);
      }
   }
}
