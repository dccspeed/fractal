package br.ufmg.cs.systems.fractal.util.collection;

import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.IntBinaryOperator;

public class AtomicBitSetArray implements Externalizable, Writable {
   protected AtomicIntegerArray internalArray;

   private static final IntBinaryOperator insert = new IntBinaryOperator() {
      @Override
      public int applyAsInt(int word, int position) {
         return (0x00000001 << position) | word;
      }
   };
   
   private static final IntBinaryOperator remove = new IntBinaryOperator() {
      @Override
      public int applyAsInt(int word, int position) {
         return (0x11111110 << position) & word;
      }
   };

   private static final IntBinaryOperator union = new IntBinaryOperator() {
      @Override
      public int applyAsInt(int word, int otherWord) {
         return word | otherWord;
      }
   };

   public AtomicBitSetArray() {}

   public AtomicBitSetArray(int size) {
      int numWords = (size + 32 - 1) / 32;
      this.internalArray = new AtomicIntegerArray(numWords); 
   }

   public void enableAll() {
      for (int i = 0; i < internalArray.length(); ++i) {
         internalArray.accumulateAndGet(i, 0xFFFFFFFF, union);
      }
   }

   public void insert(int position) {
      int wordIdx = position / 32;
      int offset = position % 32;
      int word = internalArray.get(wordIdx);
      if (((0x00000001 << offset) & word) == 0x00000000) {
         internalArray.accumulateAndGet(wordIdx, offset, insert);
      }
   }
   
   public void remove(int position) {
      int wordIdx = position / 32;
      int offset = position % 32;
      int word = internalArray.get(wordIdx);
      if (((0x00000001 << offset) & word) != 0x00000000) {
         internalArray.accumulateAndGet(wordIdx, offset, remove);
      }
   }

   public boolean contains(int position) {
      return ((0x00000001 << (position % 32)) &
            internalArray.get(position / 32)) != 0x00000000;
   }

   public AtomicBitSetArray union(AtomicBitSetArray other) {
      int minSize;

      if (other.internalArray.length() < internalArray.length()) {
         minSize = other.internalArray.length();
      } else {
         minSize = internalArray.length();
      }

      for (int i = 0; i < minSize; ++i) {
         internalArray.accumulateAndGet(i, other.internalArray.get(i), union);
      }

      return this;
   }


   /** Serialization { */
   @Override
   public void writeExternal(ObjectOutput objOutput) throws IOException {
      write(objOutput);
   }
    
   @Override
   public void readExternal(ObjectInput objInput)
         throws IOException, ClassNotFoundException {
      readFields(objInput);
   }

    
   @Override
   public void write(DataOutput dataOutput) throws IOException {
      int length = internalArray != null ? internalArray.length() : 0;
      dataOutput.writeInt(length);
      for (int i = 0; i < length; ++i) {
         dataOutput.writeInt(internalArray.get(i));
      }
   }

   @Override
   public void readFields(DataInput dataInput) throws IOException {
      int length = dataInput.readInt();
      if (length > 0) {
         internalArray = new AtomicIntegerArray(length); 
         for (int i = 0; i < length; ++i) {
            internalArray.set(i, dataInput.readInt());
         }
      }
   }
   
   /** Serialization } */

   @Override
   public String toString() {
      int numEnabledBits = 0;
      for (int i = 0; i < internalArray.length() * 32; ++i) {
         if (contains(i)) {
            ++numEnabledBits;
         }
      }
      return "AtomicBitSetArray(numWords=" + internalArray.length() +
         ", numEnabledBits=" + numEnabledBits +
         ", numDisabledBits=" + (internalArray.length() * 32 - numEnabledBits) +
         ")";
   }

   public String toDebugString() {
      StringBuilder builder = new StringBuilder();
      builder.append(toString());
      builder.append("[");
      int length = internalArray != null ? internalArray.length() : 0;
      for (int i = 0; i < length * 32; ++i) {
         if (contains(i)) {
            builder.append("1");
         } else {
            builder.append("0");
         }
      }
      builder.append("]");
      return builder.toString();
   }

}
