package br.ufmg.cs.systems.fractal.util.collection;

import com.koloboke.collect.IntCursor;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import org.apache.hadoop.io.Writable;

import java.io.*;

public class IntSet implements Externalizable, Writable {

   protected HashIntSet internalSet;

   public IntSet() {
      this.internalSet = HashIntSets.newMutableSet();
   }
   
   public IntSet(int expectedSize) {
      this.internalSet = HashIntSets.newMutableSet(expectedSize);
   }

   public int size() {
      return internalSet.size();
   }

   public boolean contains(int elem) {
      return internalSet.contains(elem);
   }

   public boolean addInt(int elem) {
      return internalSet.add(elem);
   }
   
   public boolean add(int elem) {
      return internalSet.add(elem);
   }

   public boolean removeInt(int elem) {
      return internalSet.removeInt(elem);
   }
   
   public HashIntSet getInternalSet() {
      return internalSet;
   }

   public IntSet union(IntSet otherSet) {
      internalSet.addAll(otherSet.internalSet);
      return this;
   }

   public void clear() {
      internalSet.clear();
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
      int size = internalSet.size();
      dataOutput.writeInt(size);
      if (size > 0) {
         IntCursor cur = internalSet.cursor();
         while (cur.moveNext()) {
            dataOutput.writeInt(cur.elem());
         }
      }
   }

   @Override
   public void readFields(DataInput dataInput) throws IOException {
      int size = dataInput.readInt();
      if (size > 0) {
         internalSet.ensureCapacity(size);
         for (int i = 0; i < size; ++i) {
            internalSet.add(dataInput.readInt());
         }
      }
   }
   
   /** Serialization } */

   @Override
   public String toString() {
      return "IntSet(size=" + internalSet.size() + ")";
   }
}
