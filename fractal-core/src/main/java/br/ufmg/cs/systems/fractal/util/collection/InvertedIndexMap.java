package br.ufmg.cs.systems.fractal.util.collection;

import br.ufmg.cs.systems.fractal.util.pool.IntIntMapPool;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.function.IntConsumer;

public class InvertedIndexMap implements Externalizable, Writable {

   protected IntIntMap docToFreq;
   protected long totalFreq;

   public InvertedIndexMap() {
      this.docToFreq = IntIntMapPool.instance().createObject();
      this.totalFreq = 0;
   }
   
   public InvertedIndexMap(int doc, int freq) {
      this();
      appendDoc(doc, freq);
   }

   public void appendDoc(int doc, int freq) {
      docToFreq.addValue(doc, freq, 0);
      totalFreq += freq;
      if (freq < 0) {
         throw new RuntimeException("Frequency below zero");
      }
   }

   public boolean containsDoc(int doc) {
      return docToFreq.containsKey(doc);
   }
   
   public IntCursor docCursor() {
      return docToFreq.keySet().cursor();
   }

   public IntIntCursor cursor() {
      return docToFreq.cursor();
   }

   public int getFreq(int doc) {
      return docToFreq.getOrDefault(doc, 0);
   }

   public long getTotalFreq() {
      return totalFreq;
   }

   public int size() {
      return docToFreq.size();
   }

   public void forEachDoc(IntConsumer consumer) {
      docToFreq.keySet().forEach(consumer);
   }
   
   public void clear() {
      docToFreq.clear();
   }

   public void merge(InvertedIndexMap ii2) {
      IntIntMap docToFreq1 = docToFreq;
      IntIntMap docToFreq2 = ii2.docToFreq;

      IntIntCursor cur = docToFreq2.cursor();

      while (cur.moveNext()) {
         docToFreq1.addValue(cur.key(), cur.value(), 0);
      }

      totalFreq = totalFreq + ii2.totalFreq;
   }

   public void reclaim() {
      IntIntMapPool.instance().reclaimObject(docToFreq);
   }

   /** Serialization { */
   @Override
   public void writeExternal(ObjectOutput objOutput) throws IOException {
      objOutput.writeLong(totalFreq);
      objOutput.writeInt(docToFreq.size());
      IntIntCursor cur = docToFreq.cursor();
      while (cur.moveNext()) {
         objOutput.writeInt(cur.key());
         objOutput.writeInt(cur.value());
      }
   }
    
   @Override
   public void readExternal(ObjectInput objInput)
         throws IOException, ClassNotFoundException {

      totalFreq = objInput.readLong();
      int size = objInput.readInt();
      if (size > 0) {
         if (docToFreq != null) {
            reclaim();
         }
         int[] keys = new int[size];
         int[] values = new int[size];
         for (int i = 0; i < size; ++i) {
            keys[i] = objInput.readInt();
            values[i] = objInput.readInt();
         }

         docToFreq = HashIntIntMaps.newMutableMap(keys, values, size);
      }
   }
    
   @Override
   public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeLong(totalFreq);
      dataOutput.writeInt(docToFreq.size());
      IntIntCursor cur = docToFreq.cursor();
      while (cur.moveNext()) {
         dataOutput.writeInt(cur.key());
         dataOutput.writeInt(cur.value());
      }
   }

   @Override
   public void readFields(DataInput dataInput) throws IOException {
      totalFreq = dataInput.readLong();
      int size = dataInput.readInt();
      if (size > 0) {
         if (docToFreq != null) {
            reclaim();
         }

         int[] keys = new int[size];
         int[] values = new int[size];
         for (int i = 0; i < size; ++i) {
            keys[i] = dataInput.readInt();
            values[i] = dataInput.readInt();
         }
         
         docToFreq = HashIntIntMaps.newMutableMap(keys, values, size);
      }
   }
   
   /** Serialization } */

   @Override
   public String toString() {
      return "InvertedIndexMap(size=" + docToFreq.size() +
         ",totalFreq=" + totalFreq +
         ")";
      // return "InvertedIndex(docs=" + docs + ",freqs=" + freqs + ")";
   }
}
