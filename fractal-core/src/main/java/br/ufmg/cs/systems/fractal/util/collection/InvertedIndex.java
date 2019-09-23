package br.ufmg.cs.systems.fractal.util.collection;

import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import com.koloboke.collect.IntCursor;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.function.IntConsumer;

public class InvertedIndex implements Externalizable, Writable {

   protected IntArrayList docs;
   protected IntArrayList freqs;
   protected long totalFreq;

   public InvertedIndex() {
      this.docs = IntArrayListPool.instance().createObject();
      this.freqs = IntArrayListPool.instance().createObject();
      this.totalFreq = 0;
   }
   
   public InvertedIndex(int doc, int freq) {
      this();
      appendDoc(doc, freq);
   }

   public void appendDoc(int doc, int freq) {
      docs.add(doc);
      freqs.add(freq);
      totalFreq += freq;
      if (freq < 0) {
         throw new RuntimeException("Frequency below zero");
      }
   }

   public boolean containsDoc(int doc) {
      return docs.binarySearch(doc) >= 0;
   }

   public int getDoc(int pos) {
      return docs.get(pos);
   }

   public int getFreq(int doc) {
      int pos = docs.binarySearch(doc);
      if (pos >= 0) {
         return freqs.get(pos);
      } else {
         return 0;
      }
   }

   public long getTotalFreq() {
      return totalFreq;
   }

   public int size() {
      return docs.size();
   }

   public void forEachDoc(IntConsumer consumer) {
      docs.forEach(consumer);
   }
   
   public IntCursor docCursor() {
      return docs.cursor();
   }

   public void clear() {
      docs.clear();
      freqs.clear();
   }

   public void merge(InvertedIndex ii2) {
      IntArrayList docs1 = docs;
      IntArrayList freqs1 = freqs;
      IntArrayList docs2 = ii2.docs;
      IntArrayList freqs2 = ii2.freqs;

      int size1 = size();
      int size2 = ii2.size();
      
      IntArrayList rdocs = IntArrayListPool.instance().createObject();
      IntArrayList rfreqs = IntArrayListPool.instance().createObject();

      int i1 = 0, i2 = 0;
      while (i1 < size1 && i2 < size2) {
         int d1 = docs1.get(i1);
         int d2 = docs2.get(i2);
         if (d1 < d2) { // consume from first
            rdocs.add(d1);
            rfreqs.add(freqs1.get(i1));
            ++i1;
         } else if (d1 > d2) { // consume from second
            rdocs.add(d2);
            rfreqs.add(freqs2.get(i2));
            ++i2;
         } else { // consume from both
            rdocs.add(d1);
            rfreqs.add(freqs1.get(i1) + freqs2.get(i2));
            ++i1;
            ++i2;
         }
      }

      if (i1 < size1) {
         rdocs.transferFrom(docs1, i1, rdocs.size(), (size1 - i1));
         rfreqs.transferFrom(freqs1, i1, rfreqs.size(), (size1 - i1));
      } else if (i2 < size2) {
         rdocs.transferFrom(docs2, i2, rdocs.size(), (size2 - i2));
         rfreqs.transferFrom(freqs2, i2, rfreqs.size(), (size2 - i2));
      }

      reclaim();

      docs = rdocs;
      freqs = rfreqs;
      totalFreq = totalFreq + ii2.totalFreq;
   }

   public void reclaim() {
      IntArrayListPool.instance().reclaimObject(docs);
      IntArrayListPool.instance().reclaimObject(freqs);
   }

   /** Serialization { */
   @Override
   public void writeExternal(ObjectOutput objOutput) throws IOException {
      objOutput.writeLong(totalFreq);
      docs.writeExternal(objOutput);
      freqs.writeExternal(objOutput);
   }
    
   @Override
   public void readExternal(ObjectInput objInput)
         throws IOException, ClassNotFoundException {
      totalFreq = objInput.readLong();
      docs.readExternal(objInput);
      freqs.readExternal(objInput);
   }
    
   @Override
   public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeLong(totalFreq);
      docs.write(dataOutput);
      freqs.write(dataOutput);
   }

   @Override
   public void readFields(DataInput dataInput) throws IOException {
      totalFreq = dataInput.readLong();
      docs.readFields(dataInput);
      freqs.readFields(dataInput);
   }
   
   /** Serialization } */

   @Override
   public String toString() {
      return "InvertedIndex(size=" + docs.size() +
         ",totalFreq=" + totalFreq +
         ")";
      // return "InvertedIndex(docs=" + docs + ",freqs=" + freqs + ")";
   }
}
