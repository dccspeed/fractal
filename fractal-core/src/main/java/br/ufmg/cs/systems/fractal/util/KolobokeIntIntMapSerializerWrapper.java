package br.ufmg.cs.systems.fractal.util;

import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.function.IntIntConsumer;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class KolobokeIntIntMapSerializerWrapper implements Externalizable {

   private IntIntMap underlyingMap;
   private IntIntMapWriter intIntMapWriter;

   public KolobokeIntIntMapSerializerWrapper(IntIntMap underlyingMap) {
      this.underlyingMap = underlyingMap;
      this.intIntMapWriter = new IntIntMapWriter();
   }

   public IntIntMap get() {
      return underlyingMap;
   }

   @Override
   public void writeExternal(ObjectOutput objectOutput) throws IOException {
      objectOutput.writeInt(underlyingMap.size());
      intIntMapWriter.setOutput(objectOutput);
      underlyingMap.forEach(intIntMapWriter);
   }

   @Override
   public void readExternal(ObjectInput objectInput)
           throws IOException, ClassNotFoundException {
      int size = objectInput.readInt();
      underlyingMap = HashIntIntMaps.newUpdatableMap(size);
      for (int i = 0; i < size; ++i) {
         int k = objectInput.readInt();
         int v = objectInput.readInt();
         underlyingMap.put(k, v);
      }
   }

   private class IntIntMapWriter implements IntIntConsumer {
      private ObjectOutput output;

      public void setOutput(ObjectOutput output) {
         this.output = output;
      }

      @Override
      public void accept(int k, int v) {
         try {
            output.writeInt(k);
            output.writeInt(v);
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }
   }
}
