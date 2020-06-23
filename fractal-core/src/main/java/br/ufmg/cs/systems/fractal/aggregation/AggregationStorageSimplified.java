package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.aggregation.reductions.ReductionFunction;
import com.koloboke.collect.map.ObjObjCursor;
import com.koloboke.collect.map.hash.HashObjObjMap;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class AggregationStorageSimplified<K extends Writable,
        V extends Writable> extends AggregationStorage<K,V> {

   private static final int MAX_SIZE = 100000;

   private static final org.apache.hadoop.conf.Configuration hadoopConf =
           new org.apache.hadoop.conf.Configuration();

   private String name;
   protected boolean isIncremental;
   protected Class<K> keyClass;
   protected Class<V> valueClass;
   protected ReductionFunction<V> reductionFunction;
   protected EndAggregationFunction<K, V> endAggregationFunction;

   protected K reusableKey;
   protected V reusableValue;

   protected HashObjObjMap<K,V> keyValueMap2;

   protected AggregationIterator<K,V> aggStorageIterator;

   public AggregationStorageSimplified() {
   }

   public AggregationStorageSimplified(String name, AggregationStorageMetadata<K,V> metadata) {
      init(name, metadata);
   }

   protected void init(String name, AggregationStorageMetadata<K,V> metadata) {
      keyValueMap2 = HashObjObjMaps.newMutableMap();

      reset();

      this.name = name;

      if (metadata == null) {
         return;
      }

      keyClass = metadata.getKeyClass();
      valueClass = metadata.getValueClass();
      reductionFunction = metadata.getReductionFunction();
      endAggregationFunction = metadata.getEndAggregationFunction();
      isIncremental = metadata.isIncremental();

      try {
         if (NullWritable.class.isAssignableFrom(keyClass)) {
            reusableKey = (K) NullWritable.get();
         } else if (!br.ufmg.cs.systems.fractal.pattern.Pattern.class.isAssignableFrom(keyClass)) {
            reusableKey = keyClass.newInstance();
         }
         reusableValue = valueClass.newInstance();
      } catch (InstantiationException e) {
         throw new RuntimeException("No-arg constructor not found", e);
      } catch (IllegalAccessException e) {
         throw new RuntimeException("Illegal access while instantiating resuables", e);
      }
   }

   public void reset() {
      if (keyValueMap2 != null) {
         keyValueMap2.clear();
      }
   }

   @Override
   public void setAggStorageIterator(AggregationIterator<K,V> aggStorageIterator) {
      this.aggStorageIterator = aggStorageIterator;
   }

   @Override
   public void finishAggregationStorage() {
      if (keyValueMap2 != null) {
         aggStorageIterator.addMap(keyValueMap2);
         aggStorageIterator.setMapAddingFinished(true);
         keyValueMap2 = null;
         synchronized (aggStorageIterator) {
            aggStorageIterator.notify();
         }
      }
   }

   public String getName() {
      return name;
   }

   public int getNumberMappings() {
      return keyValueMap2.size();
   }

   public Map<K, V> getMapping() {
      return Collections.unmodifiableMap(keyValueMap2);
   }

   public V getValue(K key) {
      return keyValueMap2.get(key);
   }

   public void removeKey(K key) {
      keyValueMap2.remove(key);
   }

   public void removeKeys(Set<K> keys) {
      for (K key : keys) {
         removeKey(key);
      }
   }

   // Not thread-safe
   // Watch out if reusing either Key or Value. Copies ARE NOT MADE!!!
   public void aggregate(K key, V value) {
      throw new UnsupportedOperationException("Not allowed");
   }

   public void aggregateWithReusables(K key, V value) {
      V myValue = keyValueMap2.get(key);

      if (myValue == null) {
         keyValueMap2.put(copyWritable(key), copyWritable(value));
         if (keyValueMap2.size() > MAX_SIZE) {
            // dump
            aggStorageIterator.addMap(keyValueMap2);
            synchronized (aggStorageIterator) {
               aggStorageIterator.notify();
            }
            keyValueMap2 = HashObjObjMaps.newMutableMap();
         }
      } else {
         reductionFunction.reduce(myValue, value);
      }

      reusableKey = key;
      reusableValue = value;
   }

   protected <W extends Writable> W copyWritable(W writable) {
      return WritableUtils.clone(writable, hadoopConf);
   }

   @Override
   public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeUTF(name);

      dataOutput.writeInt(keyValueMap2.size());
      ObjObjCursor<K, V> cur = keyValueMap2.cursor();
      while (cur.moveNext()) {
         cur.key().write(dataOutput);
         cur.value().write(dataOutput);
      }

   }

   @Override
   public void writeExternal(ObjectOutput objOutput) throws IOException {
      throw new UnsupportedOperationException("Not allowed");
   }

   @Override
   public void readExternal(ObjectInput objInput) throws IOException, ClassNotFoundException {

      name = objInput.readUTF();

      keyClass = (Class<K>) objInput.readObject();
      valueClass = (Class<V>) objInput.readObject();
      reductionFunction = (ReductionFunction<V>) objInput.readObject();
      endAggregationFunction = (EndAggregationFunction<K,V>) objInput.readObject();
      isIncremental = objInput.readBoolean();

      keyValueMap2 = HashObjObjMaps.newMutableMap();

      try {

         if (!NullWritable.class.isAssignableFrom(keyClass)) {
            Constructor<K> keyClassConstructor = keyClass.getConstructor();
            Constructor<V> valueClassConstructor = valueClass.getConstructor();

            int numEntries = objInput.readInt();

            for (int i = 0; i < numEntries; ++i) {
               K key = keyClassConstructor.newInstance();

               key.readFields(objInput);

               V value = valueClassConstructor.newInstance();

               value.readFields(objInput);

               keyValueMap2.put(key, value);
            }
         } else {
            Constructor<V> valueClassConstructor = valueClass.getConstructor();

            int numEntries = objInput.readInt();

            K key = (K) NullWritable.get();
            for (int i = 0; i < numEntries; ++i) {
               key.readFields(objInput);

               V value = valueClassConstructor.newInstance();

               value.readFields(objInput);

               keyValueMap2.put(key, value);
            }
         }
      } catch (Exception e) {
         throw new RuntimeException("Error reading aggregation storage", e);
      }
   }

   @Override
   public void readFields(DataInput dataInput) throws IOException {
      throw new UnsupportedOperationException("Not allowed");
   }

   public K reusableKey() {
      return reusableKey;
   }

   public V reusableValue() {
      return reusableValue;
   }

   @Override
   public String toString() {
      return "AggregationStorage{" +
              "name='" + name + '\'' +
              ",keyValueMapSize=" + keyValueMap2.size() +
              //", keyValueMap2=" + keyValueMap2 +
              ",isIncremental=" + isIncremental +
              '}';
   }

   public String toOutputString() {
      return keyValueMap2.toString();
   }

   public boolean containsKey(K key) {
      return keyValueMap2.containsKey(key);
   }

   public boolean isIncremental() {
      return isIncremental;
   }
}
