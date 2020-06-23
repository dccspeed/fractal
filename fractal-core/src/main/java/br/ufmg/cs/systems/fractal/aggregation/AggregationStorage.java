package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.aggregation.reductions.ReductionFunction;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class AggregationStorage<K extends Writable, V extends Writable> implements Writable, Externalizable {
    private static final org.apache.hadoop.conf.Configuration hadoopConf =
            new org.apache.hadoop.conf.Configuration();
    private String name;
   protected boolean isIncremental;
    protected AbstractMap<K, V> keyValueMap;
    protected Class<K> keyClass;
    protected Class<V> valueClass;
    protected ReductionFunction<V> reductionFunction;
    protected EndAggregationFunction<K, V> endAggregationFunction;

    protected K reusableKey;
    protected V reusableValue;

    public AggregationStorage() {
    }

    public AggregationStorage(String name, AggregationStorageMetadata<K,V> metadata) {
        init(name, metadata);
    }

    public AggregationStorage(String name, AggregationStorageMetadata<K,V> metadata,
          AbstractMap<K,V> keyValueMap) {
       init(name, metadata);
       this.keyValueMap = keyValueMap;
    }

    protected void init(String name, AggregationStorageMetadata<K,V> metadata) {
        if (keyValueMap == null) {
            //keyValueMap = new HashMap<>();
            keyValueMap = new ConcurrentHashMap<>();
        }

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
           if (org.apache.hadoop.io.NullWritable.class.isAssignableFrom(keyClass)) {
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
        if (keyValueMap != null) {
            keyValueMap.clear();
        }
    }

   public void setAggStorageIterator(AggregationIterator<K, V> aggStorageIterator) {
       throw new RuntimeException("Not allowed");
   }

   public void finishAggregationStorage() {

   }

   public String getName() {
        return name;
    }

    public int getNumberMappings() {
        return keyValueMap.size();
    }

   public Map<K, V> getMapping() {
        return Collections.unmodifiableMap(keyValueMap);
    }

    public V getValue(K key) {
        return keyValueMap.get(key);
    }

    public void removeKey(K key) {
        keyValueMap.remove(key);
    }

    public void removeKeys(Set<K> keys) {
        for (K key : keys) {
            removeKey(key);
        }
    }

    // Not thread-safe
    // Watch out if reusing either Key or Value. Copies ARE NOT MADE!!!
    public void aggregate(K key, V value) {
        V myValue = keyValueMap.get(key);

        if (myValue == null) {
            keyValueMap.put(key, value);
        } else {
            keyValueMap.put(key, reductionFunction.reduce(myValue, value));
        }
    }

    public void aggregateWithReusables(K key, V value) {
        V myValue = keyValueMap.get(key);

        if (myValue == null) {
            keyValueMap.put(copyWritable(key), copyWritable(value));
        } else {
            reductionFunction.reduce(myValue, value);
        }

        reusableKey = key;
        reusableValue = value;
    }

    protected <W extends Writable> W copyWritable(W writable) {
        return WritableUtils.clone(writable, hadoopConf);
    }

    // Thread-safe
    public void finalLocalAggregate(AggregationStorage<K, V> otherStorage) {
        synchronized (this) {
            aggregate(otherStorage);
        }
    }

    // Not thread-safe
    public void aggregate(AggregationStorage<K, V> otherStorage) {
        if (otherStorage == null) {
           return;
        }

        if (!getName().equals(otherStorage.getName())) {
            throw new RuntimeException("Aggregating storages with different names");
        }

        for (Map.Entry<K, V> otherStorageEntry : otherStorage.keyValueMap.entrySet()) {
            K otherKey = otherStorageEntry.getKey();
            V otherValue = otherStorageEntry.getValue();

            aggregate(otherKey, otherValue);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
       throw new UnsupportedOperationException();
    }

    @Override
    public void writeExternal(ObjectOutput objOutput) throws IOException {
        objOutput.writeUTF(name);

        objOutput.writeObject (keyClass);
        objOutput.writeObject (valueClass);
        objOutput.writeObject (reductionFunction);
        objOutput.writeObject (endAggregationFunction);
        objOutput.writeBoolean (isIncremental);

        objOutput.writeInt(keyValueMap.size());
        for (Map.Entry<K, V> entry : keyValueMap.entrySet()) {
            entry.getKey().write(objOutput);
            entry.getValue().write(objOutput);
        }

    }

    @Override
    public void readExternal(ObjectInput objInput) throws IOException, ClassNotFoundException {

        name = objInput.readUTF();

        keyClass = (Class<K>) objInput.readObject();
        valueClass = (Class<V>) objInput.readObject();
        reductionFunction = (ReductionFunction<V>) objInput.readObject();
        endAggregationFunction = (EndAggregationFunction<K,V>) objInput.readObject();
        isIncremental = objInput.readBoolean();

        if (keyValueMap == null) {
            //keyValueMap = new HashMap<>();
            keyValueMap = new ConcurrentHashMap<>();
        }

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

                 keyValueMap.put(key, value);
              }
           } else {
              Constructor<V> valueClassConstructor = valueClass.getConstructor();

              int numEntries = objInput.readInt();

              K key = (K) NullWritable.get();
              for (int i = 0; i < numEntries; ++i) {
                 key.readFields(objInput);

                 V value = valueClassConstructor.newInstance();

                 value.readFields(objInput);

                 keyValueMap.put(key, value);
              }

           }
        } catch (Exception e) {
            throw new RuntimeException("Error reading aggregation storage", e);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
       throw new UnsupportedOperationException();
    }

    public void endedAggregation() {
        if (endAggregationFunction != null) {
            endAggregationFunction.endAggregation(this);
        }
    }

    private ArrayBlockingQueue<K> keysConsumer;

    public synchronized ArrayBlockingQueue<K> getKeysConsumer() {
       if (keysConsumer == null) {
          keysConsumer = new ArrayBlockingQueue<K>(
                keyValueMap.size() > 0 ? keyValueMap.size() : 1,
                false,
                keyValueMap.keySet()
                );
       }

       return keysConsumer;
    }

    public void transferKeyFrom(K key, AggregationStorage<K, V> otherAggregationStorage) {
        aggregate(key, otherAggregationStorage.getValue(key));
        //otherAggregationStorage.removeKey(key);
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
                ",keyValueMapSize=" + keyValueMap.size() +
                 //", keyValueMap=" + keyValueMap +
                ",isIncremental=" + isIncremental +
                '}';
    }

    public String toOutputString() {
        StringBuilder strBuilder = new StringBuilder();

        ArrayList<K> keys = new ArrayList<>(keyValueMap.keySet());

        if (WritableComparable.class.isAssignableFrom(keyClass)) {
            ArrayList<? extends WritableComparable> orderedKeys = (ArrayList<? extends WritableComparable>) keys;
            Collections.sort(orderedKeys);
        }

        for (K key : keys) {
            strBuilder.append(key.toString());
            strBuilder.append(": ");
            strBuilder.append(keyValueMap.get(key));
            strBuilder.append('\n');
        }

        return strBuilder.toString();
    }

    public boolean containsKey(K key) {
        return keyValueMap.containsKey(key);
    }

    public boolean isIncremental() {
       return isIncremental;
    }
}
