package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.pattern.Pattern;
import com.koloboke.collect.map.hash.HashObjByteMap;
import com.koloboke.collect.map.hash.HashObjByteMaps;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class PatternAggregationStorage<K extends Pattern, V extends Writable> extends AggregationStorage<K, V> {
    private static final Logger LOG = Logger.getLogger(PatternAggregationStorage.class);

    private final HashObjByteMap<K> reservations;
    private final ConcurrentHashMap<K, AtomicBoolean> reservations2;
    private final ConcurrentHashMap<K, K> quick2CanonicalMap;
    private final ConcurrentHashMap<K, ConcurrentHashMap<K,Boolean>> canonical2quickMap; // Fake map, should be set,

    public PatternAggregationStorage() {
        this(null, null);
    }

    public PatternAggregationStorage(String name, AggregationStorageMetadata<K,V> metadata) {
        super(name, metadata, new ConcurrentHashMap<K,V>());
        reservations = HashObjByteMaps.getDefaultFactory().withDefaultValue((byte) 0).newMutableMap();
        reservations2 = new ConcurrentHashMap<>();
        quick2CanonicalMap = new ConcurrentHashMap<>();
        canonical2quickMap = new ConcurrentHashMap<>();
    }

    @Override
    public void reset() {
        super.reset();

        if (reservations != null) {
            reservations.clear();
        }
        
        if (reservations2 != null) {
            reservations2.clear();
        }

        if (quick2CanonicalMap != null) {
            quick2CanonicalMap.clear();
            canonical2quickMap.clear();
        }
    }

    @Override
    public K getKey(K key) {
        K superKey = super.getKey(key);

        if (superKey == null) {
            superKey = quick2CanonicalMap.get(key);
        }

        return superKey;
    }

    @Override
    public V getValue(K key) {
        V value = super.getValue(key);

        // If we didn't find a value, key might be a non-canonical pattern. If we have
        // quick2CanonicalMappings, we can attempt to translate the request to the canonical
        // pattern
        if (value == null && !quick2CanonicalMap.isEmpty()) {
            K canonical = quick2CanonicalMap.get(key);

            if (canonical != null) {
                value = super.getValue(canonical);
            }
        }

        return value;
    }

    @Override
    public void removeKey(K key) {
        super.removeKey(key);

        // If quick2Canonical is not empty then we need to clean it up.
        // Key may represent a quick or canonical pattern.
        // We need to clean all keys and values matching it.
        if (!quick2CanonicalMap.isEmpty()) {
            Iterator<Map.Entry<K, K>> quick2CanonicalIterator = quick2CanonicalMap.entrySet().iterator();

            while (quick2CanonicalIterator.hasNext()) {
                Map.Entry<K, K> entry = quick2CanonicalIterator.next();

                if (entry.getKey().equals(key) || entry.getValue().equals(key)) {
                    quick2CanonicalIterator.remove();
                }
            }
        }
    }

    @Override
    public void removeKeys(Set<K> keys) {
        for (K key : keys) {
            super.removeKey(key);
        }

        int numQuicksRemoved = 0;

        // If quick2Canonical is not empty then we need to clean it up.
        // Key may represent a quick or canonical pattern.
        // We need to clean all keys and values matching it.
        if (!quick2CanonicalMap.isEmpty()) {
            Iterator<Map.Entry<K, K>> quick2CanonicalIterator = quick2CanonicalMap.entrySet().iterator();

            while (quick2CanonicalIterator.hasNext()) {
                Map.Entry<K, K> entry = quick2CanonicalIterator.next();

                K quickPattern = entry.getKey();
                K canonicalPattern = entry.getValue();

                if (keys.contains(quickPattern) || keys.contains(canonicalPattern)) {
                    quick2CanonicalIterator.remove();
                    numQuicksRemoved += 1;
                }
            }
        }

        LOG.info("NumCanonicalPatternsRemoved=" + keys.size() +
              " NumQuickPatternsRemoved=" + numQuicksRemoved);
    }

    @Override
    // Threadsafe
    public void finalLocalAggregate(AggregationStorage<K, V> otherStorage) {
        while (!otherStorage.keyValueMap.isEmpty()) {
            Iterator<Map.Entry<K, V>> entryIterator = otherStorage.keyValueMap.entrySet().iterator();
            
            while (entryIterator.hasNext()) {
                Map.Entry<K, V> entry = entryIterator.next();

                if (canonicalAggregate(entry.getKey(), entry.getValue())) {
                    entryIterator.remove();
                }
            }
        }
    }

    @Override
    public void aggregateWithReusables(K key, V value) {
        V myValue = keyValueMap.get(key);

        if (myValue == null) {
            keyValueMap.put((K) key.copy(), copyWritable(value));
        } else {
            reductionFunction.reduce(myValue, value);
        }

        reusableKey = key;
        reusableValue = value;
    }

    @Override
    public void aggregate(AggregationStorage<K, V> otherStorage) {
        if (!(otherStorage instanceof PatternAggregationStorage)) {
            throw new RuntimeException("This should never happen");
        }

        super.aggregate(otherStorage);

        PatternAggregationStorage<K, V> otherPatternStorage = (PatternAggregationStorage<K, V>) otherStorage;

        for (Map.Entry<K, K> otherQuick2CanonicalMapEntry : otherPatternStorage.quick2CanonicalMap.entrySet()) {
            K quickPattern = otherQuick2CanonicalMapEntry.getKey();
            K canonicalPattern = otherQuick2CanonicalMapEntry.getValue();

            quick2CanonicalMap.putIfAbsent(quickPattern, canonicalPattern);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);

        dataOutput.writeInt(quick2CanonicalMap.size());

        for (Map.Entry<K, K> quick2CanonicalEntry : quick2CanonicalMap.entrySet()) {
            quick2CanonicalEntry.getKey().write(dataOutput);
            quick2CanonicalEntry.getValue().write(dataOutput);
        }
    }

    @Override
    public void writeExternal(ObjectOutput objOutput) throws IOException {
        super.writeExternal (objOutput);

        objOutput.writeInt(quick2CanonicalMap.size());

        for (Map.Entry<K, K> quick2CanonicalEntry : quick2CanonicalMap.entrySet()) {
            quick2CanonicalEntry.getKey().write(objOutput);
            quick2CanonicalEntry.getValue().write(objOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);

        try {
            Constructor<K> keyClassConstructor = keyClass.getConstructor();

            int sizeQuick2CanonicalMap = dataInput.readInt();

            for (int i = 0; i < sizeQuick2CanonicalMap; ++i) {
                K quick = keyClassConstructor.newInstance();
                quick.readFields(dataInput);

                K canonical = keyClassConstructor.newInstance();
                canonical.readFields(dataInput);

                quick2CanonicalMap.put(quick, canonical);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading quick2canonical mapping", e);
        }
    }

    @Override
    public void readExternal(ObjectInput objInput) throws IOException, ClassNotFoundException {
        super.readExternal (objInput);
        try {
            Constructor<K> keyClassConstructor = keyClass.getConstructor();

            int sizeQuick2CanonicalMap = objInput.readInt();

            for (int i = 0; i < sizeQuick2CanonicalMap; ++i) {
                K quick = keyClassConstructor.newInstance();
                quick.readFields(objInput);

                K canonical = keyClassConstructor.newInstance();
                canonical.readFields(objInput);

                quick2CanonicalMap.put(quick, canonical);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading quick2canonical mapping", e);
        }
    }

    // Thread-safe
    private boolean canonicalAggregate(K quickPattern, V value) {
        K canonicalPattern = getCanonicalPattern(quickPattern);

        if (canonicalPattern == null) {
            return false;
        }

        if (value instanceof PatternAggregationAwareValue) {
            PatternAggregationAwareValue patternAggregationAwareValue = (PatternAggregationAwareValue) value;

            patternAggregationAwareValue.handleConversionFromQuickToCanonical(quickPattern, canonicalPattern);
        }

        V currValue = keyValueMap.get(canonicalPattern);

        // in case it does not exist in the map, we will have to lock the whole
        // map to prevent bad concurrency
        if (currValue == null) {
           synchronized (keyValueMap) {
              currValue = keyValueMap.get(canonicalPattern);
              if (currValue == null) {
                 aggregate(canonicalPattern, value);
                 return true;
              }
           }
        }

        // in case the key already exists we can change the key in-place, so it
        // is safe to lock only the key itself, even if the map implementation
        // does not guarantee thread safety
        synchronized (currValue) {
           aggregate(canonicalPattern, value);
        }

        return true;
    }

    // Thread-safe
    private K getCanonicalPattern(K quickPattern) {
        K canonicalPattern = quick2CanonicalMap.get(quickPattern);

        if (canonicalPattern == null) {
            AtomicBoolean currentReservation = reservations2.get(quickPattern);

            if (currentReservation == null) {
               synchronized (reservations2) {
                  currentReservation = reservations2.get(quickPattern);
                  if (currentReservation == null) {
                     currentReservation = new AtomicBoolean(false);
                     reservations2.put(quickPattern, currentReservation);
                  }
               }
            }

            if (currentReservation.compareAndSet(false, true)) {
               canonicalPattern = (K) quickPattern.copy();
               canonicalPattern.turnCanonical();
               quick2CanonicalMap.put(quickPattern, canonicalPattern);
               add_to_canonical2Quick(canonicalPattern, quickPattern);
            } else {
               return null;
            }
        }

        return canonicalPattern;
    }
    
    private void add_to_canonical2Quick(K canonicalPattern, K quickPattern) {
       // Next, populate the inverse...
       ConcurrentHashMap<K, Boolean> canonicalHM = canonical2quickMap.get(canonicalPattern);
       if (canonicalHM==null){
          canonical2quickMap.putIfAbsent(canonicalPattern, new ConcurrentHashMap<K, Boolean>());
          canonicalHM = canonical2quickMap.get(canonicalPattern);
       }
       canonicalHM.put(quickPattern,Boolean.TRUE);
    }

    //@Override
    //public void transferKeyFrom(K key, AggregationStorage<K, V> otherAggregationStorage) {
    //    if (otherAggregationStorage instanceof PatternAggregationStorage) {
    //        PatternAggregationStorage<K, V> otherPatternAggStorage = (PatternAggregationStorage<K, V>) otherAggregationStorage;

    //        for (Map.Entry<K, K> quick2CanonicalEntry : otherPatternAggStorage.quick2CanonicalMap.entrySet()) {
    //            K quickPattern = quick2CanonicalEntry.getKey();
    //            K canonicalPattern = quick2CanonicalEntry.getValue();

    //            if (canonicalPattern.equals(key)) {
    //                quick2CanonicalMap.put(quickPattern, canonicalPattern);
    //            }
    //        }
    //    }

    //    super.transferKeyFrom(key, otherAggregationStorage);
    //}

    @Override
    public void transferKeyFrom(K key, AggregationStorage<K, V> otherAggregationStorage) {
        if (otherAggregationStorage instanceof PatternAggregationStorage) {
            PatternAggregationStorage<K, V> otherPatternAggStorage = (PatternAggregationStorage<K, V>) otherAggregationStorage;
            
            ConcurrentHashMap<K, Boolean> quickPatterns = otherPatternAggStorage.canonical2quickMap.get(key);

            if (quickPatterns == null) {
                // throw new RuntimeException("Empty quicks?");
                return;
            }

            // canonical2quickMap.put(key, new ConcurrentHashMap<K,Boolean>(quickPatterns.size()));

            for (K entry: quickPatterns.keySet()) {
                quick2CanonicalMap.put(entry, key);
                // canonical2quickMap.get(key).put(entry,Boolean.TRUE);
                otherPatternAggStorage.quick2CanonicalMap.remove(entry);
            }

            otherPatternAggStorage.canonical2quickMap.remove(key);

        }

        super.transferKeyFrom(key, otherAggregationStorage);
    }

    @Override
    public String toString() {
        return "PatternAggregationStorage{" +
                "quick2CanonicalMapSize=" + quick2CanonicalMap.size() +
                 //",quick2CanonicalMap=" + quick2CanonicalMap +
                "} " + super.toString();
    }

    @Override
    public boolean containsKey(K key) {
        // Try finding in normal mapping
        boolean result = super.containsKey(key);

        if (result) {
            return true;
        }

        // If we didn't find in normal mapping, key might be quick and
        // normal mapping might only have canonicals. Lets do the translation
        if (!quick2CanonicalMap.isEmpty()) {
            K canonical = quick2CanonicalMap.get(key);

            if (canonical != null) {
                result = super.containsKey(canonical);

                return result;
            }
        }

        return false;
    }
}
