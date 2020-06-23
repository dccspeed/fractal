package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.ReflectionUtils;
import com.koloboke.collect.map.ObjObjMap;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import org.apache.log4j.Logger;

import java.io.Serializable;

public abstract class ObjObjSubgraphAggregation
        <S extends Subgraph, K extends Serializable, V extends Serializable>
        implements SubgraphAggregation<S> {
   private static final Logger LOG = Logger.getLogger(ObjObjSubgraphAggregation.class);

   private static final int MAX_SIZE = 10000;

   private ObjObjMap<K,V> keyValueMap;

   public ObjObjSubgraphAggregation() {
      keyValueMap = HashObjObjMaps.newUpdatableMap();
   }

   @Override
   public final void aggregate(S subgraph) {
      final K key = key(subgraph);
      final V value = value(subgraph);
      final V existingValue = keyValueMap.get(key);

      if (existingValue != null) {
         aggregate(existingValue, value);
      } else {
         keyValueMap.put(ReflectionUtils.clone(key),
                 ReflectionUtils.clone(value));
         if (keyValueMap.size() > MAX_SIZE) {
            // wait until map is consumed
            notifyNextKeyValueMapAvailable();
            waitForNextKeyValueMapConsumption();
         }
      }
   }

   public void waitForNextKeyValueMap() {
      synchronized (keyValueMap) {
         try {
            keyValueMap.wait();
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
      }
   }

   public synchronized void waitForNextKeyValueMapConsumption() {
      try {
         this.wait();
      } catch (InterruptedException e) {
         e.printStackTrace();
      }
      keyValueMap.clear();
   }

   public synchronized void notifyNextKeyValueMapConsumed() {
      this.notify();
   }

   public void notifyNextKeyValueMapAvailable() {
      synchronized (keyValueMap) {
         keyValueMap.notify();
      }
   }

   public ObjObjMap<K,V> consumeKeyValueMap() {
      return keyValueMap;
   }

   public abstract K key(S subgraph);
   public abstract V value(S subgraph);
   public abstract void aggregate(V existingValue, V otherValue);
}
