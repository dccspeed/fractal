package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.ReflectionUtils;
import com.koloboke.collect.map.ObjLongMap;
import com.koloboke.collect.map.hash.HashObjLongMap;
import com.koloboke.collect.map.hash.HashObjLongMaps;
import com.koloboke.function.ObjLongToLongFunction;
import org.apache.log4j.Logger;

import java.io.*;

public abstract class ObjLongSubgraphAggregation
        <S extends Subgraph, K extends Serializable>
        implements SubgraphAggregation<S>, ObjLongToLongFunction<K> {
   private static final Logger LOG = Logger.getLogger(ObjLongSubgraphAggregation.class);

   private static final int MAX_SIZE = 10000;

   private HashObjLongMap<K> keyValueMap;
   private K key;
   private long value;
   private long lastValue;

   public ObjLongSubgraphAggregation(long defaultValue) {
      keyValueMap = HashObjLongMaps.getDefaultFactory()
              .withDefaultValue(defaultValue)
              .newMutableMap();
   }

   @Override
   public final void aggregate(S subgraph) {
      key = key(subgraph);
      value = value(subgraph);

      keyValueMap.compute(key, this);
      if (lastValue == keyValueMap.defaultValue()) {
         keyValueMap.put(ReflectionUtils.clone(key),
                 keyValueMap.removeAsLong(key));

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

   public ObjLongMap<K> consumeKeyValueMap() {
      return keyValueMap;
   }

   public abstract K key(S subgraph);
   public abstract long value(S subgraph);
   public abstract long reduce(long v1, long v2);

   @Override
   public long applyAsLong(K k, long existing) {
      lastValue = existing;
      return reduce(value, existing);
   }
}
