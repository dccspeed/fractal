package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.computation.ExecutionEngine;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.ProducerConsumerSignaling;
import br.ufmg.cs.systems.fractal.util.ReflectionSerializationUtils;
import com.koloboke.collect.hash.HashConfig;
import com.koloboke.collect.map.ObjLongMap;
import com.koloboke.collect.map.hash.HashObjLongMap;
import com.koloboke.collect.map.hash.HashObjLongMaps;
import com.koloboke.function.ObjLongToLongFunction;
import org.apache.log4j.Logger;

import java.io.Serializable;

public abstract class ObjLongSubgraphAggregation
        <S extends Subgraph, K extends Serializable>
        extends ProducerConsumerSignaling
        implements SubgraphAggregation<S>, ObjLongToLongFunction<K> {
   private static final Logger LOG = Logger.getLogger(ObjLongSubgraphAggregation.class);

   private static final int MAX_SIZE = 10000;

   private HashObjLongMap<K> keyValueMap;
   private long newValue;
   private long lastValue;

   public final void init(Configuration configuration) {
      keyValueMap = HashObjLongMaps.getDefaultFactory()
              .withDefaultValue(defaultValue())
              .withHashConfig(HashConfig.fromLoads(0.5, 0.5, 1))
              .newMutableMap(MAX_SIZE);
   }

   public long defaultValue() { return 0; }

   public final void map(K key, long value) {
      newValue = value;
      keyValueMap.compute(key, this);
      if (lastValue == keyValueMap.defaultValue()) {
         keyValueMap.put(ReflectionSerializationUtils.clone(key),
                 keyValueMap.removeAsLong(key));

         if (keyValueMap.size() > MAX_SIZE) {
            // wait until map is consumed
            notifyWorkProduced();
            waitWorkConsumed();
            keyValueMap.clear();
         }
      }
   }

   public abstract long reduce(long v1, long v2);

   public final ObjLongMap<K> getKeyValueMap() {
      return keyValueMap;
   }

   @Override
   public final long applyAsLong(K k, long existing) {
      lastValue = existing;
      return reduce(newValue, existing);
   }

   @Override
   public void report(ExecutionEngine<S> engine) {
      // empty by default
   }

}
