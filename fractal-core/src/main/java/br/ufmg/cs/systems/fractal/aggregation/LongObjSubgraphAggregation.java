package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.ProducerConsumerSignaling;
import br.ufmg.cs.systems.fractal.util.ReflectionUtils;
import com.koloboke.collect.hash.HashConfig;
import com.koloboke.collect.map.LongObjMap;
import com.koloboke.collect.map.ObjLongMap;
import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.map.hash.HashLongObjMaps;
import com.koloboke.collect.map.hash.HashObjLongMap;
import com.koloboke.collect.map.hash.HashObjLongMaps;
import com.koloboke.function.ObjLongToLongFunction;
import org.apache.log4j.Logger;

import java.io.Serializable;

public abstract class LongObjSubgraphAggregation
        <S extends Subgraph, V extends Serializable>
        extends ProducerConsumerSignaling
        implements SubgraphAggregation<S> {
   private static final Logger LOG = Logger.getLogger(
           LongObjSubgraphAggregation.class);

   private static final int MAX_SIZE = 10000;

   private HashLongObjMap<V> keyValueMap;

   public final void init(Configuration configuration) {
      keyValueMap = HashLongObjMaps.getDefaultFactory()
              .withHashConfig(HashConfig.fromLoads(0.5, 0.5, 1))
              .newMutableMap(MAX_SIZE);
   }

   public final void map(long key, V value) {
      final V existing = keyValueMap.get(key);

      if (existing != null) {
         reduce(existing, value);
      } else {
         keyValueMap.put(key, ReflectionUtils.clone(value));
         if (keyValueMap.size() > MAX_SIZE) {
            // wait until map is consumed
            notifyWorkProduced();
            waitWorkConsumed();
            keyValueMap.clear();
         }
      }
   }

   public abstract void reduce(V v1, V v2);

   public final LongObjMap<V> getKeyValueMap() {
      return keyValueMap;
   }
}
