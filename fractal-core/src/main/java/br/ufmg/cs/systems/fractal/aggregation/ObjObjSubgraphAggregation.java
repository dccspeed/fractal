package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.ProducerConsumerSignaling;
import br.ufmg.cs.systems.fractal.util.ReflectionUtils;
import com.koloboke.collect.map.ObjObjMap;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import org.apache.log4j.Logger;

import java.io.Serializable;

public abstract class ObjObjSubgraphAggregation
        <S extends Subgraph, K extends Serializable, V extends Serializable>
        extends ProducerConsumerSignaling
        implements SubgraphAggregation<S> {
   private static final Logger LOG = Logger.getLogger(ObjObjSubgraphAggregation.class);

   private static final int MAX_SIZE = 10000;

   private ObjObjMap<K,V> keyValueMap;

   public final void init(Configuration configuration) {
      keyValueMap = HashObjObjMaps.newUpdatableMap();
   }

   public final void map(K key, V value) {
      final V existingValue = keyValueMap.get(key);

      if (existingValue != null) {
         reduce(existingValue, value);
      } else {
         keyValueMap.put(ReflectionUtils.clone(key),
                 ReflectionUtils.clone(value));
         if (keyValueMap.size() > MAX_SIZE) {
            // wait until map is consumed
            notifyWorkProduced();
            waitWorkConsumed();
            keyValueMap.clear();
         }
      }
   }

   public abstract void reduce(V existingValue, V otherValue);

   public final ObjObjMap<K,V> getKeyValueMap() {
      return keyValueMap;
   }

}
