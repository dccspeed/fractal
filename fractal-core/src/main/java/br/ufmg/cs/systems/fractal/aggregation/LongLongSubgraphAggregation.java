package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.ProducerConsumerSignaling;
import com.koloboke.collect.hash.HashConfig;
import com.koloboke.collect.map.LongLongMap;
import com.koloboke.collect.map.hash.HashLongLongMaps;
import com.koloboke.function.LongLongToLongFunction;
import org.apache.log4j.Logger;

public abstract class LongLongSubgraphAggregation<S extends Subgraph>
        extends ProducerConsumerSignaling
        implements SubgraphAggregation<S>, LongLongToLongFunction {
   private static final Logger LOG = Logger.getLogger(
           LongLongSubgraphAggregation.class);

   private static final int MAX_SIZE = 10000;

   private transient LongLongMap keyValueMap;
   private transient long newValue;

   public final void init(Configuration configuration) {
      keyValueMap = HashLongLongMaps.getDefaultFactory()
              .withDefaultValue(defaultValue())
              .withHashConfig(HashConfig.fromLoads(0.5, 0.5, 1))
              .newMutableMap(MAX_SIZE);
   }

   public abstract long reduce(long v1, long v2);
   public long defaultValue() { return 0; }

   protected final void map(long key, long value) {
      newValue = value;
      keyValueMap.compute(key, this);
      if (keyValueMap.size() > MAX_SIZE) {
         // wait until map is consumed
         notifyWorkProduced();
         waitWorkConsumed();
         keyValueMap.clear();
      }
   }

   public final LongLongMap getKeyValueMap() { return keyValueMap; }

   @Override
   public final long applyAsLong(long k, long existing) {
      return reduce(newValue, existing);
   }
}
