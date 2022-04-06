package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.ProducerConsumerSignaling;
import com.koloboke.collect.hash.HashConfig;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.function.IntIntToIntFunction;
import org.apache.log4j.Logger;

public abstract class IntIntSubgraphAggregation<S extends Subgraph>
        extends ProducerConsumerSignaling
        implements SubgraphAggregation<S>, IntIntToIntFunction {
   private static final Logger LOG = Logger.getLogger(
           IntIntSubgraphAggregation.class);

   private static final int MAX_SIZE = 10000;

   private transient IntIntMap keyValueMap;
   private transient IntIntCursor cursor;
   private transient int newValue;

   public final void init(Configuration configuration) {
      keyValueMap = HashIntIntMaps.getDefaultFactory()
              .withDefaultValue(defaultValue())
              .withHashConfig(HashConfig.fromLoads(0.5, 0.5, 1))
              .newMutableMap(MAX_SIZE);
   }

   public abstract int reduce(int v1, int v2);
   public abstract int defaultValue();

   protected final void map(int key, int value) {
      newValue = value;
      keyValueMap.compute(key, this);
      if (keyValueMap.size() > MAX_SIZE) {
         // wait until map is consumed
         cursor = keyValueMap.cursor();
         notifyWorkProduced();
         waitWorkConsumed();
         keyValueMap.clear();
      }
   }

   public final IntIntMap getKeyValueMap() { return keyValueMap; }

   @Override
   public final int applyAsInt(int k, int existing) {
      return reduce(newValue, existing);
   }
}
