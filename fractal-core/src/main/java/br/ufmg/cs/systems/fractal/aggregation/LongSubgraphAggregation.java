package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.ReflectionUtils;
import com.koloboke.collect.map.ObjLongMap;
import com.koloboke.collect.map.hash.HashObjLongMap;
import com.koloboke.collect.map.hash.HashObjLongMaps;
import com.koloboke.function.ObjLongToLongFunction;
import org.apache.log4j.Logger;

import java.io.Serializable;

public abstract class LongSubgraphAggregation<S extends Subgraph>
        implements SubgraphAggregation<S> {

   private long value;

   public LongSubgraphAggregation(long defaultValue) {
      this.value = defaultValue;
   }

   public long value() {
      return value;
   }

   @Override
   public final void aggregate(S subgraph) {
      value = reduce(value, value(subgraph));
   }

   public abstract long value(S subgraph);
   public abstract long reduce(long v1, long v2);
}
