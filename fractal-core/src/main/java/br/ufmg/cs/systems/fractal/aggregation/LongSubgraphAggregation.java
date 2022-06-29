package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

public abstract class LongSubgraphAggregation<S extends Subgraph>
        implements SubgraphAggregation<S> {

   private long value;

   public final void init(Configuration configuration) {
      value = defaultValue();
   }

   public abstract long defaultValue();

   public abstract long reduce(long v1, long v2);

   public final void map(long value) {
      this.value = reduce(this.value, value);
   }

   public long value() { return value; }

}
