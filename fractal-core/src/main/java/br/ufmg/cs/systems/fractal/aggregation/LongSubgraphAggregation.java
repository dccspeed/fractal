package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

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
