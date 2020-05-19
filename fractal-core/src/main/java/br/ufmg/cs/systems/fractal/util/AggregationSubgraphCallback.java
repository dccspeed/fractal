package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.aggregation.AggregationStorage;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import org.apache.hadoop.io.Writable;

public interface AggregationSubgraphCallback<S extends Subgraph,
        K extends Writable, V extends Writable> extends SubgraphCallback<S> {
   AggregationStorage<K,V> aggregationStorage();
}
