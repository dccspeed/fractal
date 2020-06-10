package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

public interface SubgraphAggregation<S extends Subgraph> {
   void aggregate(S subgraph);
}
