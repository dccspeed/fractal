package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

import java.io.Serializable;

public interface SubgraphAggregation<S extends Subgraph> extends Serializable {
   void aggregate(S subgraph);
}
