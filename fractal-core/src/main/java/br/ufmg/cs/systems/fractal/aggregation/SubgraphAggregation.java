package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.computation.ExecutionEngine;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

import java.io.Serializable;

public interface SubgraphAggregation<S extends Subgraph> extends Serializable {
   void aggregate_AGGREGATION_PRIMITIVE(S subgraph);
   void report(ExecutionEngine<S> engine);
}
