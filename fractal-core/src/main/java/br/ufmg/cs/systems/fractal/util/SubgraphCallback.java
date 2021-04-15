package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

import java.io.Serializable;

public interface SubgraphCallback<S extends Subgraph> extends Serializable {
   void apply(S subgraph, Computation<S> computation);
   void init(Computation<S> computation);
}
