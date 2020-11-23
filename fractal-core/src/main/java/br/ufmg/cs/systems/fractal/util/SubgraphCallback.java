package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

import java.io.Serializable;

public interface SubgraphCallback<S extends Subgraph> extends Serializable {
   SubgraphCallback<PatternInducedSubgraph> defaultPatternInducedCallback =
           new SubgraphCallback<PatternInducedSubgraph>() {
              @Override
              public void apply(PatternInducedSubgraph subgraph, Computation<PatternInducedSubgraph> computation) {

              }

              @Override
              public void init(Computation computation) {

              }
           };
   void apply(S subgraph, Computation<S> computation);
   void init(Computation<S> computation);
}
