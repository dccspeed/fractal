package br.ufmg.cs.systems.fractal.callback;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

public interface SubgraphConverter<IN extends Subgraph, OUT extends Subgraph>
        extends SubgraphCallback<IN> {
   boolean convert(IN subgraphIn, Computation<IN> computationIn,
                OUT subgraphOut, Computation<OUT> computationOut);
}
