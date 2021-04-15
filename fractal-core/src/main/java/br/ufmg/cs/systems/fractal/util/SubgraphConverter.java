package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

import java.io.Serializable;

public interface SubgraphConverter<IN extends Subgraph, OUT extends Subgraph>
        extends SubgraphCallback<IN> {
   void convert(IN subgraphIn, Computation<IN> computationIn,
                OUT subgraphOut, Computation<OUT> computationOut);
}
