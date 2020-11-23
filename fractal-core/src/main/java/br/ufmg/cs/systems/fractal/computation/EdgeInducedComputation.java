package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.subgraph.EdgeInducedSubgraph;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

public abstract class EdgeInducedComputation<E extends EdgeInducedSubgraph> extends BasicComputation<E> {
    @Override
    public final int getInitialNumWords() {
        return getMainGraph().numEdges();
    }

    @Override
    public Class<? extends Subgraph> getSubgraphClass() {
        return EdgeInducedSubgraph.class;
    }

}
