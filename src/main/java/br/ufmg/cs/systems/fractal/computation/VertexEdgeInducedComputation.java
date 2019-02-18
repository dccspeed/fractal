package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph;

public abstract class VertexEdgeInducedComputation<E extends PatternInducedSubgraph> extends BasicComputation<E> {
    @Override
    public final int getInitialNumWords() {
        return getMainGraph().getNumberVertices();
    }

    @Override
    public Class<? extends Subgraph> getSubgraphClass() {
        return PatternInducedSubgraph.class;
    }
}
