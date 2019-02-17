package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.subgraph.VertexEdgeInducedSubgraph;

public abstract class VertexEdgeInducedComputation<E extends VertexEdgeInducedSubgraph> extends BasicComputation<E> {
    @Override
    public final int getInitialNumWords() {
        return getMainGraph().getNumberVertices();
    }

    @Override
    public Class<? extends Subgraph> getSubgraphClass() {
        return VertexEdgeInducedSubgraph.class;
    }
}
