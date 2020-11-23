package br.ufmg.cs.systems.fractal.computation.vertexinduced;

import br.ufmg.cs.systems.fractal.computation.BasicComputation;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;

public abstract class VertexInducedComputation<E extends VertexInducedSubgraph> extends BasicComputation<E> {
    @Override
    public final int getInitialNumWords() {
        return getMainGraph().numVertices();
    }

    @Override
    public Class<? extends Subgraph> getSubgraphClass() {
        return VertexInducedSubgraph.class;
    }

}
