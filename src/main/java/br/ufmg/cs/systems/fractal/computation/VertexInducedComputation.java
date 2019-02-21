package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;

public abstract class VertexInducedComputation<E extends VertexInducedSubgraph> extends BasicComputation<E> {
    @Override
    public final int getInitialNumWords() {
        return getMainGraph().getNumberVertices();
    }

    @Override
    public Class<? extends Subgraph> getSubgraphClass() {
        return VertexInducedSubgraph.class;
    }

    @Override
    public boolean containsWord(int vertexId) {
       return getMainGraph().getVertex(vertexId) != null;
    }
}
