package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

public abstract class PatternInducedComputation<E extends PatternInducedSubgraph> extends BasicComputation<E> {
    @Override
    public final int getInitialNumWords() {
        return getMainGraph().numVertices();
    }

    @Override
    public Class<? extends Subgraph> getSubgraphClass() {
        return PatternInducedSubgraph.class;
    }
    
    @Override
    public boolean containsWord(int vertexId) {
       return getMainGraph().containsVertex(vertexId);
    }
}
