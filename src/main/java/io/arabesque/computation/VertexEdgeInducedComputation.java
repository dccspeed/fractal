package io.arabesque.computation;

import io.arabesque.embedding.VertexEdgeInducedEmbedding;
import io.arabesque.embedding.Embedding;

public abstract class VertexEdgeInducedComputation<E extends VertexEdgeInducedEmbedding> extends BasicComputation<E> {
    @Override
    public final int getInitialNumWords() {
        return getMainGraph().getNumberVertices();
    }

    @Override
    public Class<? extends Embedding> getEmbeddingClass() {
        return VertexEdgeInducedEmbedding.class;
    }
}
