package br.ufmg.cs.systems.fractal.computation.vertexinduced;

import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.computation.BasicComputation;
import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;
import br.ufmg.cs.systems.fractal.util.ReflectionSerializationUtils;

public abstract class VertexInducedComputation<E extends VertexInducedSubgraph> extends BasicComputation<E> {
    @Override
    public final int getInitialNumWords() {
        return getMainGraph().numVertices();
    }

    @Override
    public Class<? extends Subgraph> getSubgraphClass() {
        return VertexInducedSubgraph.class;
    }

    @Override
    public String asPrimitiveString() {
        if (primitive() == Primitive.F) {
            return "F(p)";
        } else {
            SubgraphEnumerator senum = ReflectionSerializationUtils.newInstance(
                    getSubgraphEnumeratorClass());
            return String.format("E(Tv,%s)",
                    senum.asExtensionMethodString(this));
        }
    }

}
