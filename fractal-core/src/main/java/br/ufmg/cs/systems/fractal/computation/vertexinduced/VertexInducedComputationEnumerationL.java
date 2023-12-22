package br.ufmg.cs.systems.fractal.computation.vertexinduced;

import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;

public class VertexInducedComputationEnumerationL<S extends VertexInducedSubgraph>
        extends VertexInducedComputation<S> {

   @Override
   public Primitive primitive() {
      return null;
   }

   @Override
   public Primitive[] primitives() {
      return new Primitive[0];
   }

   @Override
   public long processCompute(SubgraphEnumerator<S> subgraphEnumerator) {
      while (subgraphEnumerator.extend_EXTENSION_PRIMITIVE()) process(subgraph);
      return 0;
   }

   @Override
   public boolean filter_FILTERING_PRIMITIVE(S subgraph) {
      throw new UnsupportedOperationException();
   }

   @Override
   public String toString() {
      return "Efl";
   }

}
