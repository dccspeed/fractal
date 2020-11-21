package br.ufmg.cs.systems.fractal.computation.vertexinduced;

import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;

public class VertexInducedComputationEnumerationF<S extends VertexInducedSubgraph>
        extends VertexInducedComputation<S> {

   @Override
   public boolean filter(S subgraph) {
      throw new UnsupportedOperationException();
   }

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
      while (subgraphEnumerator.extend()) {
         nextComputation.compute();
      }
      return 0;
   }

   @Override
   public String toString() {
      return "Ef";
   }

}
