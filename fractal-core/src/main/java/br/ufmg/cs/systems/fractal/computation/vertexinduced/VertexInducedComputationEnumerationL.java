package br.ufmg.cs.systems.fractal.computation.vertexinduced;

import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;

import javax.annotation.OverridingMethodsMustInvokeSuper;

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
      while (subgraphEnumerator.extend()) process(subgraph);
      return 0;
   }

   @Override
   public boolean filter(S subgraph) {
      throw new UnsupportedOperationException();
   }

   @Override
   public String toString() {
      return "Efl";
   }

}
