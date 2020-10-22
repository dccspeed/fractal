package br.ufmg.cs.systems.fractal.computation.vertexinduced;

import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;

public class VertexInducedComputationEnumerationM<S extends VertexInducedSubgraph>
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
   public long processCompute(SubgraphEnumerator<S> expansions) {
      return 0;
   }

   @Override
   public void computeAndProcessExtensions() {
      subgraphEnumerator.computeExtensions();
      processExtensions();
   }

   @Override
   public void processExtensions() {
      while (subgraphEnumerator.extend()) {
         nextComputation().computeAndProcessExtensions();
      }
   }

   @Override
   public String toString() {
      return "Em";
   }
}
