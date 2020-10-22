package br.ufmg.cs.systems.fractal.computation.vertexinduced;

import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import br.ufmg.cs.systems.fractal.computation.WorkStealingSystem2;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;

public class VertexInducedComputationEnumerationF<S extends VertexInducedSubgraph>
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
      // compute extensions
      subgraphEnumerator.computeExtensions();

      // originally assigned computation
      processExtensions();

      // work stealing computation
      if (configuration.wsEnabled()) {
         // create work stealing system
         WorkStealingSystem2<S> wsSys = new WorkStealingSystem2<>(this);
         wsSys.workStealingCompute();
         // work stealing computation
      }
   }

   @Override
   public void processExtensions() {
      while (subgraphEnumerator.extend()) {
         nextComputation().computeAndProcessExtensions();
      }
   }

   @Override
   public String toString() {
      return "Ef";
   }

}
