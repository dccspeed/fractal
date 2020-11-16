package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import com.koloboke.collect.IntCollection;

public class BypassSubgraphEnumerator<S extends Subgraph>
        extends SubgraphEnumerator<S> {

   @Override
   public void computeExtensions() {
   }

   @Override
   public synchronized void newExtensions(IntCollection wordIds) {
   }

   @Override
   public boolean extend() {
      return false;
   }

   @Override
   public synchronized boolean forkEnumerator(Computation<S> computation) {
      return false;
   }

   @Override
   public String toString() {
      return "emptyEnumerator";
   }
}
