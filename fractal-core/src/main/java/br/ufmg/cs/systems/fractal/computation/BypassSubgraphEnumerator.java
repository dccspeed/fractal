package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import com.koloboke.collect.IntCollection;

public class BypassSubgraphEnumerator<S extends Subgraph> extends SubgraphEnumerator<S> {
   @Override
   public synchronized void newExtensions(IntCollection wordIds) {}

   @Override
   public void computeExtensions() {
      newExtensions(null);
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
