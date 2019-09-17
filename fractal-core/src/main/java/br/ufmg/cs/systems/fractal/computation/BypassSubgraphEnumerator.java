package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import com.koloboke.collect.IntCollection;

public class BypassSubgraphEnumerator<S extends Subgraph> extends SubgraphEnumerator<S> {
  private boolean hasNext;

  @Override
  public synchronized SubgraphEnumerator<S> set(
          Computation<S> computation, S subgraph) {
    this.computation = computation;
    this.subgraph = subgraph;
    this.hasNext = true;
    return this;
  }

  @Override
  public synchronized SubgraphEnumerator<S> set(IntCollection wordIds) {
    this.wordIds = wordIds;
    return this;
  }

  @Override
  public void computeExtensions() {
    set(null);
  }

  @Override
  public synchronized SubgraphEnumerator<S> forkEnumerator(Computation<S> computation) {
    return this;
  }

  @Override
  public boolean isActive() {
    return false;
  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public S next() {
    hasNext = false;
    return subgraph;
  }

  @Override
  public String toString() {
    return "emptyEnumerator";
  }
}
