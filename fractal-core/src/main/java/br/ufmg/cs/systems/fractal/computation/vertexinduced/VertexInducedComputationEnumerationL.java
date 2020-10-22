package br.ufmg.cs.systems.fractal.computation.vertexinduced;

import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;

import java.util.function.IntConsumer;

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
      IntArrayList extensions = subgraphEnumerator.getExtensions();
      int numExtensions = extensions.size();
      for (int i = 0; i < numExtensions; ++i) {
         subgraph.addWord(extensions.getu(i));
         process(subgraph);
         subgraph.removeLastWord();
      }
   }

   @Override
   public String toString() {
      return "El";
   }
}
