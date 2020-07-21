package br.ufmg.cs.systems.fractal.computation;

import akka.actor.ActorRef;
import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.IntConsumer;

public class VertexInducedComputationEFL<S extends VertexInducedSubgraph>
        extends VertexInducedComputation<S> {
   transient private LastStepConsumer lastStepConsumer = new LastStepConsumer();

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
   public long compute(S subgraph) {
      // compute extensions
      subgraphEnumerator.computeExtensions();

      // originally assigned computation
      processExtensions();

      // work stealing computation
      if (configuration.wsEnabled()) {
         // create work stealing system

         // work stealing computation
      }

      return 0;
   }

   @Override
   public void processExtensions() {
      subgraphEnumerator.getWordIds().forEach(lastStepConsumer);
   }

   private class LastStepConsumer implements IntConsumer {
      private VertexInducedComputationEFL<S> computation =
              VertexInducedComputationEFL.this;

      private S subgraph = computation.subgraphEnumerator.getSubgraph();

      @Override
      public void accept(int u) {
         subgraph.addWord(u);
         computation.process(subgraph);
         subgraph.removeLastWord();
      }
   }
}
