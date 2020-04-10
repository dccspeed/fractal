package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

/*
 * Visits a fraction of subgraphs uniformly at random.
 */
public class SamplingEnumerator<S extends Subgraph> extends SubgraphEnumerator<S> {
   protected double fraction;
   protected double depth;
   protected double depthProb;

   @Override
   public void init(Configuration<S> config) {
      fraction = config.getDouble("sampling_fraction", -1.0); 
      if (fraction <= 0) {
         throw new RuntimeException("Invalid/missing sampling fraction (" +
               "sampling_fraction=" + fraction + ")");
      }

      Primitive[] primitives = config.getObject("primitives_workflow", null);
      depth = 0;
      for (int i = 0; i < primitives.length; ++i) {
         if (primitives[i] == Primitive.E) {
            depth++;
         }
      }

      //depth = config.getInteger("sampling_depth", -1); 
      if (depth <= 0) {
         throw new RuntimeException("Invalid/missing sampling depth (" +
               "sampling_depth=" + depth + ")");
      }

      // this is the probability used for each depth but the first and last
      // note that the product of the probabilities on each level must be equal
      // to *fraction*
      depthProb = Math.pow(fraction, 1 / (depth - 2));
   }

   @Override
   public boolean hasNext() {
      while (super.hasNext()) {
         // distribute evenly the probabilities except for the first and last
         // sampling depth
         double prob = subgraph.getNumWords() > 0 ? depthProb : 1;
         double n = ThreadLocalRandom.current().nextDouble();
         if (n <= prob) {
            return true;
         } else {
            next(); // ignore this rejected word
         }
      }
      return false;
   }
}
