package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;

import java.util.Random;

/*
 * Visits a fraction of subgraphs uniformly at random.
 */
public class SamplingEnumerator<S extends Subgraph> extends SubgraphEnumerator<S> {
   protected double fraction;
   protected double depth;
   protected double depthProb;
   protected IntArrayList sampledExtensions;
   private Random rand;

   @Override
   public void init(Configuration config, Computation<S> computation) {
      rand = new Random();
      sampledExtensions = new IntArrayList();
      fraction = config.getDouble("sampling_fraction", -1.0); 
      if (fraction <= 0) {
         throw new RuntimeException("Invalid/missing sampling fraction (" +
               "sampling_fraction=" + fraction + ")");
      }

      //Primitive[] primitives = config.getObject("primitives_workflow", null);
      Primitive[] primitives = computation.getExecutionEngine().primitives();
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

      // this is the probability used for each depth but the first
      // note that the product of the probabilities on each level must be equal
      // to *fraction*
      depthProb = Math.pow(fraction, 1 / (depth - 1));
   }

   @Override
   public void computeExtensions_EXTENSION_PRIMITIVE() {
      super.computeExtensions_EXTENSION_PRIMITIVE();
      if (getPrefix().size() == 0) return;

      sampledExtensions.clear();
      for (int i = 0; i < extensions.size(); ++i) {
         if (rand.nextDouble() <= depthProb) {
            sampledExtensions.add(extensions.getu(i));
         }
      }

      newExtensions(sampledExtensions);
   }
}
