package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import org.apache.log4j.Logger;

import java.util.Random;

/*
 * Visits subgraphs around a random word.
 */
public class AroundWordEnumerator<S extends Subgraph> extends SubgraphEnumerator<S> {
   protected static final Logger LOG =
           Logger.getLogger(SubgraphEnumerator.class);
   private Random rand;

   @Override
   public void init(Configuration config, Computation<S> computation) {
      //long seed = config.getLong("sampling_seed", 1L);
      rand = new Random();
   }

   @Override
   public void computeFirstLevelExtensions_EXTENSION_PRIMITIVE() {
      int numWords = computation.getInitialNumWords();
      extensions.clear();
      int randomInitialWord = rand.nextInt(numWords);
      LOG.warn("RandomWord " + randomInitialWord);
      for (int u = randomInitialWord; u < numWords; ++u) {
         extensions.add(u);
      }
      newExtensions(extensions);
   }
}
