package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import org.apache.log4j.Logger;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/*
 * Visits a fraction of subgraphs uniformly at random.
 */
public class RandomWalkEnumerator<S extends Subgraph> extends SubgraphEnumerator<S> {
   protected static final Logger LOG =
           Logger.getLogger(SubgraphEnumerator.class);
   private Random rand;
   private int samplesPerThread;

   @Override
   public void init(Configuration config, Computation<S> computation) {
      long seed = config.getLong("random_walk_seed", 1L);
      seed += computation.getPartitionId();
      rand = new Random(seed);
      samplesPerThread = config.getInteger("samples_per_thread", 1);
   }

   @Override
   public void computeFirstLevelExtensions_EXTENSION_PRIMITIVE() {
      int numVertices = subgraph.getMainGraph().numVertices();
      extensions.clear();
      for (int i = 0; i < samplesPerThread; ++i) {
         extensions.add(rand.nextInt(numVertices));
      }
      newExtensions(extensions);
   }

   @Override
   public void computeExtensions_EXTENSION_PRIMITIVE() {
      super.computeExtensions_EXTENSION_PRIMITIVE();
      if (getPrefix().size() == 0) return;
      if (extensions.isEmpty()) return;

      int sampleIdx = rand.nextInt(extensions.size());
      for (int i = 0; i < extensions.size(); ++i) {
         if (extensions.getu(sampleIdx) > extensions.getu(i)) {
            sampleIdx = i;
         }
      }

      int sample = extensions.get(sampleIdx);
      extensions.clear();
      extensions.add(sample);

      newExtensions(extensions);
   }
}
