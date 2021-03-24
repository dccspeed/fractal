package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.IntCollection;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

public class SubgraphEnumerator<S extends Subgraph> {
   public static final int INVALID_EXTENSION = Integer.MIN_VALUE;
   protected static final Logger LOG =
           Logger.getLogger(SubgraphEnumerator.class);
   protected Computation<S> computation;
   protected S subgraph;
   protected IntArrayList extensions;
   protected AtomicInteger extensionsIdx;
   protected int extensionsSize;
   private IntArrayList prefix;
   private int prefixSize;

   public SubgraphEnumerator() {
      this.prefix = new IntArrayList();
      this.extensions = new IntArrayList();
      this.extensionsIdx = new AtomicInteger();
   }

   /**
    * This method is used to generate the set of extensions in preparation for
    * extension routines.
    */
   public synchronized void computeExtensions_EXTENSION_PRIMITIVE() {
      extensions.clear();
      subgraph.computeExtensions(computation, extensions);
      this.extensionsSize = this.extensions.size();
      this.prefix.setFrom(subgraph.getWords());
      this.prefixSize = this.prefix.size();
      this.extensionsIdx.set(0);
   }

   public synchronized void computeFirstLevelExtensions_EXTENSION_PRIMITIVE() {
      this.extensions.clear();
      this.subgraph.computeFirstLevelExtensions(computation, extensions);
      this.extensionsSize = this.extensions.size();
      this.prefix.clear();
      this.prefixSize = 0;
      this.extensionsIdx.set(0);
   }

   public synchronized void newExtensions(IntCollection newExtensions) {
      this.extensions.setFrom(newExtensions);
      this.extensionsSize = this.extensions.size();
      this.prefix.setFrom(subgraph.getWords());
      this.prefixSize = this.prefix.size();
      this.extensionsIdx.set(0);
   }

   public boolean extend_EXTENSION_PRIMITIVE() {
      int eidx = extensionsIdx.getAndIncrement();
      if (eidx < extensionsSize) {
         while (subgraph.getNumWords() > prefixSize) subgraph.removeLastWord();
         subgraph.addWord(extensions.getu(eidx));
         return true;
      }
      return false;
   }

   public synchronized boolean forkEnumerator(Computation<S> computation) {
      // prevent last depth steal
      if (this.computation.nextComputation() == null) return false;

      int eidx = extensionsIdx.getAndIncrement();

      if (eidx < extensionsSize) {
         int extension = extensions.getu(eidx);
         SubgraphEnumerator<S> iter = computation.getSubgraphEnumerator();
         synchronized (iter) {
            int prefixWord;

            // prefix and subgraph
            iter.prefix.clear();
            iter.subgraph.reset();
            iter.prefixSize = prefixSize;
            if (prefixSize > 0) {
               prefixWord = prefix.getu(0);
               iter.subgraph.addWord(prefixWord);
               iter.prefix.add(prefixWord);

               for (int i = 1; i < prefixSize; ++i) {
                  prefixWord = prefix.getu(i);
                  iter.subgraph.addWord(prefixWord);
                  iter.prefix.add(prefixWord);
               }
            }

            // extensions
            iter.extensions.clear();
            iter.extensions.add(extension);
            iter.extensionsSize = iter.extensions.size();
            iter.extensionsIdx.set(0);

            iter.rebuildState();
         }
         return true;
      } else {
         return false;
      }
   }

   /**
    * Called after a internal/external work-stealing to reconstruct this
    * enumerator state for an alternative execution thread.
    */
   public void rebuildState() {
      // empty by default
   }

   public Computation<S> getComputation() {
      return this.computation;
   }

   public void setComputation(Computation<S> computation) {
      this.computation = computation;
   }

   public IntArrayList getExtensions() {
      return extensions;
   }

   public IntArrayList getPrefix() {
      return prefix;
   }

   public S getSubgraph() {
      return subgraph;
   }

   public void setSubgraph(S subgraph) {
      this.subgraph = subgraph;
   }

   /**
    * Enumerator initialization. We assume a default no-parameter constructor
    * for the subgraph enumerator and use this method to initialize any internal
    * structures the custom implementation may need.
    *
    * @param config current configuration
    */
   public void init(Configuration config, Computation<S> computation) {
      // empty by default
   }

   public int nextExtension() {
      int eidx = extensionsIdx.getAndIncrement();
      if (eidx < extensionsSize) {
         return extensions.getu(eidx);
      } else {
         return INVALID_EXTENSION;
      }
   }

   public synchronized void terminate() {
      if (extensionsIdx != null) {
         extensionsIdx.set(extensionsSize);
      }
   }

   @Override
   public String toString() {
      return "senum(" + prefix + "," + extensionsIdx + "," + extensions + ")";
   }
}

