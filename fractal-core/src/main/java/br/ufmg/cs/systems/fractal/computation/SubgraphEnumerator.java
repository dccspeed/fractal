package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.EdgeInducedSubgraph;
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;
import br.ufmg.cs.systems.fractal.util.ReflectionSerializationUtils;
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
   protected IntArrayList prefix;
   protected int prefixSize;

   private boolean workStealingAllowed;

   public SubgraphEnumerator() {
      this.prefix = new IntArrayList();
      this.extensions = new IntArrayList();
      this.extensionsIdx = new AtomicInteger();
      this.workStealingAllowed = true;
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
      if (!computation.isActive()) return;
      this.extensions.setFrom(newExtensions);
      resetExtensions();
   }

   private synchronized final void resetExtensions() {
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

   public synchronized boolean forkEnumerator(
           Computation<S> computation, boolean updateState) {

      // this enumerator is busy with another work stealing event
      if (!workStealingAllowed) return false;

      // prevent last depth steal
      if (computation.nextComputation() == null) return false;

      int eidx = extensionsIdx.getAndIncrement();
      if (eidx < extensionsSize) {
         int extension = extensions.getu(eidx);
         SubgraphEnumerator<S> iter = computation.getSubgraphEnumerator();
         synchronized (iter) {
            //maybeUpdateState(iter, prefix, updateState);
            iter.maybeUpdateState(prefix, updateState);
            iter.extensions.clear();
            iter.extensions.add(extension);
            iter.extensionsSize = iter.extensions.size();
            iter.extensionsIdx.set(0);
         }
         return true;
      } else {
         return false;
      }
   }

   private synchronized final void updateStateComputeExtensionsFirstLevel() {
      subgraph.reset();
      computeFirstLevelExtensions_EXTENSION_PRIMITIVE();
      extensionsIdx.set(extensionsSize);
   }

   private synchronized final void updateStateComputeExtensions() {
      computeExtensions_EXTENSION_PRIMITIVE();
      extensionsIdx.set(extensionsSize);
   }

   private synchronized final void updateStateExtend(int prefixWord) {
      extensions.clear();
      extensions.add(prefixWord);
      extensionsSize = extensions.size();
      extensionsIdx.set(0);
      extend_EXTENSION_PRIMITIVE();
   }

   public synchronized final void maybeUpdateState(IntArrayList newPrefix,
                                             boolean updateStateAllowed) {
      int newPrefixSize = newPrefix.size();

      if (!updateStateAllowed || !shouldRebuildState()) {
         int prefixWord;
         // prefix and subgraph
         prefix.clear();
         subgraph.reset();
         prefixSize = newPrefixSize;
         if (newPrefixSize > 0) {
            prefixWord = newPrefix.getu(0);
            subgraph.addWord(prefixWord);
            prefix.add(prefixWord);

            for (int i = 1; i < newPrefixSize; ++i) {
               prefixWord = newPrefix.getu(i);
               subgraph.addWord(prefixWord);
               prefix.add(prefixWord);
            }
         }
      } else {
         // find root subgraph enumerator and disallow work stealing temporarily
         SubgraphEnumerator<S> currEnum = this;
         SubgraphEnumerator<S> prevEnum = currEnum.previousEnumerator();
         while (prevEnum != null) {
            currEnum = prevEnum;
            prevEnum = currEnum.previousEnumerator();
         }

         currEnum.updateStateComputeExtensionsFirstLevel();
         if (newPrefixSize > 0) {
            currEnum.updateStateExtend(newPrefix.getu(0));
            currEnum = currEnum.nextEnumerator();
            currEnum.updateStateComputeExtensions();

            for (int i = 1; i < newPrefixSize; ++i) {
               currEnum.updateStateExtend(newPrefix.getu(i));
               currEnum = currEnum.nextEnumerator();
               currEnum.updateStateComputeExtensions();
            }
         }
      }
   }

   public boolean shouldRebuildState() {
      return false;
   }

   public final Computation<S> getComputation() {
      return this.computation;
   }

   public final void setComputation(Computation<S> computation) {
      this.computation = computation;
   }

   public final IntArrayList getExtensions() {
      return extensions;
   }

   public final IntArrayList getPrefix() {
      return prefix;
   }

   public final S getSubgraph() {
      return subgraph;
   }

   public final void setSubgraph(S subgraph) {
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

   public final int nextExtension() {
      int eidx = extensionsIdx.getAndIncrement();
      if (eidx < extensionsSize) {
         while (subgraph.getNumWords() > prefixSize) subgraph.removeLastWord();
         return extensions.getu(eidx);
      } else {
         return INVALID_EXTENSION;
      }
   }

   public final synchronized void terminate() {
      if (extensionsIdx != null) {
         extensionsIdx.set(extensionsSize);
      }
   }

   public String asExtensionMethodString(Computation<S> computation) {
      S subgraph = (S) ReflectionSerializationUtils.newInstance(
                      computation.getSubgraphClass());
      Class<S> sclass = (Class<S>) subgraph.getClass();
      if (VertexInducedSubgraph.class.isAssignableFrom(sclass)) {
         return "Mc";
      } else if (EdgeInducedSubgraph.class.isAssignableFrom(sclass)) {
         return "Mc";
      } else if (PatternInducedSubgraph.class.isAssignableFrom(sclass)) {
         return "Mp";
      } else {
         throw new RuntimeException("Unknown subgraph class " + sclass);
      }
   }

   public final <SE extends SubgraphEnumerator<S>> SE nextEnumerator() {
      Computation<S> nextComputation = computation.nextComputation();
      while (nextComputation != null && nextComputation.getSubgraphEnumerator() instanceof BypassSubgraphEnumerator) {
         nextComputation = nextComputation.nextComputation();
      }
      if (nextComputation != null) {
         return (SE) nextComputation.getSubgraphEnumerator();
      } else {
         return null;
      }
   }

   public final <SE extends SubgraphEnumerator<S>> SE previousEnumerator() {
      Computation<S> previousComputation = computation.previousComputation();
      while (previousComputation != null && previousComputation.getSubgraphEnumerator() instanceof BypassSubgraphEnumerator) {
         previousComputation = previousComputation.previousComputation();
      }
      if (previousComputation != null) {
         return (SE) previousComputation.getSubgraphEnumerator();
      } else {
         return null;
      }
   }

   @Override
   public String toString() {
      return "senum(" + prefix + "," + extensionsIdx + "," + extensions + ")";
   }
}

