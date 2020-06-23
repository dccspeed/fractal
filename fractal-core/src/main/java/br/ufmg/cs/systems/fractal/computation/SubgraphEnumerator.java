package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.IntCursor;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class SubgraphEnumerator<S extends Subgraph> implements Iterator<S> {
   protected static final Logger LOG = Logger.getLogger(SubgraphEnumerator.class);

   protected ReentrantLock rlock;

   protected Computation<S> computation;

   protected IntArrayList prefix;
   
   protected S subgraph;

   protected boolean lastHasNext;

   protected int currElem;

   protected IntCollection wordIds;

   protected IntCursor cur;

   protected boolean shouldRemoveLastWord;
   
   protected AtomicBoolean active;

   public String computationLabel() {
      return computation.computationLabel();
   }

   public boolean isActive() {
      return active != null && active.get();
   }

   public SubgraphEnumerator() {
      this.rlock = new ReentrantLock();
      this.prefix = IntArrayListPool.instance().createObject();
   }

   public synchronized void terminate() {
      if (active == null || rlock == null) return;
      try {
         rlock.lock();
         active.set(false);
      } finally {
         rlock.unlock();
      }
   }

   /**
    * Enumerator initialization. We assume a default no-parameter constructor
    * for the subgraph enumerator and use this method to initialize any internal
    * structures the custom implementation may need.
    * @param config current configuration
    */
   public void init(Configuration<S> config, Computation<S> computation) {
      // empty by default
   }

   /**
    * Called after a internal/external work-stealing to reconstruct this
    * enumerator state for an alternative execution thread.
    */
   public void rebuildState(SubgraphEnumerator<S> that) {
      // empty by default
   }

   /**
    * Called after a internal/external work-stealing to reconstruct this
    * enumerator state for an alternative execution thread.
    */
   public void rebuildState() {
      // empty by default
   }

   /**
    * This method is used to generate the set of extensions in preparation for
    * extension routines.
    */
   public void computeExtensions() {
      IntCollection extensions = subgraph.computeExtensions(computation);
      set(extensions);
   }

   /**
    * An extend call consumes an extension and returns the next enumerator,
    * equivalent to the current one plus the extension. The default
    * implementation is memory efficient because it reuses the same structure
    * in-place for further extensions (returns 'this').
    * @return the updated extended subgraph enumerator
    */
   public SubgraphEnumerator<S> extend() {
      S subgraph = next();
      Computation<S> nextComp = computation.nextComputation();
      SubgraphEnumerator<S> nextEnum = nextComp.getSubgraphEnumerator();
      nextEnum.set(nextComp, subgraph, this);
      return nextEnum;
      //return this;
   }

   public synchronized SubgraphEnumerator<S> set(
           Computation<S> computation, S subgraph,
           SubgraphEnumerator<S> previous) {
      this.computation = computation;
      this.subgraph = subgraph;
      return this;
   }


   public synchronized SubgraphEnumerator<S> set(IntCollection wordIds) {
      this.prefix.clear();
      this.prefix.addAll(subgraph.getWords());
      this.lastHasNext = false;
      this.currElem = -1;
      this.wordIds = wordIds;
      this.cur = wordIds.cursor();
      this.shouldRemoveLastWord = false;
      this.active = new AtomicBoolean(true);
      return this;
   }

   public synchronized SubgraphEnumerator<S> forkEnumerator(Computation<S> computation) {
      // create new consumer, adding just enough to verify if there is still
      // work in it
      SubgraphEnumerator<S> iter = computation.
              getConfig().createSubgraphEnumerator(computation);
      iter.subgraph = computation.getConfig().createSubgraph();
      iter.rlock = this.rlock;
      iter.computation = computation;
      iter.lastHasNext = false;
      iter.cur = this.cur;
      iter.wordIds = this.wordIds;
      iter.shouldRemoveLastWord = false;
      iter.active = this.active;

      // expensive operations, only do if iterator is not empty
      if (iter.hasNext()) {
         iter.prefix.clear();
         iter.prefix.addAll(this.prefix);

         if (prefix.size() > 0) {
            iter.subgraph.addWord(prefix.getu(0));
         }

         for (int i = 1; i < prefix.size(); ++i) {
            iter.subgraph.nextExtensionLevel(subgraph);
            iter.subgraph.addWord(prefix.getu(i));
         }

         //iter.rebuildState(this);
         iter.rebuildState();
      }

      return iter;
   }

   private void maybeRemoveLastWord() {
      if (shouldRemoveLastWord) {
         subgraph.removeLastWord();
         shouldRemoveLastWord = false;
      }
   }

   @Override
   public boolean hasNext() {
      // if currElem has a valid word to be consumed
      if (lastHasNext) {
         return true;
      }

      // this test is to make sure we do not remove the last word in the
      // first *hasNext* call
      maybeRemoveLastWord();

      try {
         rlock.lock();
         if (isActive()) {
            // skip extensions that turn the subgraph not canonical
            while (cur.moveNext()) {
               currElem = cur.elem();
               lastHasNext = true;
               return true;
            }
            active.set(false);
         } else {
            maybeRemoveLastWord();
         }
         return false;
      } finally {
         rlock.unlock();
      }
   }

   @Override
   public S next() {
      shouldRemoveLastWord = true;
      subgraph.addWord(nextElem());
      return subgraph;
   }

   public int nextElem() {
      lastHasNext = false;
      return currElem;
   }

   @Override
   public void remove() {
      throw new UnsupportedOperationException();
   }

   public Computation<S> getComputation() {
      return this.computation;
   }

   public IntArrayList getPrefix() {
      return prefix;
   }

   public S getSubgraph() {
      return subgraph;
   }

   public IntCollection getWordIds() {
      return wordIds;
   }

   @Override
   public String toString() {
      return "SubgraphEnumerator(" +
         "active=" + active +
              ",cur=" + cur +
              ",wordIds=" + wordIds +
              ",shouldRemoveLastWord=" + shouldRemoveLastWord +
              ",subgraph=" + subgraph +
         ",prefix=" + prefix + ")";
   }
}

