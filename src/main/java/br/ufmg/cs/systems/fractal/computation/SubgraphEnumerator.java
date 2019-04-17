package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.IntCursor;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

public class SubgraphEnumerator<E extends Subgraph> implements Iterator<E> {
   private static final Logger LOG = Logger.getLogger(SubgraphEnumerator.class);

   private static final SubgraphEnumerator emptyIter = new SubgraphEnumerator() {
      @Override
      public boolean isActive() {
         return false;
      }
      @Override
      public boolean hasNext() {
         return false;
      }
   };

   protected ReentrantLock rlock;

   protected Computation<E> computation;

   protected IntArrayList prefix;
   
   protected E subgraph;

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

   public void init(Configuration<E> config) {
   }

   public synchronized SubgraphEnumerator<E> set(
           Computation<E> computation, E subgraph) {
      this.computation = computation;
      this.subgraph = subgraph;
      return this;
   }

   public synchronized void computeExtensions() {
      IntCollection extensions = subgraph.computeExtensions(computation);
      set(extensions);
   }

   public synchronized SubgraphEnumerator<E> set(IntCollection wordIds) {
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

   public synchronized SubgraphEnumerator<E> forkEnumerator() {
      // create new consumer, adding just enough to verify if there is still
      // work in it
      SubgraphEnumerator<E> iter = new SubgraphEnumerator<E>();
      iter.subgraph = computation.getConfig().createSubgraph();
      iter.rlock = this.rlock;
      iter.computation = this.computation;
      iter.lastHasNext = false;
      iter.cur = this.cur;
      iter.wordIds = this.wordIds;
      iter.shouldRemoveLastWord = false;
      iter.active = this.active;

      // expensive operations, only do if iterator is not empty
      if (iter.hasNext()) {
         iter.prefix = IntArrayListPool.instance().createObject();
         iter.prefix.addAll(this.prefix);

         if (prefix.size() > 0) {
            iter.subgraph.addWord(prefix.getUnchecked(0));
         }

         for (int i = 1; i < prefix.size(); ++i) {
            iter.subgraph.nextExtensionLevel(subgraph);
            iter.subgraph.addWord(prefix.getUnchecked(i));
         }

         iter.subgraph.setState(null);
      }

      return iter;
   }

   public synchronized void joinConsumer() {
      IntArrayListPool.instance().reclaimObject(prefix);
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
               if (computation.filter(subgraph, currElem)) {
                  lastHasNext = true;
                  return true;
               }
            }
            active.set(false);
         } else {
            maybeRemoveLastWord();
         }
         return false;
      } catch (java.util.ConcurrentModificationException e) {
         throw new RuntimeException(e + " " + this);
      } finally {
         rlock.unlock();
      }
   }

   @Override
   public synchronized E next() {
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

   public Computation<E> getComputation() {
      return this.computation;
   }

   public IntArrayList getPrefix() {
      return prefix;
   }

   public E getSubgraph() {
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

