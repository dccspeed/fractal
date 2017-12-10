package io.arabesque.computation;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.IntCursor;
import io.arabesque.embedding.Embedding;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.pool.IntArrayListPool;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class EmbeddingIterator<E extends Embedding> implements Iterator<E> {
   private static final Logger LOG = Logger.getLogger(EmbeddingIterator.class);

   protected EmbeddingIterator<E> parent;

   protected ReentrantLock rlock;

   protected Computation<E> computation;

   protected IntArrayList prefix;

   protected E embedding;

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

   public synchronized void invalidate() {
      active.set(false);
      hasNext();
   }

   public EmbeddingIterator() {
      this.rlock = new ReentrantLock();
      this.prefix = IntArrayListPool.instance().createObject();
   }

   public synchronized EmbeddingIterator<E> set(Computation<E> computation,
         E embedding, IntCollection wordIds) {
      this.computation = computation;
      this.prefix.clear();
      this.prefix.addAll(embedding.getWords());
      this.embedding = embedding;
      this.lastHasNext = false;
      this.currElem = -1;
      this.wordIds = wordIds;
      this.cur = wordIds.cursor();
      this.shouldRemoveLastWord = false;
      this.active = new AtomicBoolean(true);
      return this;
   }

   public synchronized EmbeddingIterator<E> forkConsumer() {
      // create new consumer
      EmbeddingIterator<E> iter = new EmbeddingIterator<E>();
      iter.parent = this;
      iter.rlock = this.rlock;
      iter.computation = this.computation;
      iter.prefix = IntArrayListPool.instance().createObject();
      iter.prefix.addAll(this.prefix);

      //iter.embedding = this.embedding;
      iter.embedding = computation.getConfig().createEmbedding();
      if (prefix.size() > 0) {
         iter.embedding.addWord(prefix.getUnchecked(0));
      }
      for (int i = 1; i < prefix.size(); ++i) {
         iter.embedding.nextExtensionLevel(this.embedding);
         iter.embedding.addWord(prefix.getUnchecked(i));
      }

      iter.lastHasNext = false;
      iter.cur = this.cur;
      iter.wordIds = this.wordIds;
      iter.shouldRemoveLastWord = false;
      iter.active = this.active;
      return iter;
   }

   public synchronized void joinConsumer() {
      IntArrayListPool.instance().reclaimObject(prefix);
   }

   private void maybeRemoveLastWord() {
      if (shouldRemoveLastWord) {
         embedding.removeLastWord();
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
            // skip extensions that turn the embedding not canonical
            while (cur.moveNext()) {
               currElem = cur.elem();
               cur.remove();
               if (computation.filter(embedding, currElem)) {
                  lastHasNext = true;
                  return true;
               }
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
   public synchronized E next() {
      shouldRemoveLastWord = true;
      embedding.addWord(nextElem());
      return embedding;
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

   public Embedding getEmbedding() {
      return embedding;
   }

   public IntCollection getWordIds() {
      return wordIds;
   }

   @Override
   public String toString() {
      return "EmbeddingIterator(" +
         "active=" + isActive() +
         ",prefix=" + prefix + ")";
   }
}

