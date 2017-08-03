package io.arabesque.computation;

import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.utils.collection.IntArrayList;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.IntCursor;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import scala.Tuple2;

class EmbeddingIterator<E extends Embedding> implements Iterator<E> {

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

   protected ConcurrentLinkedQueue<EmbeddingIterator<E>> consumerPool;

   public String computationLabel() {
      return computation.computationLabel();
   }

   public boolean isActive() {
      return active != null && active.get();
   }

   public synchronized EmbeddingIterator<E> set(Computation<E> computation,
         E embedding, IntCollection wordIds) {
      this.rlock = new ReentrantLock();
      this.computation = computation;
      this.prefix = new IntArrayList(embedding.getNumWords());
      this.prefix.addAll(embedding.getWords());
      this.embedding = embedding;
      this.lastHasNext = false;
      this.currElem = -1;
      this.wordIds = wordIds;
      this.cur = wordIds.cursor();
      this.shouldRemoveLastWord = false;
      this.active = new AtomicBoolean(true);
      this.consumerPool = new ConcurrentLinkedQueue<EmbeddingIterator<E>>();
      return this;
   }

   public synchronized EmbeddingIterator<E> forkConsumer() {
      //EmbeddingIterator<E> iter = consumerPool.poll();
      //if (iter != null) {
      //   return iter;
      //}
      EmbeddingIterator<E> iter = new EmbeddingIterator<E>();
      iter.parent = this;
      iter.rlock = this.rlock;
      iter.computation = this.computation;
      iter.prefix = this.prefix;

      iter.embedding = computation.getConfig().createEmbedding();
      for (int i = 0; i < prefix.size(); ++i) {
         iter.embedding.addWord(prefix.getUnchecked(i));
      }

      iter.lastHasNext = false;
      iter.cur = this.cur;
      iter.shouldRemoveLastWord = false;
      iter.active = this.active;
      return iter;
   }

   public synchronized void joinConsumer() {
      assert !lastHasNext;
      this.parent.consumerPool.offer(this);
   }

   @Override
   public boolean hasNext() {
      if (lastHasNext) {
         return true;
      }

      // this test is to make sure we do not remove the last word in the
      // first *hasNext* call
      if (shouldRemoveLastWord) {
         embedding.removeLastWord();
         shouldRemoveLastWord = false;
      }

      try {
         rlock.lock();
         if (isActive()) {
            // skip extensions that turn the embedding not canonical
            while (cur.moveNext()) {
               currElem = cur.elem();
               if (computation.filter(embedding, currElem)) {
                  lastHasNext = true;
                  return true;
               }
            }
            active.set(false);
         }
         return false;
      } finally {
         rlock.unlock();
      }
   }

   @Override
   public E next() {
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

   @Override
   public String toString() {
      return "EmbeddingIterator(" +
         ",computation=" + computation +
         ",prefix=" + prefix +
         ",currElem=" + currElem +
         ",wordIds=" + wordIds +
         ",shouldRemoveLastWord=" + shouldRemoveLastWord +
         ")";
   }
}

