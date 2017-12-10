package io.arabesque.utils.collection

import com.koloboke.collect.{IntCollection, IntCursor}

import io.arabesque.computation.Computation
import io.arabesque.embedding.Embedding

class EmbeddingIterator [E <: Embedding] (
    _computation: Computation[E]) extends Iterator[E] with Serializable {

  private def computation: Computation[E] = _computation

  private var embedding: E = _
  
  private var cur: IntCursor = _

  private var dirty: Boolean = _

  def set(embedding: E, wordIds: IntCollection): Iterator[E] = {
    this.embedding = embedding
    this.cur = wordIds.cursor()
    this.dirty = false
    this
  }

  //private def firstHasNext(): Boolean = {
  //  embedding.removeLastWord()
  //  currentHasNext = remainingHasNexts _
  //  remainingHasNexts()
  //}

  //private def remainingHasNexts(): Boolean = {
  //  while (cur.moveNext()) {
  //    if (computation.filter(embedding, cur.elem()))
  //      return true
  //  }
  //  false
  //}

  //private var currentHasNext: () => Boolean = firstHasNext _

  //override def hasNext(): Boolean = currentHasNext()

  override def hasNext(): Boolean = {
    while (cur.moveNext()) {
      if (computation.filter (embedding, cur.elem()))
        return true
    }
    false
  }
  
  override def next(): E = {
    if (dirty) {
      embedding.removeLastWord()
    }
    embedding.addWord(cur.elem())
    dirty = true
    embedding
  }
}


