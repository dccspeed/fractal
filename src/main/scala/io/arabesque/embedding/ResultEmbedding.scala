package io.arabesque.embedding

import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.graph.BasicMainGraph

import org.apache.hadoop.io.Writable

/**
 *
 */
trait ResultEmbedding[T] extends Writable {
  def words: Array[T]
  def combinations(k: Int): Iterator[ResultEmbedding[T]]
  def toInternalEmbedding[E <: Embedding](config: SparkConfiguration[E]): E
  
  override def hashCode(): Int = {
    words.toSet.hashCode()
  }

  override def equals(_other: Any): Boolean = {
    if (_other == null || getClass != _other.getClass) return false
    return equals (_other.asInstanceOf[ResultEmbedding[T]])
  }

  def equals(other: ResultEmbedding[T]): Boolean = {
    if (other == null) return false

    if (this.words.length != other.words.length) return false
    return this.words.toSet == other.words.toSet
  }

}

object ResultEmbedding {

  def apply (strEmbedding: String) = {
    if (strEmbedding contains "-")
      EEmbedding (strEmbedding)
    else
      VEmbedding (strEmbedding)
  }

  def apply(embedding: Embedding, config: Configuration[_]) = {
    if (embedding.isInstanceOf[EdgeInducedEmbedding]) {
      val mainGraph = config.getMainGraph[BasicMainGraph[_,_]]
      val edges = new Array [(Int,Int)] (embedding.getNumEdges)
      val edgesIter = embedding.getEdges.iterator
      var i = 0
      while (edgesIter.hasNext) {
        val e = mainGraph.getEdge(edgesIter.next)
        edges(i) = (e.getSourceId, e.getDestinationId)
        i += 1
      }
      new EEmbedding (edges)
    } else if (embedding.isInstanceOf[VertexInducedEmbedding]) {
      new VEmbedding (embedding.getVertices.toIntArray)
    } else {
      throw new RuntimeException(s"Unknown embedding type: ${embedding}")
    }
  }
}
