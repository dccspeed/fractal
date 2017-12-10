package io.arabesque.embedding

import io.arabesque.conf.SparkConfiguration
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
    var result = this.words.length
    var i = 0
    while (i < this.words.length) {
      result = 31 * result + this.words(i).hashCode
      i += 1
    }
    result
  }

  override def equals(_other: Any): Boolean = {
    if (_other == null || getClass != _other.getClass) return false
    return equals (_other.asInstanceOf[ResultEmbedding[T]])
  }

  def equals(other: ResultEmbedding[T]): Boolean = {
    if (other == null) return false

    if (this.words.length != other.words.length) return false

    var i = 0
    while (i < this.words.length) {
      if (this.words(i) != other.words(i)) return false
      i += 1
    }

    return true
  }

}

object ResultEmbedding {

  def apply (strEmbedding: String) = {
    if (strEmbedding contains "-")
      EEmbedding (strEmbedding)
    else
      VEmbedding (strEmbedding)
  }

  def apply(embedding: Embedding, config: SparkConfiguration[_]) = {
    if (embedding.isInstanceOf[EdgeInducedEmbedding]) {
      val mainGraph = config.getMainGraph[BasicMainGraph]
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
