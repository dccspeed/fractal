package io.arabesque.embedding

import io.arabesque.conf.SparkConfiguration

import java.io.{DataInput, DataOutput}

/**
  * A vertex induced embedding
  *
  * Current semantics: Array(a, b, c, d) returns the
  * embedding induced by the vertices {a, b, c, d}
  *
  * @param words integer array indicating the embedding vertices
  */
case class VEmbedding(var words: Array[Int]) extends ResultEmbedding[Int] {

  // must have because we are messing around with Writables
  def this() = {
    this(null)
  }
  
  def toInternalEmbedding[E <: Embedding](config: SparkConfiguration[E]): E = {
    val embedding = config.createEmbedding[E]
    var i = 0
    while (i < words.length) {
      embedding.addWord(words(i))
      i += 1
    }
    embedding
  }

  def combinations(k: Int): Iterator[VEmbedding] = {
    words.combinations(k).map (new VEmbedding(_))
  }

  override def write(out: DataOutput): Unit = {
    out.writeInt (words.size)
    words.foreach (w => out.writeInt(w))
  }

  override def readFields(in: DataInput): Unit = {
    val wordsLen = in.readInt
    words = new Array[Int](wordsLen)
    for (i <- 0 until wordsLen) words(i) = in.readInt
  }

  override def toString = {
    s"VEmbedding(${words.mkString (", ")})"
  }

}

/**
  * A vertex induced embedding
  */
object VEmbedding {
  def apply (strEmbedding: String) = {
    val vertices = (strEmbedding split "\\s+").
      map (_.toInt)
    new VEmbedding (vertices)
  }
}
