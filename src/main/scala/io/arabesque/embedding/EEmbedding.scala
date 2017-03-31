package io.arabesque.embedding

import java.io.{DataInput, DataOutput}

/** An edge induced embedding
  *
  * Current semantics: Array(a, b, c, d) returns
  * the embedding induced by the edges (a, b) (c, d)
  *
  * @param words integer array indicating the embedding edges
  */
case class EEmbedding(var words: Array[(Int,Int)]) extends ResultEmbedding[(Int,Int)] {

  // must have because we are messing around with Writables
  def this() = {
    this(null.asInstanceOf[Array[(Int,Int)]])
  }

  def combinations(k: Int): Iterator[EEmbedding] = {
    words.combinations(k).map (new EEmbedding(_))
  }

  override def write(out: DataOutput): Unit = {
    out.writeInt (words.size)
    words.foreach {w => out.writeInt(w._1); out.writeInt(w._2)}
  }

  override def readFields(in: DataInput): Unit = {
    val wordsLen = in.readInt
    words = new Array [(Int,Int)] (wordsLen)
    for (i <- 0 until wordsLen) words(i) = (in.readInt, in.readInt)
  }

  override def toString = {
    s"EEmbedding(${words.mkString (", ")})"
  }
}

/**
  * An edge induced embedding
  */
object EEmbedding {
  def apply (strEmbedding: String) = {
    val edgesStr = strEmbedding split "\\s+"
    val edges = new Array[(Int,Int)](edgesStr.size)
    for (i <- 0 until edges.size) {
      val words = (edgesStr(i) split "-").map (_.toInt)
      edges(i) = (words(0), words(1))
    }

    new EEmbedding (edges)
  }
}
