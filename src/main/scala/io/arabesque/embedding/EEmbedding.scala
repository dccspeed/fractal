package io.arabesque.embedding

import io.arabesque.conf.SparkConfiguration
import io.arabesque.graph.BasicMainGraph

import java.io.{DataInput, DataOutput}

/** An edge induced embedding
  *
  * @param words integer array indicating the embedding edges
  */
case class EEmbedding(var words: Array[(Int,Int)])
    extends ResultEmbedding[(Int,Int)] {

  // must have because we are messing around with Writables
  def this() = {
    this(null.asInstanceOf[Array[(Int,Int)]])
  }

  def toInternalEmbedding[E <: Embedding](config: SparkConfiguration[E]): E = {
    val mainGraph = config.getMainGraph[BasicMainGraph]
    val embedding = config.createEmbedding[E]
    var i = 0
    while (i < words.length) {
      val (src, dest) = words(i)
      val edgeIdsCur = mainGraph.getVertexNeighbourhood(src).
        getEdgesWithNeighbourVertex(dest).cursor()
      while (edgeIdsCur.moveNext()) {
        embedding.addWord(edgeIdsCur.elem())
      }
    }
    embedding
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
