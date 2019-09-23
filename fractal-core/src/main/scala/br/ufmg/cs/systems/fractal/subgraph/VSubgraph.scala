package br.ufmg.cs.systems.fractal.subgraph

import java.io.{DataInput, DataOutput}

import br.ufmg.cs.systems.fractal.conf.SparkConfiguration

/**
  * A vertex induced subgraph
  *
  * Current semantics: Array(a, b, c, d) returns the
  * subgraph induced by the vertices {a, b, c, d}
  *
  * @param words integer array indicating the subgraph vertices
  */
case class VSubgraph(var words: Array[Int]) extends ResultSubgraph[Int] {

  // must have because we are messing around with Writables
  def this() = {
    this(null)
  }
  
  def toInternalSubgraph[E <: Subgraph](config: SparkConfiguration[E]): E = {
    val subgraph = config.createSubgraph[E]
    var i = 0
    while (i < words.length) {
      subgraph.addWord(words(i))
      i += 1
    }
    subgraph
  }

  def combinations(k: Int): Iterator[VSubgraph] = {
    words.combinations(k).map (new VSubgraph(_))
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
    s"VSubgraph(${words.mkString (", ")})"
  }

}

/**
  * A vertex induced subgraph
  */
object VSubgraph {
  def apply (strSubgraph: String) = {
    val vertices = (strSubgraph split "\\s+").
      map (_.toInt)
    new VSubgraph (vertices)
  }
}
