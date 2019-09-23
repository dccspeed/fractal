package br.ufmg.cs.systems.fractal.subgraph

import java.io.{DataInput, DataOutput}

import br.ufmg.cs.systems.fractal.conf.SparkConfiguration
import br.ufmg.cs.systems.fractal.graph.BasicMainGraph

/** An edge induced subgraph
  *
  * @param words integer array indicating the subgraph edges
  */
case class VESubgraph(var words: Array[(Int,Int)])
    extends ResultSubgraph[(Int,Int)] {

  // must have because we are messing around with Writables
  def this() = {
    this(null.asInstanceOf[Array[(Int,Int)]])
  }

  def toInternalSubgraph[E <: Subgraph](config: SparkConfiguration[E]): E = {
    val mainGraph = config.getMainGraph[BasicMainGraph[_,_]]
    val subgraph = config.createSubgraph[E]
    var i = 0
    while (i < words.length) {
      val (src, dest) = words(i)
      val edgeIdsCur = mainGraph.getVertexNeighbourhood(src).
        getEdgesWithNeighbourVertex(dest).cursor()
      while (edgeIdsCur.moveNext()) {
        subgraph.addWord(edgeIdsCur.elem())
      }
      i += 1
    }
    subgraph
  }

  def combinations(k: Int): Iterator[VESubgraph] = {
    words.combinations(k).map (new VESubgraph(_))
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
    s"VESubgraph(${words.mkString (", ")})"
  }
}

/**
  * An edge induced subgraph
  */
object VESubgraph {
  def apply (strSubgraph: String) = {
    val edgesStr = strSubgraph split "\\s+"
    val edges = new Array[(Int,Int)](edgesStr.size)
    for (i <- 0 until edges.size) {
      val words = (edgesStr(i) split "-").map (_.toInt)
      edges(i) = (words(0), words(1))
    }

    new VESubgraph (edges)
  }
}
