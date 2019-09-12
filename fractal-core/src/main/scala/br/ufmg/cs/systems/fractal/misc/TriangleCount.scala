package br.ufmg.cs.systems.fractal.misc

import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader}

class TriangleCount(sc: SparkContext, inputFile: String, numPartitions: Int,
    inputType: String) extends Logging {

  def run {
    val start = System.currentTimeMillis
    val graph = TriangleCount.loadGraph (sc, inputFile, inputType,
      numPartitions)
    val triCounts = graph.triangleCount()
    val numTriangles = triCounts.vertices.values.sum / 3
    val elapsed = System.currentTimeMillis - start
    logInfo (s"TriangleCount numTriangles=${numTriangles} elapsed=${elapsed}")
  }
}

object TriangleCount {

  val INPUT_EDGES = "edges"
  val INPUT_ADJLISTS = "adjLists"
  val INPUT_ADJLISTS_LABEL = "adjListsLabel"
  
  def loadGraph (sc: SparkContext, inputFile: String, inputType: String,
      numPartitions: Int) = inputType match {

    case INPUT_EDGES =>
      GraphLoader.edgeListFile(sc, inputFile, false, numPartitions)

    case INPUT_ADJLISTS =>
      val edges = sc.textFile (inputFile, numPartitions).flatMap { line =>
        if (line startsWith "#") {
          Iterator.empty
        } else {
          val vertices = line split "\\s+"
          val from = vertices.head.toLong
          for (i <- 1 until vertices.size)
            yield Edge (from, vertices(i).toLong, 1) 
        }
      }
      Graph.fromEdges (edges, 1)

    case INPUT_ADJLISTS_LABEL =>
      val edges = sc.textFile (inputFile, numPartitions).flatMap { line =>
        if (line startsWith "#") {
          Iterator.empty
        } else {
          val vertices = line split "\\s+"
          val from = vertices.head.toLong
          for (i <- 2 until vertices.size)
            yield Edge (from, vertices(i).toLong, 1) 
        }
      }
      Graph.fromEdges (edges, 1)

  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TriangleCount")
    val sc = new SparkContext(conf)
    sc.setLogLevel ("INFO")
    new TriangleCount (sc, args(0), args(1).toInt, args(2)).run
    sc.stop()
  }
}
