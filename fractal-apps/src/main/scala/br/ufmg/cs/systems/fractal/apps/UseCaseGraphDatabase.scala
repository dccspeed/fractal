package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.{SparkConf, SparkContext}

import java.util

object UseCaseGraphDatabase extends Logging {
  def main(args: Array[String]): Unit = {
    // environment setup
    val conf = new SparkConf().setAppName("MyMotifsApp")
    val sc = new SparkContext(conf)
    val fc = new FractalContext(sc)
    val graphPath = args(0) // input graph
    val vertexToGraphIdxPath = s"${graphPath}/${args(1)}"
    val numEdges = args(2).toInt
    val minSupport = args(3).toDouble
    val fgraph = fc
       .textFile(graphPath,
         "br.ufmg.cs.systems.fractal.graph.VELabeledMainGraph")
       .set("edge_labeled", true)

    val patternsSupports = fgraph
       .fsmDatabasePO(vertexToGraphIdxPath, minSupport, numEdges)

    patternsSupports.cache()
    val iter = patternsSupports.collect().sortBy(_._2.size).iterator
    var numPatterns = 0L
    while (iter.hasNext) {
      val (motif, graphs) = iter.next()
      logApp(s"motif=${motif} support=${graphs.size}")
      numPatterns += 1
    }

    patternsSupports.unpersist()

    logApp(s"Number of patterns: ${numPatterns}")

    // environment cleaning
    fc.stop()
    sc.stop()
  }
}
