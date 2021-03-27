package br.ufmg.cs.systems.fractal.apps

import java.util

import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.util.{FractalSparkListener, Logging, ThreadStats}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object LoadBalancingTest extends Logging {
   def main(args: Array[String]): Unit = {
      // environment setup
      val conf = new SparkConf().setAppName("MyMotifsApp")
      val sc = new SparkContext(conf)
      val fc = new FractalContext(sc, "info")
      val graphPath = args(0) // input graph
      val numVertices = args(1).toInt // number of vertices
      val fgraph = fc.textFile(graphPath)
         .set("ws_internal", false)
         .set("ws_external", false)

      val subgraphs = fgraph.cliquesSF(numVertices)

      val partitionWordToNumSubgraphs = subgraphs
         .aggregationLongLong(
            s => s.getVertices.get(0), 0L, _ => 1L, _ + _)
         .sortByKey()
         .collect()

      var i = 0
      while (i < partitionWordToNumSubgraphs.length) {
         val (vertexId, count) = partitionWordToNumSubgraphs(i)
         logApp(s"${vertexId} ${count}")
         i += 1
      }

      // environment cleaning
      fc.stop()
      sc.stop()
   }
}
