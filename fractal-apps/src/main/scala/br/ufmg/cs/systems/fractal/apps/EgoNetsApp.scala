package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.computation.EgoNetEnumeratorVertexInduced
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.{SparkConf, SparkContext}

object EgoNetsApp extends Logging {
  def main(args: Array[String]): Unit = {
    // environment setup
    val conf = new SparkConf().setAppName("EgoNetsApp")
    val sc = new SparkContext(conf)
    val fc = new FractalContext(sc, logLevel = "INFO")
    val graphPath = args(0) // input graph
    val k = args(1).toInt // number of hops
    val fgraph = fc.unlabeledGraphFromAdjLists(graphPath)

    val frac = fgraph
      //.set("ws_internal", false)
      .set("ws_external", false)
      .vfractoid
      .extend(k, classOf[EgoNetEnumeratorVertexInduced])

    //val subgraphs = frac.subgraphs().collect()

    //for (subgraph <- subgraphs) {
    //  logApp(subgraph.toString)
    //}

    logApp(s"NumEgoNets: ${frac.aggregationCount}")

    // environment cleaning
    fc.stop()
    sc.stop()
  }
}
