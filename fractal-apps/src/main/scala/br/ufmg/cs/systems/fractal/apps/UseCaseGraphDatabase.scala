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
    while (iter.hasNext) {
      val (motif, graphs) = iter.next()
      logApp(s"motif=${motif} support=${graphs.size}")
    }

    patternsSupports.unpersist()

    //val vertexToGraphIdx = sc.textFile(vertexToGraphIdxPath)
    //   .map(_.toInt).collect()
    //val numGraphs = vertexToGraphIdx.distinct.length
    //val vertexToGraphIdxBc = sc.broadcast(vertexToGraphIdx)

    //val motifs = fgraph.efractoid.expand(numEdges)

    //val motifsCountsRDD = motifs
    //   .aggregationCanonicalPatternObj[collection.mutable.HashSet[Int]](
    //     s => s.quickPattern(),
    //     s => collection.mutable.HashSet(
    //       vertexToGraphIdxBc.value(s.getVertices.getLast)),
    //     (s1,s2) => s2.foreach(s1.add(_))
    //   )
    //   .mapValues(_.size)

    //motifsCountsRDD.cache()
    //val iter = motifsCountsRDD.collect().sortBy(_._2).iterator
    //while (iter.hasNext) {
    //  val (motif, count) = iter.next()
    //  val support = count / numGraphs.toDouble
    //  if (support >= minSupport) {
    //    logApp(s"motif=${motif} support=${support}")
    //  }
    //}

    //motifsCountsRDD.unpersist()
    
    // environment cleaning
    fc.stop()
    sc.stop()
  }
}
