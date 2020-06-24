package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.graph.MainGraph
import br.ufmg.cs.systems.fractal.util.{Logging, PairWritable}
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import org.apache.hadoop.io.IntWritable
import org.apache.spark.{SparkConf, SparkContext}

object ShortestPathApp extends Logging {
  def main(args: Array[String]): Unit = {
    // environment setup
    val conf = new SparkConf().setAppName("MyMotifsApp")
    val sc = new SparkContext(conf)
    val fc = new FractalContext(sc)
    val graphPath = args(0) // input graph
    val fgraph = fc.textFile(graphPath)

    val SP_AGG = "sps"

    val bootstrap = fgraph.efractoid.
      expand(1).
      aggregate[PairWritable[IntWritable, IntWritable], IntArrayList]("sps",
        (subg, comp, value) => {
          val v = subg.getVertices; val k = subg.getNumVertices; val n1 = new IntWritable(v.get(0)); val n2 = new IntWritable(v.get(k - 1)); new PairWritable(n1, n2)
        },
        (subg, comp, value) => {
          subg.getVertices
        },
        (value1, value2) => {
          value1
        },
        isIncremental = true
      )

    var paths = bootstrap
    var spaths = paths.aggregationMap[PairWritable[IntWritable, IntWritable], IntArrayList]("sps")

    var remainingSteps = 3
    var continue = true
    var iteration = 0

    while (continue) {
      val previous_size = spaths.size
      logInfo(s"Path iteration=${iteration} ${previous_size}")
      spaths.foreach { case (pair, path) =>
        logInfo(s"Path iteration=${iteration} ${pair} ${path}")
      }

      paths = paths.
        expand(1).
        filter { (subg, c) => {
          val v = subg.getVertices;
          val k = subg.getNumVertices;
          val numEdges = subg.getNumEdges;
          val edges = subg.getEdges;
          val newEdge = c.getConfig().getMainGraph[MainGraph[_, _]]().getEdge(edges.get(numEdges - 1));

          val b = newEdge.getSourceId == v.get(0) || newEdge.getSourceId == v.get(k - 2) || newEdge.getDestinationId == v.get(0) || newEdge.getDestinationId == v.get(k - 2);
          subg.numVerticesAdded == 1 && b
        }
        }.
        filter[PairWritable[IntWritable, IntWritable], IntArrayList]("sps") {
          (subg, agg) => !agg.containsKey({
            val v = subg.getVertices; val k = v.size; val n1 = new IntWritable(v.get(0)); val n2 = new IntWritable(v.get(k - 1)); new PairWritable(n1, n2)
          })
        }.
        aggregate[PairWritable[IntWritable, IntWritable], IntArrayList]("sps",
          (subg, comp, value) => {
            val v = subg.getVertices; val k = v.size; val n1 = new IntWritable(v.get(0)); val n2 = new IntWritable(v.get(k - 1)); new PairWritable(n1, n2)
          },
          (subg, comp, value) => {
            subg.getVertices
          },
          (value1, value2) => {
            value1
          },
          isIncremental = true)

      spaths = paths.aggregationMap[PairWritable[IntWritable, IntWritable], IntArrayList]("sps")
      val new_size = spaths.size

      iteration += 1
      remainingSteps -= 1
      continue = previous_size < new_size && remainingSteps > 0


    }

    paths
    // environment cleaning
    fc.stop()
  }
}
