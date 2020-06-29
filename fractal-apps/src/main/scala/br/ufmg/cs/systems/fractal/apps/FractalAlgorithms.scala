package br.ufmg.cs.systems.fractal.mpmg

import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.graph.MainGraph
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import br.ufmg.cs.systems.fractal.util.{Logging, PairWritable}
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import scala.collection.mutable.Map

class FractalAlgorithms extends Logging {
   
  def mapvertices(fgraph: FractalGraph) : Map[IntWritable, Text] = {
    fgraph.vfractoid.
      expand(1).
      aggregate[IntWritable, Text]("mapv",
	     (subg, comp, value) => {
            val v = subg.getVertices;
            val n = new IntWritable(v.get(0));
            n
          },
          (subg, comp, value) => {
            val v = subg.getVertices;
	    val vertex = v.get(0)
            new Text(comp.getConfig().getMainGraph[MainGraph[_, _]]().getVertex(vertex).getVertexOriginalId)
          },
          (value1, value2) => {
            value1
          }).aggregationMap[IntWritable, Text]("mapv")
  }

  /**
   * All-cliques listing implementing the efficient DAG structure from
   * [[https://dl.acm.org/citation.cfm?id=3186125]]
   *
   * @param cliqueSize
   * @return Fractoid with the initial state for cliques
   */
  def cliques(fgraph: FractalGraph, cliqueSize: Int): Fractoid[VertexInducedSubgraph] = {
    fgraph.vfractoid.
      expand(1).
      set("subgraph_enumerator",
        "br.ufmg.cs.systems.fractal.gmlib.clique.KClistEnumerator")
  }

  /**
   * Shortest paths (SPs) listing implemented with aggregations
   *
   * @param numSteps maximum number of exploration steps
   * @return Fractoid with the initial state for the SPs
   */
  def spaths(fgraph: FractalGraph, numSteps: Int): Fractoid[EdgeInducedSubgraph] = {
    val SP_AGG = "sps"

    val bootstrap = fgraph.efractoid.
      expand(1).
      aggregate[PairWritable[IntWritable, IntWritable], IntArrayList](SP_AGG,
        (subg, comp, value) => {
          val v = subg.getVertices;
          val k = subg.getNumVertices;
          val n1 = new IntWritable(v.get(0));
          val n2 = new IntWritable(v.get(k - 1));
          new PairWritable(n1, n2)
        },
        (subg, comp, value) => {
          subg.getVertices
        },
        (value1, value2) => {
          value1
        },
        isIncremental = true
      )

    var fpaths = bootstrap
    var spaths = fpaths.aggregationMap[PairWritable[IntWritable, IntWritable], IntArrayList](SP_AGG)

    var remainingSteps = 3
    var continue = true
    var iteration = 0

    while (continue) {
      val previous_size = spaths.size
      logInfo(s"Path iteration=${iteration} ${previous_size}")
      //spaths.foreach { case (pair, path) =>
      //  logInfo(s"Path iteration=${iteration} ${pair} ${path}")
      //}

      fpaths = fpaths.
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
        filter[PairWritable[IntWritable, IntWritable], IntArrayList](SP_AGG) {
          (subg, agg) =>
            !agg.containsKey({
              val v = subg.getVertices;
              val k = v.size;
              val n1 = new IntWritable(v.get(0));
              val n2 = new IntWritable(v.get(k - 1));
              new PairWritable(n1, n2)
            })
        }.
        aggregate[PairWritable[IntWritable, IntWritable], IntArrayList](SP_AGG,
          (subg, comp, value) => {
            val v = subg.getVertices;
            val k = v.size;
            val n1 = new IntWritable(v.get(0));
            val n2 = new IntWritable(v.get(k - 1));
            new PairWritable(n1, n2)
          },
          (subg, comp, value) => {
            subg.getVertices
          },
          (value1, value2) => {
            value1
          },
          isIncremental = true)

      spaths = fpaths.aggregationMap[PairWritable[IntWritable, IntWritable], IntArrayList](SP_AGG)
      val new_size = spaths.size

      iteration += 1
      remainingSteps -= 1
      continue = previous_size < new_size && remainingSteps > 0

    }
    fpaths
  }
}
