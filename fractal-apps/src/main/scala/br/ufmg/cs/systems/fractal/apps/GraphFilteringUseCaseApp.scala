package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph
import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import org.apache.spark.{SparkConf, SparkContext}

object GraphFilteringUseCaseApp extends Logging {
   def main(args: Array[String]): Unit = {
      // environment setup
      val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      val sc = new SparkContext(conf)
      val fc = new FractalContext(sc, "info")
      val graphPath = args(0) // input graph
      val targetNumVertices = 6

      // mico dataset
      val targetLabels = new IntArrayList()
      targetLabels.add(11)
      targetLabels.add(3)
      targetLabels.add(20)

      // without filtering
      val fg = fc.textFile(graphPath,
         graphClass = "br.ufmg.cs.systems.fractal.graph.VLabeledMainGraph")

      // with graph filtering
      val filteredFg = fg
         .filterEdges(
            (u,uLabels,v,vLabels,e,eLabels) => {
               targetLabels.contains(uLabels.getu(0)) &&
                  targetLabels.contains(vLabels.getu(0))
            })

      // force reading main graph
      val startReadingGraph1 = System.currentTimeMillis()
      fg.vfractoid.expand(1).aggregationCount
      val elapsedReadingGraph1 = System.currentTimeMillis() - startReadingGraph1

      val startReadingGraph2 = System.currentTimeMillis()
      filteredFg.vfractoid.expand(1).aggregationCount
      val elapsedReadingGraph2 = System.currentTimeMillis() - startReadingGraph2

      val labelFilter
      : (VertexInducedSubgraph, Computation[VertexInducedSubgraph]) => Boolean =
         (s,c) => {
            val graph = s.getMainGraph
            val vertices = s.getVertices
            val numVertices = vertices.size()
            var valid = true
            var i = 0
            while (valid && i < numVertices) {
               val u = vertices.getu(i)
               if (!targetLabels.contains(graph.firstVertexLabel(u))) {
                  valid = false
               }
               i += 1
            }
            valid
         }

      val start1 = System.currentTimeMillis()
      val count1 = fg.vfractoid.expand(1)
         .filter(labelFilter)
         .explore(targetNumVertices - 1)
         .aggregationCount
      val elapsed1 = System.currentTimeMillis() - start1

      val start2 = System.currentTimeMillis()
      val count2 = filteredFg.vfractoid.expand(1)
         .filter(labelFilter)
         .explore(targetNumVertices - 1)
         .aggregationCount
      val elapsed2 = System.currentTimeMillis() - start2

      logApp(s"WithoutGraphFiltering numSubgraphs=${count1}" +
         s" elapsedReadingGraph=${elapsedReadingGraph1}" +
         s" elapsedEnumeratingSubgraphs=${elapsed1}")

      logApp(s"WithGraphFiltering numSubgraphs=${count2}" +
         s" elapsedReadingGraph=${elapsedReadingGraph2}" +
         s" elapsedEnumeratingSubgraphs=${elapsed2}")

      // environment cleaning
      fc.stop()
      sc.stop()
   }
}
