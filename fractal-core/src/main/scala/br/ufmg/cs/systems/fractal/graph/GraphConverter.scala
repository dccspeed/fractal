package br.ufmg.cs.systems.fractal.graph

import java.util.StringTokenizer

import br.ufmg.cs.systems.fractal.util.collection.{IntArrayList, ObjArrayList}
import org.apache.spark.graphx.{Graph, VertexId, Edge => GraphXEdge}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

// TODO: merge this into the FractalGraphRDD -- support for GraphX convertions
object GraphConverter {

   type GraphXType = Graph[IntArrayList, IntArrayList]

   def fromFileToGraphX(sc: SparkContext, path: String): GraphXType = {

      val verticesRDD = sc.textFile(path)
         .map{ line =>
            val lineTokenizer = new StringTokenizer(line, " ")
            val vertexTokenizer = new StringTokenizer(
               lineTokenizer.nextToken(), ",")
            val vertexId = vertexTokenizer.nextToken().toInt
            val vertexData = new IntArrayList()
            vertexData.add(vertexId)
            while (vertexTokenizer.hasMoreTokens) {
               vertexData.add(vertexTokenizer.nextToken().toInt)
            }
            (vertexId.toLong, vertexData)
         }

      val edgesRDD = sc.textFile(path)
         .flatMap{ line =>
            val lineTokenizer = new StringTokenizer(line, " ")
            val vertexTokenizer = new StringTokenizer(
               lineTokenizer.nextToken(), ",")
            val vertexId: VertexId = vertexTokenizer.nextToken().toInt
            val edges = new ObjArrayList[GraphXEdge[IntArrayList]]()
            while (lineTokenizer.hasMoreTokens) {
               val edgeTokenizer = new StringTokenizer(
                  lineTokenizer.nextToken(), ",")
               val neighborId: VertexId = edgeTokenizer.nextToken().toInt
               val edgeId: Int = edgeTokenizer.nextToken().toInt // jump edge id
               val edgeData = new IntArrayList()
               edgeData.add(neighborId.toInt)
               edgeData.add(edgeId)
               while (edgeTokenizer.hasMoreTokens) {
                  edgeData.add(edgeTokenizer.nextToken().toInt)
               }
               edges.add(GraphXEdge(vertexId, neighborId, edgeData))
            }
            edges.iterator().asScala
         }

      Graph(verticesRDD, edgesRDD)
   }

   def fromGraphXtoFile(sc: SparkContext, graph: GraphXType)
   : String => Unit = path => {

      val vertexNeighborhoods = graph.aggregateMessages
         [(IntArrayList, List[IntArrayList])](
         triplet => {
            val adj = List(triplet.attr)
            triplet.sendToSrc((triplet.srcAttr, adj))
         },
         (adj1,adj2) => { (adj1._1, List.concat(adj1._2, adj2._2)) }
      )

      val sortedVertexNeighborhoods = vertexNeighborhoods.sortBy(
         kv => (kv._2._2.size, kv._1)
      )

      val vertexRemappingBc = sc.broadcast(sortedVertexNeighborhoods.keys
         .zipWithIndex().collectAsMap())

      val edgeRemappingBc = sc.broadcast(
         sortedVertexNeighborhoods.values
            .flatMap{kv =>
               val vertexId = kv._1.get(0)
               val edges = kv._2.filter(_.get(0) > vertexId).map(_.get(1))
               edges
            }
            .zipWithIndex()
            .collectAsMap()
      )

      val strGraphRDD = sortedVertexNeighborhoods.map{kv =>
         val vertexRemapping = vertexRemappingBc.value
         val edgeRemapping = edgeRemappingBc.value

         val vattr = kv._2._1
         val eattrs = kv._2._2.sortBy(arr => vertexRemapping(arr.get(0)))
         val sbuilder = new StringBuilder
         val vcur = vattr.cursor()

         // vertex id
         vcur.moveNext()
         sbuilder.append(s"${vertexRemapping(vcur.elem())}")

         // vertex labels
         while (vcur.moveNext()) {
            sbuilder.append(s",${vcur.elem()}")
         }

         for (eattr <- eattrs) {
            val ecur = eattr.cursor()

            // neighbor id
            ecur.moveNext()
            sbuilder.append(s" ${vertexRemapping(ecur.elem())}")

            // edge id
            ecur.moveNext()
            sbuilder.append(s",${edgeRemapping(ecur.elem())}")

            // edge labels
            while (ecur.moveNext()) {
               sbuilder.append(s",${ecur.elem()}")
            }
         }

         sbuilder.toString()
      }

      // save this remapped graph into file system
      strGraphRDD.saveAsTextFile(path)

      // clear cache
      graph.unpersist()
      vertexRemappingBc.unpersist()
      edgeRemappingBc.unpersist()
   }

   def main(args: Array[String]): Unit = {
      val path = args(0)
      println(path)
      val conf = new SparkConf().setMaster("local").setAppName("gconverter")
      val sc = new SparkContext(conf)
      val graph: Graph[IntArrayList,IntArrayList] = fromFileToGraphX(sc, path)
      println(s"GraphOriginal numVertices=${graph.numVertices} " +
         s"numEdges=${graph.numEdges}")
      val filteredGraph: GraphXType = graph.filter[IntArrayList,IntArrayList](
         _ => graph, e => true, (vid, vattr) => vid != 0)
      println(s"GraphFiltered numVertices=${filteredGraph.numVertices} " +
         s"numEdges=${filteredGraph.numEdges}")
      fromGraphXtoFile(sc, filteredGraph).apply(s"${path}.converted")

      sc.stop()
   }
}
