package br.ufmg.cs.systems.fractal.graph

import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * TODO: Graph format has changed, fix this
 *
 * Raw format is a collection of strings with the following format:
 *    first line is "numVertices numEdges"
 *    other lines represent adjancecy lists:
 *       vertexData edge1Data edge2Data ...
 *          vertexData := vertexLabel1,vertexLabel2,...
 *          edgeData := neighborId,edgeId,edgeLabel1,edgeLabel2,...
 *
 * In-place format is a collection of int arrays with the following format:
 *    [vertexId,numEdges,vertexDataPtx,edge1DataPtx,edge2DataPtx,...,
 *    vertexData1,vertexData2,...,edge1Data1,edge1Data2,...,edge2Data1,...]
 *
 *    Vertex data contains vertexLabel1,vertexLabel2,...
 *    Edge data contains neighborId,edgeId,edgeLabel1,edgeLabel2,...
 *
 * @param numVertices
 * @param numEdges
 * @param adjListsRDD
 */
class FractalGraphRDD(val numVertices: Int, val numEdges: Int,
                      val adjListsRDD: RDD[IntArrayList]) extends Logging {
   import FractalGraphRDD._

   private def sc: SparkContext = adjListsRDD.sparkContext

   /**
    * Make this RDD ready for output writing in Fractal graph format
    * @return RDD of strings representing each line of the graph file
    */
   private def toRawGraphRDD: RDD[String] = {
      val _numVertices = numVertices
      val _numEdges = numEdges

      val rawGraphRDD = adjListsRDD

         // map serialized format to string format
         .map(adjListArray => {
            getLineFromAdjListInPlace(adjListArray)
         })

         // add number of vertices and edges as first line
         .mapPartitionsWithIndex((idx, iter) => {
            if (idx == 0) Iterator(s"${_numVertices} ${_numEdges}") ++ iter
            else iter
         })

      rawGraphRDD
   }

   /**
    * Change the ordering of this graph
    * @param ordering Integer array representing the ordering
    * @return new fractal graph with vertices AND edges reordered
    */
   def applyOrdering(ordering: IntArrayList): FractalGraphRDD = {
      // sanity check to see if the ordering is valid for this graph
      if (ordering.size() != numVertices) {
         throw new RuntimeException(s"Ordering must have ${numVertices} items.")
      } else {
         var i = 0
         while (i < numVertices) {
            if (ordering.get(i) >= numVertices) {
               throw new RuntimeException(s"Ordering must not have items " +
                  s" >=  ${numVertices}")
            }
            i += 1
         }
      }

      // 1. remap only vertices and reorder adjlists
      val orderingBc = sc.broadcast(ordering)
      val sortedAdjListsRDD = adjListsRDD
            .map(adjListArray => remapAdjList(adjListArray, orderingBc.value))
            .sortBy(adjListArray => adjListArray.get(0))
            .persist(StorageLevel.DISK_ONLY)

      // 2. number of upper edges per partition: edges (u,v); u < v
      val numNeighborsAbove = sortedAdjListsRDD
         .mapPartitions(iter => {
            var numNeighborsBelowVertexId = 0
            var numEdgesTotal = 0
            while (iter.hasNext) {
               val adjListArray = iter.next()
               val vertexId = getVertexIdInPlace(adjListArray)
               val numEdges = getNumEdgesInPlace(adjListArray)
               numEdgesTotal += numEdges
               var i = 0
               while (i < numEdges) {
                  val neighborId = getNeighborIdInPlace(adjListArray, i)
                  if (neighborId < vertexId) numNeighborsBelowVertexId += 1
                  i += 1
               }
            }
            Iterator(numEdgesTotal - numNeighborsBelowVertexId)
         })
         .collect()

      // cumulative number of upper edges per adjacency list, we need this to
      // remap edge ids to contiguous and in order space
      var i = 1
      while (i < numNeighborsAbove.length) {
         numNeighborsAbove(i) += numNeighborsAbove(i - 1)
         i += 1
      }

      // 3. remap upper edges
      val numNeighborsAboveBc = sc.broadcast(numNeighborsAbove)
      val semiRemappedEdgesRDD = sortedAdjListsRDD
         .mapPartitionsWithIndex((idx, iter) => {
            var nextEdgeId = if (idx == 0) 0 else numNeighborsAboveBc.value(idx - 1)
            iter.map(adjListArray => {
               val vertexId = getVertexIdInPlace(adjListArray)
               val numEdges = getNumEdgesInPlace(adjListArray)
               var i = 0
               while (i < numEdges) {
                  val start = getEdgeDataStartInPlace(adjListArray, i)
                  val neibhborId = adjListArray.get(start)
                  if (neibhborId > vertexId) {
                     adjListArray.set(start + 1, nextEdgeId)
                     nextEdgeId += 1
                  }
                  i += 1
               }
               adjListArray
            })
         })

      // 4. remap edges below
      val vertexIdBounds = semiRemappedEdgesRDD
         .mapPartitions(iter => {
            var minVertexId = Integer.MAX_VALUE
            var maxVertexId = Integer.MIN_VALUE
            while (iter.hasNext) {
               val adjListArray = iter.next()
               val vertexId = getVertexIdInPlace(adjListArray)
               minVertexId = Math.min(minVertexId, vertexId)
               maxVertexId = Math.max(maxVertexId, vertexId)
            }
            Iterator((minVertexId, maxVertexId))
         })
         .collect()

      // graph partitioner make sure lower edges are sent to the right place
      val vertexIdBoundsBc = sc.broadcast(vertexIdBounds)
      val rangePartitionerAdjLists = new GraphPartitioner(vertexIdBoundsBc)

      val rightJoinAdjListRDD = semiRemappedEdgesRDD
         .mapPartitions(iter => {
            iter.flatMap(adjListArray => {
               val vertexId = getVertexIdInPlace(adjListArray)
               val numEdges = getNumEdgesInPlace(adjListArray)
               var tuples = List.empty[(Int,(Int,Int))]
               var i = 0
               while (i < numEdges) {
                  val neighborId = getNeighborIdInPlace(adjListArray, i)
                  if (neighborId > vertexId) {
                     val edgeId = getEdgeIdInPlace(adjListArray, i)
                     tuples = (neighborId, (vertexId, edgeId)) :: tuples
                  }
                  i += 1
               }
               tuples.iterator
            })
         })
         .partitionBy(rangePartitionerAdjLists)

      // final remap: fix edge ids of edges (u,v); u > v
      // we zip partitions because the second RDD is already partitioned
      // according to the same ranges (adjacency list ranges)
      val joinedAdjListsRDD = semiRemappedEdgesRDD
         .zipPartitions(rightJoinAdjListRDD, preservesPartitioning = true)(
            (iter1, iter2) => {
               val map = iter2.map(kv => ((kv._1, kv._2._1), kv._2._2)).toMap
               iter1.map(adjListArray => {
                  val vertexId = getVertexIdInPlace(adjListArray)
                  val numEdges = getNumEdgesInPlace(adjListArray)
                  var i = 0
                  while (i < numEdges) {
                     val start = getEdgeDataStartInPlace(adjListArray, i)
                     val neighborId = adjListArray.get(start)
                     if (vertexId > neighborId) {
                        val newEdgeId = map((vertexId, neighborId))
                        adjListArray.set(start + 1, newEdgeId)
                     }
                     i += 1
                  }
                  adjListArray
               })
            }
         )

      new FractalGraphRDD(numVertices, numEdges, joinedAdjListsRDD)
   }

   /**
    * Output this graph to disk using Fractal graph format
    * @param path
    */
   def saveAsTextFile(path: String): Unit = toRawGraphRDD.saveAsTextFile(path)
}

/**
 * This companion contains several auxiliary methods for accessing the graph
 * RDD using the in-place format
 */
object FractalGraphRDD {
   def apply(sc: SparkContext, path: String): FractalGraphRDD = {
      val rawGraphRDD = sc.textFile(path)
      val toks = rawGraphRDD.first().split(" ")
      val numVertices = toks(0).toInt
      val numEdges = toks(1).toInt
      val adjListsRDD: RDD[IntArrayList] = getAdjLists(rawGraphRDD)
      new FractalGraphRDD(numVertices, numEdges, adjListsRDD)
   }

   private def getAdjLists(rawGraphRDD: RDD[String]): RDD[IntArrayList] = {
      rawGraphRDD
         .mapPartitionsWithIndex((idx, iter) => {
            if (idx == 0) iter.drop(1) else iter
         })
         .zipWithIndex()
         .map(kv => {
            val vertexId = kv._2.toInt
            val line = kv._1
            getAdjListInPlace(vertexId, line)
         })
   }

   private def getAdjListInPlace(vertexId: Int, line: String): IntArrayList = {
      val adjListArray = new IntArrayList()
      val lineToks = line.split(" ")
      val vertexDataToks = lineToks(0).split(",")
      val numEdges = lineToks.length - 1

      val vertexDataPtxIdx = 2

      adjListArray.add(vertexId)
      adjListArray.add(numEdges)
      adjListArray.add(-1) // placeholder for vertexDataPtx

      var i = 0
      while (i < numEdges) {
         adjListArray.add(-1) // placeholder for edgeDataPtx
         i += 1
      }

      val extraEdgeDataPtxIdx = adjListArray.size()

      // add extra edgeDataPtx for the last edge
      adjListArray.add(-1)

      // set vertexDataPtx
      adjListArray.set(vertexDataPtxIdx, adjListArray.size())

      // add vertexData
      i = 0
      while (i < vertexDataToks.length) {
         adjListArray.add(vertexDataToks(i).toInt)
         i += 1
      }

      var edgeDataPtxIdx = 3
      i = 0
      while (i < numEdges) {
         val edgeDataPtx = lineToks(i + 1).split(",")

         // add new edgeDataPtx
         adjListArray.set(edgeDataPtxIdx, adjListArray.size())

         // add edgeData
         var j = 0
         while (j < edgeDataPtx.length) {
            adjListArray.add(edgeDataPtx(j).toInt)
            j += 1
         }
         edgeDataPtxIdx += 1
         i += 1
      }

      adjListArray.set(extraEdgeDataPtxIdx, adjListArray.size())

      adjListArray
   }

   private def getLineFromAdjListInPlace(adjListArray: IntArrayList): String = {
      val sb = new StringBuilder

      // vertex data
      {
         var start = getVertexDataStartInPlace(adjListArray)
         val end = getVertexDataEndInPlace(adjListArray)
         sb.append(adjListArray.get(start))
         start += 1
         while (start < end) {
            sb.append(",").append(adjListArray.get(start))
            start += 1
         }
      }

      // edges
      {
         val numEdges = getNumEdgesInPlace(adjListArray)
         var i = 0
         while (i < numEdges) {
            var start = getEdgeDataStartInPlace(adjListArray, i)
            val end = getEdgeDataEndInPlace(adjListArray, i)
            sb.append(" ").append(adjListArray.get(start))
            start += 1
            while (start < end) {
               sb.append(",").append(adjListArray.get(start))
               start += 1
            }
            i += 1
         }
      }

      sb.toString()
   }

   private def getVertexIdInPlace(adjListArray: IntArrayList): Int = {
      adjListArray.get(0)
   }

   private def getNumEdgesInPlace(adjListArray: IntArrayList): Int = {
      adjListArray.get(1)
   }

   private def getVertexDataStartInPlace(adjListArray: IntArrayList): Int = {
      adjListArray.get(2)
   }

   private def getVertexDataEndInPlace(adjListArray: IntArrayList): Int = {
      adjListArray.get(3)
   }

   private def getEdgeDataStartInPlace(adjListArray: IntArrayList,
                                       edgeIdx: Int): Int = {
      adjListArray.get(3 + edgeIdx)
   }

   private def getEdgeDataEndInPlace(adjListArray: IntArrayList,
                                     edgeIdx: Int): Int = {
      adjListArray.get(3 + edgeIdx + 1)
   }

   private def getNeighborIdInPlace(adjListArray: IntArrayList,
                                    edgeIdx: Int): Int = {
      adjListArray.get(getEdgeDataStartInPlace(adjListArray, edgeIdx))
   }

   private def getEdgeIdInPlace(adjListArray: IntArrayList,
                                edgeIdx: Int): Int = {
      adjListArray.get(getEdgeDataStartInPlace(adjListArray, edgeIdx) + 1)
   }

   private def remapAdjList(adjListArray: IntArrayList,
                            ordering: IntArrayList): IntArrayList = {
      val numEdges = getNumEdgesInPlace(adjListArray)

      val oldVertexId = getVertexIdInPlace(adjListArray)
      val newVertexId = ordering.get(oldVertexId)

      // get new order
      var i = 0
      var numUpperNeighbors = 0
      val indices = new Array[Int](numEdges)
      val neighbors = new Array[Int](numEdges)
      while (i < numEdges) {
         val oldNeighborId = getNeighborIdInPlace(adjListArray, i)
         val newNeighborId = ordering.get(oldNeighborId)
         if (newNeighborId > newVertexId) numUpperNeighbors += 1
         indices(i) = i
         neighbors(i) = newNeighborId
         i += 1
      }

      // insertion sort
      i = 1
      while (i < numEdges) {
         val v = indices(i)
         val c = neighbors(v)
         var j = i - 1
         while (j >= 0 && neighbors(indices(j)) > c) {
            indices(j + 1) = indices(j)
            j -= 1
         }
         indices(j + 1) = v
         i += 1
      }

      // fill new adj list
      val newAdjListArray = new IntArrayList()
      newAdjListArray.add(newVertexId)
      newAdjListArray.add(numEdges)
      newAdjListArray.add(-1) // vertex data ptx

      // edge data ptx
      i = 0
      while (i < numEdges) {
         newAdjListArray.add(-1)
         i += 1
      }

      // extra edge ptx placeholder
      newAdjListArray.add(-1)

      // add vertex data
      newAdjListArray.set(2, newAdjListArray.size())
      var start = getVertexDataStartInPlace(adjListArray)
      var end = getVertexDataEndInPlace(adjListArray)
      while (start < end) {
         newAdjListArray.add(adjListArray.get(start))
         start += 1
      }

      i = 0
      var edgePtx = 3
      while (i < numEdges) {
         val j = indices(i)
         // add edge data
         newAdjListArray.set(edgePtx, newAdjListArray.size())
         start = getEdgeDataStartInPlace(adjListArray, j)
         end = getEdgeDataEndInPlace(adjListArray, j)
         val oldNeighborId = adjListArray.get(start)
         val newNeighborId = ordering.get(oldNeighborId)
         newAdjListArray.add(newNeighborId)
         start += 1
         while (start < end) {
            newAdjListArray.add(adjListArray.get(start))
            start += 1
         }

         edgePtx += 1
         i += 1
      }

      // set extra edge ptx
      newAdjListArray.set(edgePtx, newAdjListArray.size())

      newAdjListArray
   }

   def main(args: Array[String]): Unit = {
      val graphPath = args(0)
      println(graphPath)
      val conf = new SparkConf().setMaster("local").setAppName("FractalGraph")
      val sc = new SparkContext(conf)
      val graph = FractalGraphRDD(sc, graphPath)

      var adjList = graph.adjListsRDD.first()
      println(s"${graph.numVertices} ${graph.numEdges}")
      println(s"${adjList}" +
         s" vertexId=${getVertexIdInPlace(adjList)}" +
         s" numEdges=${getNumEdgesInPlace(adjList)}" +
         s" vertexDataStart=${getVertexDataStartInPlace(adjList)}" +
         s" vertexDataEnd=${getVertexDataEndInPlace(adjList)}" +
         s" edgeDataStart=${getEdgeDataStartInPlace(adjList, 0)}" +
         s" edgeDataEnd=${getEdgeDataEndInPlace(adjList, 0)}" +
         s" neighborId=${getNeighborIdInPlace(adjList, 0)}" +
         s" edgeId=${getEdgeIdInPlace(adjList, 0)}")

      var i = graph.numVertices - 1
      val ordering = new IntArrayList()
      while (i >= 0) {
         ordering.add(i)
         i -= 1
      }

      val outputPath = "file:///tmp/mico.sc"

      val newGraph = graph.applyOrdering(ordering)

      newGraph.saveAsTextFile(outputPath)


      //graph.saveAsTextFile(outputPath)

      //val graph2 = new FractalGraphRDD(sc, outputPath)

      //adjList = graph2.adjListsRDD.first()
      //println(s"${graph.numVertices} ${graph.numEdges}")
      //println(s"${adjList}" +
      //   s" vertexId=${getVertexIdInPlace(adjList)}" +
      //   s" numEdges=${getNumEdgesInPlace(adjList)}" +
      //   s" vertexDataStart=${getVertexDataStartInPlace(adjList)}" +
      //   s" vertexDataEnd=${getVertexDataEndInPlace(adjList)}" +
      //   s" edgeDataStart=${getEdgeDataStartInPlace(adjList, 0)}" +
      //   s" edgeDataEnd=${getEdgeDataEndInPlace(adjList, 0)}" +
      //   s" neighborId=${getNeighborIdInPlace(adjList, 0)}" +
      //   s" edgeId=${getEdgeIdInPlace(adjList, 0)}")


      //val fc = new FractalContext(sc, logLevel = "info")
      //val fg = fc.textFile(outputPath, local = true)

      //val motifs = fg.motifsSF(3)

      //val motifsArray = motifs.collect()

      //println(s"${motifsArray.mkString("\n")}")

      //fc.stop()
      sc.stop()
   }
}

/**
 * Splits adjacency lists by vertex range
 * @param vertexIdBoundsBc broadcast containing partition ranges, in order
 */
private class GraphPartitioner(val vertexIdBoundsBc: Broadcast[Array[(Int,Int)]])
   extends Partitioner {

   override val numPartitions: Int = vertexIdBoundsBc.value.size

   override def getPartition(key: Any): Int = {
      val keyAsInt = key.hashCode()
      find(keyAsInt, 0, numPartitions)
   }

   private def vertexIdBounds: Array[(Int, Int)] = vertexIdBoundsBc.value

   private def find(key: Int, from: Int, to: Int): Int = {
      val idx = (from + to) / 2
      val (low, high) = vertexIdBounds(idx)
      if (key >= low && key <= high) return idx
      if (key < low) find(key, from, idx)
      else find(key, idx + 1, to)
   }
}
