package br.ufmg.cs.systems.fractal.subgraph

import br.ufmg.cs.systems.fractal.util.Logging

import scala.collection.mutable.ArrayBuffer
import org.json4s._
import org.json4s.jackson.Serialization


case class SerializableSubgraph(vids: Array[Int], eids: Array[Int],
                                pedges: Array[(Int,Int)],
                                pvlabels: Array[Int], pelabels: Array[Int]) {
   def toIntArray(): Array[Int] = {
      Array(vids.length) ++ Array(eids.length) ++ vids ++ eids ++
         pedges.flatMap(kv => Array(kv._1,kv._2)) ++ pvlabels ++ pelabels
   }

   def asString(): String = {
      toIntArray().mkString(",");
   }

   override def toString: String = {
      s"SerializableSubgraph(${vids.mkString(",")}, ${eids.mkString(",")}, ${pedges.mkString(",")}, ${pvlabels.mkString(",")}, ${pelabels.mkString(",")})"
   }
}

object SerializableSubgraph {
   def fromInternalSubgraph(s: Subgraph): SerializableSubgraph = {
      val numVertices = s.getNumVertices
      val numEdges = s.getNumEdges
      val vids = new Array[Int](numVertices)
      val eids = new Array[Int](numEdges)
      val pedges = new Array[(Int,Int)](numEdges)
      val pvlabels = new Array[Int](numVertices)
      val pelabels = new Array[Int](numEdges)

      var i = 0
      while (i < numVertices) {
         val u = s.getVertices.get(i)
         vids(i) = u
         pvlabels(i) = s.getMainGraph.firstVertexLabel(u)
         i += 1
      }

      val pattern = s.quickPattern()
      pattern.turnCanonical()
      val patternEdges = pattern.getEdges
      i = 0
      while (i < numEdges) {
         val e = s.getEdges.get(i)
         eids(i) = e
         pelabels(i) = s.getMainGraph.firstEdgeLabel(e)
         val pedge = patternEdges.get(i)
         pedges(i) = (pedge.getSrcPos, pedge.getDestPos)
         i += 1
      }

      SerializableSubgraph(vids, eids, pedges, pvlabels, pelabels)
   }

   def fromInternalSubgraphToJSON(s: Subgraph): String = {
      implicit val formats = DefaultFormats

      val subgraph = fromInternalSubgraph(s)


      val numVertices = subgraph.vids.length
      val numEdges = subgraph.eids.length

      // json map
      val vertexKeys = (1 to numVertices).map(i => s"v$i")
      val edgeKeys = (1 to numEdges).map(i => s"e$i")
      val jsonMap = new ArrayBuffer[(String,Any)]()
      jsonMap += ("numVertices" -> numVertices)
      jsonMap += ("numEdges" -> numEdges)

      var i = 0
      while (i < numVertices) {
         val v = subgraph.vids(i)
         val label = subgraph.pvlabels(i)
         jsonMap += (s"v${i+1}" -> Map("id" -> v, "label" -> label))
         i += 1
      }

      i = 0
      while (i < numEdges) {
         val e = subgraph.eids(i)
         val label = subgraph.pelabels(i)
         val srcDst = subgraph.pedges(i)
         val src = srcDst._1
         val dst = srcDst._2
         jsonMap += (s"e${i+1}" -> Map("id" -> e, "label" -> label, "src" -> src, "dst" -> dst))
         i += 1
      }

      val jsonString = Serialization.write(jsonMap.toMap)

      jsonString
   }
}
