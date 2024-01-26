package br.ufmg.cs.systems.fractal.subgraph

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
}
