package br.ufmg.cs.systems.fractal.pattern

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.read

case class Node(label: Int, id: Int)
case class Link(label: Int, source: Int, target: Int)
case class SerializablePattern(directed: Boolean, multigraph: Boolean, graph: Map[String, Any],
                               nodes: List[Node], links: List[Link]) {
  val nodesArray: Array[Node] = nodes.toArray
  val linksArray: Array[Link] = links.toArray
}


object SerializablePattern {
  def fromNodeLinkNetworkxJSON(data: String): SerializablePattern = {
    implicit val formats = DefaultFormats
    val serializablePattern = read[SerializablePattern](data)
    serializablePattern
  }
}
