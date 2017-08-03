package io.arabesque

import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.conf.Configuration._
import io.arabesque.conf.SparkConfiguration._

import org.apache.spark.{SparkConf, SparkContext}

trait ArabesqueSparkApp {
  def arabGraph: ArabesqueGraph
  def execute: Unit
}

class CliquesApp(val arabGraph: ArabesqueGraph,
    commStrategy: String) extends ArabesqueSparkApp {
  def execute: Unit = {
    val cliquesRes = arabGraph.cliques.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", 24).
      exploreAll()
    val embeddings = cliquesRes.embeddings((_,_) => false)
    println (s"num cliques = ${embeddings.count}")

  }
}

object SparkRunner {
  def main(args: Array[String]) {
    // args
    var i = 0
    val graphPath = args(i)
    i += 1
    val algorithm = args(i)
    i += 1
    val commStrategy = args(i)

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val arab = new ArabesqueContext(sc, "info")
    val arabGraph = arab.textFile (graphPath)

    algorithm.toLowerCase match {
      case "cliques" =>
        new CliquesApp(arabGraph, commStrategy).execute
      case _ =>
    }
  }
}
