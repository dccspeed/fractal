package io.arabesque

import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.conf.Configuration._
import io.arabesque.conf.SparkConfiguration._

import org.apache.spark.{SparkConf, SparkContext}

trait ArabesqueSparkApp {
  def arabGraph: ArabesqueGraph
  def execute: Unit
}

class MotifsApp(val arabGraph: ArabesqueGraph,
    commStrategy: String,
    numPartitions: Int,
    explorationSteps: Int) extends ArabesqueSparkApp {
  def execute: Unit = {
    val motifsRes = arabGraph.motifs.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      //copy (mustSync = true).
      explore(explorationSteps)

    val patterns = motifsRes.aggregation("motifs", (_,_) => true)
    //val embeddings = motifsRes.embeddings((_,_) => false)
    //println (s"num motifs = ${embeddings.count}")
  }
}

class CliquesApp(val arabGraph: ArabesqueGraph,
    commStrategy: String,
    numPartitions: Int,
    explorationSteps: Int) extends ArabesqueSparkApp {
  def execute: Unit = {
    val cliquesRes = arabGraph.cliques.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      copy (mustSync = true).
      explore(explorationSteps)
    val embeddings = cliquesRes.embeddings((_,_) => false)
    println (s"num cliques = ${embeddings.count}")
  }
}

class FSMApp(val arabGraph: ArabesqueGraph,
    commStrategy: String,
    numPartitions: Int,
    explorationSteps: Int,
    support: Int) extends ArabesqueSparkApp {
  def execute: Unit = {
    val _fsmRes = arabGraph.fsm(support).
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions)
    val fsmRes = if (explorationSteps >= 0) {
      _fsmRes.exploreExp (explorationSteps)
    } else {
      _fsmRes.exploreAll()
    }
    val embeddings = fsmRes.embeddings((_,_) => false)
    println (s"num frequent embeddings = ${embeddings.count}")
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
    i += 1
    val numPartitions = args(i).toInt
    i += 1
    val explorationSteps = args(i).toInt
    i += 1
    val logLevel = args(i)

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val arab = new ArabesqueContext(sc, logLevel)
    val arabGraph = arab.textFile (graphPath)

    algorithm.toLowerCase match {
      case "motifs" =>
        new MotifsApp(arabGraph, commStrategy,
          numPartitions, explorationSteps).execute
      case "cliques" =>
        new CliquesApp(arabGraph, commStrategy,
          numPartitions, explorationSteps).execute
      case "fsm" =>
        i += 1
        val support = args(i).toInt
        new FSMApp(arabGraph, commStrategy, numPartitions,
          explorationSteps, support).execute
      case _ =>
    }

    arab.stop()
    sc.stop()
  }
}
