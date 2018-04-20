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
      //copy (mustSync = true).
      explore(explorationSteps)
    val embeddings = cliquesRes.embeddings((_,_) => false)
    println (s"num cliques = ${embeddings.count}")
  }
}

class ECliquesApp(val arabGraph: ArabesqueGraph,
    commStrategy: String,
    numPartitions: Int,
    explorationSteps: Int) extends ArabesqueSparkApp {
  def execute: Unit = {
    val cliquesRes = arabGraph.ecliques.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)
    val embeddings = cliquesRes.embeddings((_,_) => false)
    println (s"num cliques = ${embeddings.count}")
  }
}

class QuasiCliquesApp(val arabGraph: ArabesqueGraph,
    commStrategy: String,
    numPartitions: Int,
    explorationSteps: Int,
    minDensity: Double) extends ArabesqueSparkApp {
  def execute: Unit = {
    val quasiCliquesRes = arabGraph.quasiCliques(explorationSteps, minDensity).
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions)
    val embeddings = quasiCliquesRes.embeddings((_,_) => false)
    println (s"num quasiCliques = ${embeddings.count}")
  }
}

class FSMApp(val arabGraph: ArabesqueGraph,
    commStrategy: String,
    numPartitions: Int,
    explorationSteps: Int,
    support: Int) extends ArabesqueSparkApp {
  def execute: Unit = {
    arabGraph.fsm2(support, explorationSteps)
  }
}

class KeywordSearchApp(val arabGraph: ArabesqueGraph,
    commStrategy: String,
    numPartitions: Int,
    explorationSteps: Int,
    queryWords: Array[String]) extends ArabesqueSparkApp {
  def execute: Unit = {
    val kwsRes = arabGraph.keywordSearch(numPartitions, queryWords)
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

    // TODO: this is ugly but have to make sure all spark executors are up by
    // the time we start running things
    Thread.sleep(3000)

    val arab = new ArabesqueContext(sc, logLevel,
      "/scratch/dossantosdias.1/tmp/fractal")
    val arabGraph = arab.textFile (graphPath)

    val app = algorithm.toLowerCase match {
      case "motifs" =>
        new MotifsApp(arabGraph, commStrategy,
          numPartitions, explorationSteps)
      case "cliques" =>
        new CliquesApp(arabGraph, commStrategy,
          numPartitions, explorationSteps)
      case "ecliques" =>
        new ECliquesApp(arabGraph, commStrategy,
          numPartitions, explorationSteps)
      case "quasicliques" =>
        i += 1
        val minDensity = args(i).toDouble
        new QuasiCliquesApp(arabGraph, commStrategy, numPartitions,
          explorationSteps, minDensity)
      case "fsm" =>
        i += 1
        val support = args(i).toInt
        new FSMApp(arabGraph, commStrategy, numPartitions,
          explorationSteps, support)
      case "kws" =>
        i += 1
        val queryWords = args.slice(i, args.length)
        new KeywordSearchApp(arabGraph, commStrategy,
          numPartitions, explorationSteps, queryWords)
      case appName =>
        throw new RuntimeException(s"Unknown app: ${appName}")
    }

    i += 1
    while (i < args.length) {
      val kv = args(i).split(":")
      arabGraph.set (kv(0), kv(1))
      i += 1
    }

    app.execute

    arab.stop()
    sc.stop()
  }
}
