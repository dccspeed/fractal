package io.arabesque

import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.conf.Configuration._
import io.arabesque.conf.SparkConfiguration._
import io.arabesque.utils.collection.AtomicBitSetArray

import java.io.IOException

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.hadoop.io._

import org.apache.spark.{SparkConf, SparkContext}

trait ArabesqueSparkApp {
  def arabGraph: ArabesqueGraph
  def execute: Unit
}

class VSubgraphsApp(val arabGraph: ArabesqueGraph,
    commStrategy: String,
    numPartitions: Int,
    explorationSteps: Int) extends ArabesqueSparkApp {
  def execute: Unit = {
    val vsubgraphsRes = arabGraph.vertexInducedComputation.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      exploreExp (explorationSteps)

    vsubgraphsRes.compute()
  }
}

class MotifsApp(val arabGraph: ArabesqueGraph,
    commStrategy: String,
    numPartitions: Int,
    explorationSteps: Int) extends ArabesqueSparkApp {
  def execute: Unit = {
    val motifsRes = arabGraph.motifs.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    motifsRes.embeddings((_,_) => false).count()

    //val patterns = motifsRes.aggregation("motifs", (_,_) => true)
  }
}

class MotifsGtrieApp(val arabGraph: ArabesqueGraph,
    commStrategy: String,
    numPartitions: Int,
    explorationSteps: Int) extends ArabesqueSparkApp {
  def execute: Unit = {
    val motifsRes = arabGraph.motifsGtrie(explorationSteps + 1).
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    motifsRes.compute()
  }
}

class CliquesNaiveApp(val arabGraph: ArabesqueGraph,
    commStrategy: String,
    numPartitions: Int,
    explorationSteps: Int) extends ArabesqueSparkApp {
  def execute: Unit = {
    val cliquesRes = arabGraph.cliques.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    val (counting, elapsed) = SparkRunner.time {
      cliquesRes.aggregation [IntWritable,LongWritable] ("clique_counting")
    }

    println (s"CliquesNaiveApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${arabGraph} " +
      s" counting=${counting.head._2} elapsed=${elapsed}"
      )
  }
}

class CliquesOptApp(val arabGraph: ArabesqueGraph,
    commStrategy: String,
    numPartitions: Int,
    explorationSteps: Int) extends ArabesqueSparkApp {
  def execute: Unit = {
    val cliquesRes = arabGraph.cliquesOpt(explorationSteps + 1).
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    val (accums, elapsed) = SparkRunner.time {
      cliquesRes.compute()
    }

    println (s"CliquesOptApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${arabGraph} " +
      s" accums=${accums} elapsed=${elapsed}"
      )
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
      set ("arabesque.optimizations", "io.arabesque.optimization.CliqueOptimization").
      explore(explorationSteps)

    val (counting, elapsed) = SparkRunner.time {
      cliquesRes.aggregation [IntWritable,LongWritable] ("clique_counting")
    }

    println (s"CliquesApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${arabGraph} " +
      s" counting=${counting.head._2} elapsed=${elapsed}"
      )
  }
}

class MaximalCliquesApp(val arabGraph: ArabesqueGraph,
    commStrategy: String,
    numPartitions: Int,
    explorationSteps: Int) extends ArabesqueSparkApp {
  def execute: Unit = {
    val maximalcliquesRes = arabGraph.maximalcliques.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    val (counting, elapsed) = SparkRunner.time {
      //maximalcliquesRes.aggregation [IntWritable,LongWritable] ("maximal_clique_counting")
      maximalcliquesRes.embeddings ((_,_) => false).count
    }

    println (s"MaximalCliquesApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${arabGraph} elapsed=${elapsed}"
      )
  }
}

class MaximalCliquesSetsApp(val arabGraph: ArabesqueGraph,
    commStrategy: String,
    numPartitions: Int,
    explorationSteps: Int) extends ArabesqueSparkApp {
  def execute: Unit = {
    val maximalcliquesRes = arabGraph.maximalcliquesSets.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    val (counting, elapsed) = SparkRunner.time {
      maximalcliquesRes.aggregation [IntWritable,LongWritable] ("maximal_clique_counting")
    }

    println (s"MaximalCliquesSetsApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${arabGraph} " +
      s" counting=${counting.head._2} elapsed=${elapsed}"
      )
  }
}

class MaximalCliquesNaiveApp(val arabGraph: ArabesqueGraph,
    commStrategy: String,
    numPartitions: Int,
    explorationSteps: Int) extends ArabesqueSparkApp {
  def execute: Unit = {
    val maximalcliquesRes = arabGraph.maximalcliquesNaive.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    val (counting, elapsed) = SparkRunner.time {
      maximalcliquesRes.aggregation [IntWritable,LongWritable] ("maximal_clique_counting")
    }

    println (s"MaximalCliquesNaiveApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${arabGraph} " +
      s" counting=${counting.head._2} elapsed=${elapsed}"
      )
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
    arabGraph.set ("comm_strategy", commStrategy)
    arabGraph.set ("num_partitions", numPartitions)
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

class GMatchingApp(val arabGraph: ArabesqueGraph,
    commStrategy: String,
    numPartitions: Int,
    explorationSteps: Int,
    subgraphPath: String) extends ArabesqueSparkApp {
  def execute: Unit = {

    //val fs = FileSystem.get(
    //  arabGraph.arabContext.sparkContext.hadoopConfiguration)
    //val path = new Path("file:///tmp/tag")
    //var tag = try {
    //  SparkConfiguration.deserialize [Array[AtomicBitSetArray]] (fs.open(path))
    //} catch {
    //  case e: IOException =>
    //    println(s"Graph tagging not found")
    //    Array[AtomicBitSetArray]()
    //  case e: Throwable =>
    //    throw e
    //}

    val subgraph = new ArabesqueGraph(
      subgraphPath, arabGraph.arabContext, "warn") 

    val (gmatchingRes, symmetryBreakingElapsed) = SparkRunner.time {
      var _gmatchingRes = arabGraph.gmatching(subgraph).
        set ("comm_strategy", commStrategy).
        set ("num_partitions", numPartitions)
      //if (tag.length > 0) {
      //  _gmatchingRes = _gmatchingRes.set("vtag", tag(0)).set("etag", tag(1))
      //}
      _gmatchingRes.exploreExp(explorationSteps)
    }

    val (counting, elapsed) = SparkRunner.time {
      gmatchingRes.aggregation [IntWritable,LongWritable] ("subgraph_counting")
    }

    println (s"GMatchingApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${arabGraph} subgraph=${subgraph}" +
      s" symmetryBreakingElapsed=${symmetryBreakingElapsed}" +
      s" counting=${counting.head._2} elapsed=${elapsed}"
      )

    //val vtag = gmatchingRes.aggregation [NullWritable,AtomicBitSetArray] (
    //  "vprevious_enumeration").head._2
    //
    //val etag = gmatchingRes.aggregation [NullWritable,AtomicBitSetArray] (
    //  "eprevious_enumeration").head._2

    //val tagBytes = SparkConfiguration.serialize(Array(vtag, etag))

    //val res = fs.delete (path, true)
    //val out = fs.create(path)
    //out.write(tagBytes)
    //out.close()
  }
}

class GMatchingNaiveApp(val arabGraph: ArabesqueGraph,
    commStrategy: String,
    numPartitions: Int,
    explorationSteps: Int,
    subgraphPath: String) extends ArabesqueSparkApp {
  def execute: Unit = {

    val subgraph = new ArabesqueGraph(
      subgraphPath, arabGraph.arabContext, "warn") 

    val gmatchingRes = arabGraph.gmatchingNaive(subgraph).
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      exploreExp(explorationSteps)

    val (counting, elapsed) = SparkRunner.time {
      gmatchingRes.aggregation [IntWritable,LongWritable] ("subgraph_counting")
    }

    println (s"GMatchingNaiveApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${arabGraph} subgraph=${subgraph}" +
      s" counting=${counting.head._2} elapsed=${elapsed}"
      )
  }
}

object SparkRunner {
  def time[R](block: => R): (R, Long) = { 
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    (result, t1 - t0)
  }

  def main(args: Array[String]) {
    // args
    var i = 0
    val graphClass = args(i) match {
      case "al" =>
        "io.arabesque.graph.BasicMainGraph"
      case "el" =>
        "io.arabesque.graph.EdgeListGraph"
      case "al-kws" =>
        "io.arabesque.graph.KeywordSearchGraph"
      case other =>
        throw new RuntimeException(s"Input graph format '${other}' is invalid")
    }
    i += 1
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

    if (!sc.isLocal) {
      // TODO: this is ugly but have to make sure all spark executors are up by
      // the time we start running things
      Thread.sleep(10000)
    }

    val arab = new ArabesqueContext(sc, logLevel,
      "/scratch/dossantosdias.1/tmp/fractal")
    val arabGraph = arab.textFile (graphPath, graphClass = graphClass)

    val app = algorithm.toLowerCase match {
      case "vsubgraphs" =>
        new VSubgraphsApp(arabGraph, commStrategy,
          numPartitions, explorationSteps)
      case "motifs" =>
        new MotifsApp(arabGraph, commStrategy,
          numPartitions, explorationSteps)
      case "motifsgtrie" =>
        new MotifsGtrieApp(arabGraph, commStrategy,
          numPartitions, explorationSteps)
      case "cliquesnaive" =>
        new CliquesNaiveApp(arabGraph, commStrategy,
          numPartitions, explorationSteps)
      case "cliques" =>
        new CliquesApp(arabGraph, commStrategy,
          numPartitions, explorationSteps)
      case "cliquesopt" =>
        new CliquesOptApp(arabGraph, commStrategy,
          numPartitions, explorationSteps)
      case "maximalcliquesnaive" =>
        new MaximalCliquesNaiveApp(arabGraph, commStrategy,
          numPartitions, explorationSteps)
      case "maximalcliquessets" =>
        new MaximalCliquesSetsApp(arabGraph, commStrategy,
          numPartitions, explorationSteps)
      case "maximalcliques" =>
        new MaximalCliquesApp(arabGraph, commStrategy,
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
      case "gmatching" =>
        i += 1
        val subgraphPath = args(i)
        new GMatchingApp(arabGraph, commStrategy,
          numPartitions, explorationSteps, subgraphPath)
      case "gmatchingnaive" =>
        i += 1
        val subgraphPath = args(i)
        new GMatchingNaiveApp(arabGraph, commStrategy,
          numPartitions, explorationSteps, subgraphPath)
      case appName =>
        throw new RuntimeException(s"Unknown app: ${appName}")
    }

    i += 1
    while (i < args.length) {
      println (s"Found config=${args(i)}")
      val kv = args(i).split(":")
      if (kv.length == 2) {
        arabGraph.set (kv(0), kv(1))
      }
      i += 1
    }

    app.execute

    arab.stop()
    sc.stop()
  }
}
