package br.ufmg.cs.systems.fractal

import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.hadoop.io._
import org.apache.spark.{SparkConf, SparkContext}

trait FractalSparkApp extends Logging {
  def fractalGraph: FractalGraph
  def execute: Unit
}

class VSubgraphsApp(val fractalGraph: FractalGraph,
                    commStrategy: String,
                    numPartitions: Int,
                    explorationSteps: Int) extends FractalSparkApp {
  def execute: Unit = {
    val vsubgraphsRes = fractalGraph.vfractoidAndExpand.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore (explorationSteps)

    vsubgraphsRes.compute()
  }
}

class MotifsApp(val fractalGraph: FractalGraph,
                commStrategy: String,
                numPartitions: Int,
                explorationSteps: Int) extends FractalSparkApp {
  def execute: Unit = {
    val motifsRes = fractalGraph.motifs.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    val (accums, elapsed) = FractalSparkRunner.time {
      motifsRes.compute()
    }

    logInfo (s"MotifsApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${motifsRes.numValidSubgraphs()} elapsed=${elapsed}"
    )
  }
}

class CliquesOptApp(val fractalGraph: FractalGraph,
                    commStrategy: String,
                    numPartitions: Int,
                    explorationSteps: Int) extends FractalSparkApp {
  def execute: Unit = {
    val cliquesRes = fractalGraph.cliquesKClist(explorationSteps + 1).
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    val (accums, elapsed) = FractalSparkRunner.time {
      cliquesRes.compute()
    }

    logInfo (s"CliquesOptApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${cliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
      )
  }
}

class CliquesApp(val fractalGraph: FractalGraph,
                 commStrategy: String,
                 numPartitions: Int,
                 explorationSteps: Int) extends FractalSparkApp {
  def execute: Unit = {
    val cliquesRes = fractalGraph.cliques.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      set ("fractal.optimizations", "br.ufmg.cs.systems.fractal.optimization.CliqueOptimization").
      explore(explorationSteps)

    val (accums, elapsed) = FractalSparkRunner.time {
      cliquesRes.compute()
    }

    logInfo (s"CliquesApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${cliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
      )
  }
}

class MaximalCliquesApp(val fractalGraph: FractalGraph,
                        commStrategy: String,
                        numPartitions: Int,
                        explorationSteps: Int) extends FractalSparkApp {
  def execute: Unit = {
    val maximalcliquesRes = fractalGraph.maximalcliques.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    val (counting, elapsed) = FractalSparkRunner.time {
      maximalcliquesRes.compute()
    }

    logInfo (s"MaximalCliquesApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${maximalcliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
    )
  }
}

class QuasiCliquesApp(val fractalGraph: FractalGraph,
                      commStrategy: String,
                      numPartitions: Int,
                      explorationSteps: Int,
                      minDensity: Double) extends FractalSparkApp {
  def execute: Unit = {
    val quasiCliquesRes = fractalGraph.quasiCliques(explorationSteps, minDensity).
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions)

    val (counting, elapsed) = FractalSparkRunner.time {
      quasiCliquesRes.compute()
    }

    logInfo (s"QuasiCliquesApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${quasiCliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
    )
  }
}

class FSMApp(val fractalGraph: FractalGraph,
             commStrategy: String,
             numPartitions: Int,
             explorationSteps: Int,
             support: Int) extends FractalSparkApp {
  def execute: Unit = {
    fractalGraph.set ("comm_strategy", commStrategy)
    fractalGraph.set ("num_partitions", numPartitions)

    val (fsm, elapsed) = FractalSparkRunner.time {
      fractalGraph.fsm(support, explorationSteps)
    }

    logInfo (s"FSMApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${fsm.numValidSubgraphs()} elapsed=${elapsed}"
    )
  }
}

class KeywordSearchApp(val fractalGraph: FractalGraph,
                       commStrategy: String,
                       numPartitions: Int,
                       explorationSteps: Int,
                       queryWords: Array[String]) extends FractalSparkApp {
  def execute: Unit = {
    val (kws, elapsed) = FractalSparkRunner.time {
      fractalGraph.keywordSearch(numPartitions, queryWords)
    }

    logInfo (s"KeywordSearchApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${kws.numValidSubgraphs()} elapsed=${elapsed}"
    )
  }
}

class GQueryingApp(val fractalGraph: FractalGraph,
                   commStrategy: String,
                   numPartitions: Int,
                   explorationSteps: Int,
                   subgraphPath: String) extends FractalSparkApp {
  def execute: Unit = {

    val subgraph = new FractalGraph(
      subgraphPath, fractalGraph.fractalContext, "warn")

    val gquerying = fractalGraph.gquerying(subgraph).
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    val (accums, elapsed) = FractalSparkRunner.time {
      gquerying.compute()
    }

    println (s"GQueryingApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} subgraph=${subgraph}" +
      s" counting=${gquerying.numValidSubgraphs()} elapsed=${elapsed}"
      )
  }
}

object FractalSparkRunner {
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
        "br.ufmg.cs.systems.fractal.graph.BasicMainGraph"
      case "el" =>
        "br.ufmg.cs.systems.fractal.graph.EdgeListGraph"
      case "al-kws" =>
        "br.ufmg.cs.systems.fractal.gmlib.keywordsearch.KeywordSearchGraph"
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
      // the time we start executing fractal applications
      Thread.sleep(10000)
    }

    val fc = new FractalContext(sc, logLevel)
    val fractalGraph = fc.textFile (graphPath, graphClass = graphClass)

    val app = algorithm.toLowerCase match {
      case "vsubgraphs" =>
        new VSubgraphsApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "motifs" =>
        new MotifsApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "cliques" =>
        new CliquesApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "cliquesopt" =>
        new CliquesOptApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "maximalcliques" =>
        new MaximalCliquesApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "quasicliques" =>
        i += 1
        val minDensity = args(i).toDouble
        new QuasiCliquesApp(fractalGraph, commStrategy, numPartitions,
          explorationSteps, minDensity)
      case "fsm" =>
        i += 1
        val support = args(i).toInt
        new FSMApp(fractalGraph, commStrategy, numPartitions,
          explorationSteps, support)
      case "kws" =>
        i += 1
        val queryWords = args.slice(i, args.length)
        new KeywordSearchApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps, queryWords)
      case "gquerying" =>
        i += 1
        val subgraphPath = args(i)
        new GQueryingApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps, subgraphPath)
      case appName =>
        throw new RuntimeException(s"Unknown app: ${appName}")
    }

    i += 1
    while (i < args.length) {
      println (s"Found config=${args(i)}")
      val kv = args(i).split(":")
      if (kv.length == 2) {
        fractalGraph.set (kv(0), kv(1))
      }
      i += 1
    }

    app.execute

    fc.stop()
    sc.stop()
  }
}
