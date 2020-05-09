package br.ufmg.cs.systems.fractal

import java.util.function.{IntConsumer, IntPredicate}

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.graph.MainGraph
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternExplorationPlanMCVCVgroups, PatternUtils}
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph
import br.ufmg.cs.systems.fractal.util.{EdgePredicate, EdgePredicates, Logging}
import br.ufmg.cs.systems.fractal.util.collection.{IntArrayList, ObjArrayList}
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool
import com.koloboke.collect.map.hash.HashObjIntMap
import org.apache.hadoop.io._
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

trait FractalSparkApp extends Logging {
   def fractalGraph: FractalGraph
   def execute: Unit
}

class ESubgraphsApp(val fractalGraph: FractalGraph,
                    commStrategy: String,
                    numPartitions: Int,
                    explorationSteps: Int) extends FractalSparkApp {
   def execute: Unit = {
      val esubgraphsRes = fractalGraph.efractoidAndExpand.
         set ("comm_strategy", commStrategy).
         set ("num_partitions", numPartitions).
         explore (explorationSteps)

      esubgraphsRes.compute()
   }
}

class VSubgraphsWithEdgesApp(val fractalGraph: FractalGraph,
                             commStrategy: String,
                             numPartitions: Int,
                             explorationSteps: Int) extends FractalSparkApp {
   def execute: Unit = {
      val vsubgraphsRes = fractalGraph.vfractoidAndExpand.
         set ("comm_strategy", commStrategy).
         set ("num_partitions", numPartitions).
         explore (explorationSteps)

      vsubgraphsRes.compute((s,c) => s.getEdges())
   }
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

class VSubgraphsAppSampling(val fractalGraph: FractalGraph,
                            commStrategy: String,
                            numPartitions: Int,
                            explorationSteps: Int,
                            fraction: Double) extends FractalSparkApp {
   def execute: Unit = {
      val vsubgraphsRes = fractalGraph.svfractoid(fraction).
         expand(1).
         set ("comm_strategy", commStrategy).
         set ("num_partitions", numPartitions).
         explore (explorationSteps)

      vsubgraphsRes.compute()
   }
}

class MotifsAppPatternFirst(val fractalGraph: FractalGraph,
                            commStrategy: String,
                            numPartitions: Int,
                            explorationSteps: Int) extends FractalSparkApp {
   def execute: Unit = {

      val (patterns, elapsed) = FractalSparkRunner.time {
         var patterns = PatternUtils.singleVertexPatternSet();
         logInfo(s"PatternSet ${patterns}")
         for (i <- 0 until explorationSteps) {
            patterns = PatternUtils.extendByVertex(patterns)
            logInfo(s"PatternSetExtension[${i + 1}] ${patterns}")
         }
         patterns
      }

      logInfo(s"CanonicalPatterns numVertices=${explorationSteps + 1}" +
         s" numPatterns=${patterns.size()}" +
         s" elapsed=${elapsed}")

      val patternsArray = new Array[Pattern](patterns.size())
      val countsArray = new Array[Long](patterns.size())
      val elapsedTimes = new Array[Long](patterns.size())
      var i = 0

      val cur = patterns.cursor()
      while (cur.moveNext()) {
         val pattern = cur.elem()

         /**
          * Motifs counting consider induced subgraphs
          */
         pattern.setInduced(true);

         val gqueryingRes = fractalGraph.gquerying(pattern).
            set ("comm_strategy", commStrategy).
            set ("num_partitions", numPartitions).
            explore(explorationSteps)

         val (accums, elapsed) = FractalSparkRunner.time {
            gqueryingRes.compute()
         }

         elapsedTimes(i) = elapsed
         patternsArray(i) = pattern
         countsArray(i) = gqueryingRes.numValidSubgraphs()
         i += 1
      }

      for (i <- 0 until patternsArray.length) {
         logInfo (s"MotifsAppPatternFirst comm=${commStrategy}" +
            s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
            s" graph=${fractalGraph} " +
            s" pattern=${patternsArray(i)}" +
            s" count=${countsArray(i)}" +
            s" elapsed=${elapsedTimes(i)}"
         )
      }

   }
}

class MotifsAppPatternFirstLabeled(val fractalGraph: FractalGraph,
                            commStrategy: String,
                            numPartitions: Int,
                            explorationSteps: Int) extends FractalSparkApp {
   def execute: Unit = {

      val (patterns, elapsed) = FractalSparkRunner.time {
         var patterns = PatternUtils.singleVertexPatternSet();
         logInfo(s"PatternSet ${patterns}")
         for (i <- 0 until explorationSteps) {
            patterns = PatternUtils.extendByVertex(patterns)
            logInfo(s"PatternSetExtension[${i + 1}] ${patterns}")
         }
         patterns
      }

      logInfo(s"CanonicalPatterns numVertices=${explorationSteps + 1}" +
         s" numPatterns=${patterns.size()}" +
         s" elapsed=${elapsed}")

      var patternsCounts = scala.collection.mutable.Map.empty[Pattern,LongWritable]
      var i = 0

      val cur = patterns.cursor()
      while (cur.moveNext()) {
         val pattern = cur.elem()

         /**
          * Motifs counting consider induced subgraphs, unlabeled at first
          */
         pattern.setInduced(true)
         pattern.setVertexLabeled(false)

         val gqueryingRes = fractalGraph.gquerying(pattern).
            aggregate[Pattern,LongWritable](
               "motifs",
               (s,c,k) => {s.labeledPattern(c.getPattern)},
               (s,c,v) => {v.set(1); v},
               (v1,v2) => {v1.set(v1.get() + v2.get()); v1}
            ).
            set ("comm_strategy", commStrategy).
            set ("num_partitions", numPartitions).
            explore(explorationSteps)

         val (motifsCounts, elapsed) = FractalSparkRunner.time {
            gqueryingRes.aggregationMap[Pattern,LongWritable]("motifs")
         }

         patternsCounts = patternsCounts ++ motifsCounts
         i += 1
      }

      for ((p,c) <- patternsCounts) {
         logInfo(s"${p}: ${c}")
      }
   }
}

class MotifsAppSampling(val fractalGraph: FractalGraph,
                        commStrategy: String,
                        numPartitions: Int,
                        explorationSteps: Int,
                        fraction: Double) extends FractalSparkApp {
   def execute: Unit = {
      val motifsRes = fractalGraph.motifs(fraction).
         set ("comm_strategy", commStrategy).
         set ("num_partitions", numPartitions).
         explore(explorationSteps)

      val (accums, elapsed) = FractalSparkRunner.time {
         motifsRes.compute()
      }

      logInfo (s"MotifsAppSampling comm=${commStrategy}" +
         s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
         s" graph=${fractalGraph} " +
         s" numValidSubgraphs=${motifsRes.numValidSubgraphs()} elapsed=${elapsed}"
      )
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
         //set ("fractal.optimizations", "br.ufmg.cs.systems.fractal.optimization.CliqueOptimization").
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

class GQueryingMCVCApp(val fractalGraph: FractalGraph,
                   commStrategy: String,
                   numPartitions: Int,
                   explorationSteps: Int,
                   subgraphPath: String) extends FractalSparkApp {
   def execute: Unit = {
      val subgraph = new FractalGraph(
         subgraphPath, fractalGraph.fractalContext, "warn")

      val pattern = subgraph.asPattern
      val partialResults = fractalGraph.gqueryingmcvc(pattern)
      var numValidSubgraphs = 0L
      var totalElapsed = 0L

      partialResults.foreach { gquerying =>

         val (numValidSubgraphs, elapsed) = FractalSparkRunner.time {
            gquerying.
               set("comm_strategy", commStrategy).
               set("num_partitions", numPartitions).
               compute()
         }

         totalElapsed += elapsed
         logInfo(s"GQueryingMCVCAppPartial comm=${commStrategy}" +
            s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
            s" graph=${fractalGraph} pattern=${pattern}" +
            s" counting=${numValidSubgraphs} elapsed=${elapsed}")
      }

      logInfo(s"GQueryingMCVCApp comm=${commStrategy}" +
         s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
         s" graph=${fractalGraph} pattern=${pattern}" +
         s" counting=${numValidSubgraphs} elapsed=${totalElapsed}")
   }
}

class GQueryingAppSampling(val fractalGraph: FractalGraph,
                           commStrategy: String,
                           numPartitions: Int,
                           explorationSteps: Int,
                           subgraphPath: String,
                           fraction: Double) extends FractalSparkApp {
   def execute: Unit = {
      val subgraph = new FractalGraph(
         subgraphPath, fractalGraph.fractalContext, "warn")

      val gquerying = fractalGraph.gquerying(subgraph.asPattern, fraction).
         set ("comm_strategy", commStrategy).
         set ("num_partitions", numPartitions).
         explore(explorationSteps)

      val (accums, elapsed) = FractalSparkRunner.time {
         gquerying.compute()
      }

      logInfo(s"GQueryingAppSampling comm=${commStrategy}" +
         s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
         s" graph=${fractalGraph} subgraph=${subgraph}" +
         s" counting=${gquerying.numValidSubgraphs()} elapsed=${elapsed}"
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

      val gquerying = fractalGraph.gquerying(subgraph.asPattern).
         set ("comm_strategy", commStrategy).
         set ("num_partitions", numPartitions).
         explore(explorationSteps)

      val (accums, elapsed) = FractalSparkRunner.time {
         gquerying.compute()
      }

      logInfo(s"GQueryingApp comm=${commStrategy}" +
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
         case "sc" =>
            "br.ufmg.cs.systems.fractal.graph.SuccinctMainGraph"
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
         case "esubgraphs" =>
            new ESubgraphsApp(fractalGraph, commStrategy,
               numPartitions, explorationSteps)
         case "vsubgraphswithedges" =>
            new VSubgraphsWithEdgesApp(fractalGraph, commStrategy,
               numPartitions, explorationSteps)
         case "vsubgraphs" =>
            new VSubgraphsApp(fractalGraph, commStrategy,
               numPartitions, explorationSteps)
         case "vsubgraphssampling" =>
            i += 1
            val fraction = args(i).toDouble
            new VSubgraphsAppSampling(fractalGraph, commStrategy,
               numPartitions, explorationSteps, fraction)
         case "motifspf" =>
            new MotifsAppPatternFirst(fractalGraph, commStrategy,
               numPartitions, explorationSteps)
         case "motifspflabeled" =>
            new MotifsAppPatternFirstLabeled(fractalGraph, commStrategy,
               numPartitions, explorationSteps)
         case "motifs" =>
            new MotifsApp(fractalGraph, commStrategy,
               numPartitions, explorationSteps)
         case "motifssampling" =>
            i += 1
            val fraction = args(i).toDouble
            new MotifsAppSampling(fractalGraph, commStrategy,
               numPartitions, explorationSteps, fraction)
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
         case "gqueryingmcvc" =>
            i += 1
            val subgraphPath = args(i)
            new GQueryingMCVCApp(fractalGraph, commStrategy,
               numPartitions, explorationSteps, subgraphPath)
         case "gquerying" =>
            i += 1
            val subgraphPath = args(i)
            new GQueryingApp(fractalGraph, commStrategy,
               numPartitions, explorationSteps, subgraphPath)
         case "gqueryingsampling" =>
            i += 1
            val subgraphPath = args(i)
            i += 1
            val fraction = args(i).toDouble
            new GQueryingAppSampling(fractalGraph, commStrategy,
               numPartitions, explorationSteps, subgraphPath, fraction)
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
