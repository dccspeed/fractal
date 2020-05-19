package br.ufmg.cs.systems.fractal

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.fsm.DomainSupport
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternExplorationPlan, PatternUtils}
import br.ufmg.cs.systems.fractal.subgraph.{PatternInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.Logging
import com.koloboke.collect.set.hash.HashObjSets
import org.apache.hadoop.io._
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
            patterns = PatternUtils.extendByVertex(patterns, 1)
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
      fractalGraph.set("num_partitions", numPartitions)
      fractalGraph.set("comm_strategy", commStrategy)
      val motifsFractoids = fractalGraph.motifspf(explorationSteps + 1)
      for (frac <- motifsFractoids) {
         val motifsCounts = frac
            .aggregationMap[Pattern,LongWritable]("motifs")
         for ((p,c) <- motifsCounts) {
            logInfo(s"MotifCount ${p}: ${c}")
         }
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

      val motifs = motifsRes
         .aggregationMap[Pattern,LongWritable]("motifs")

      for ((m,c) <- motifs) {
         logInfo(s"MotifCount ${m} ${c}")
      }

      logInfo (s"MotifsAppSampling comm=${commStrategy}" +
         s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
         s" graph=${fractalGraph} " +
         s" numPatterns=${motifs.size}"
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

      val motifs = motifsRes
         .aggregationMap[Pattern,LongWritable]("motifs")

      for ((m,c) <- motifs) {
         logInfo(s"MotifCount ${m} ${c}")
      }

      logInfo (s"MotifsApp comm=${commStrategy}" +
         s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
         s" graph=${fractalGraph} numPatterns=${motifs.size}"
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
      val maximalcliquesRes = fractalGraph.maximalCliques.
         set ("comm_strategy", commStrategy).
         set ("num_partitions", numPartitions).
         explore(explorationSteps)

      val numMaximalCliquesAccum = fractalGraph.fractalContext
         .sparkContext.longAccumulator

      val callback = (s: VertexInducedSubgraph,
                      c: Computation[VertexInducedSubgraph]) => {
         numMaximalCliquesAccum.add(1)
      }

      maximalcliquesRes.compute(callback)

      logInfo(s"MaximalCliquesApp" +
         s" numMaximalCliques=${numMaximalCliquesAccum.value}")
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
      fractalGraph.fsm(support, explorationSteps)
   }
}

class FSMAppPatternFirst(val fractalGraph: FractalGraph,
                         commStrategy: String,
                         numPartitions: Int,
                         explorationSteps: Int,
                         supportThreshold: Int) extends FractalSparkApp {
   def execute: Unit = {

      var patterns = HashObjSets.newMutableSet[Pattern]()
      var infrequentPatterns = HashObjSets.newMutableSet[Pattern]()

      do {
         patterns = PatternUtils.extendByEdge(patterns, 1)

         logInfo(s"PatternSetBeforeRemovingInfrequent size=${patterns.size()} ${patterns}")

         if (!infrequentPatterns.isEmpty) {
            logInfo(s"InfrequentPatternsBeforeExtension size=${infrequentPatterns.size()} ${infrequentPatterns}")
            infrequentPatterns = PatternUtils
               .extendByEdge(infrequentPatterns, 1)
            logInfo(s"InfrequentPatternsAfterExtension size=${infrequentPatterns.size()} ${infrequentPatterns}")
            patterns.removeAll(infrequentPatterns)
            infrequentPatterns.clear()
         }

         logInfo(s"PatternSetAfterRemovingInfrequent size=${patterns.size()} ${patterns}")

         val patternsCur = patterns.cursor()
         while (patternsCur.moveNext()) {
            val patternWithoutPlan = patternsCur.elem()
            patternWithoutPlan.setVertexLabeled(false)
            val pattern = PatternExplorationPlan.apply(patternWithoutPlan).get(0)
            val minSupport = supportThreshold
            val gqueryingRes = fractalGraph.gquerying(pattern).
               set("comm_strategy", commStrategy).
               set("num_partitions", numPartitions).
               explore(pattern.getNumberOfVertices - 1).
               aggregate [NullWritable, DomainSupport] (
                  "support",
                  (s,c,k) => {k},
                  (s,c,v) => {v.setSupport(minSupport); v.setSubgraph(s); v},
                  (v1,v2) => {v1.aggregate(v2); v1}
               )

            /**
             * Observation: right now we are being conservative and ignoring the time to fix the domain
             * sets w.r.t. the automorphisms of this pattern (vertex equivalence)
             */
            val support = gqueryingRes.aggregationMap [NullWritable,DomainSupport] ("support")(NullWritable.get())
            val (_, finalAggregateElapsed) = FractalSparkRunner.time {
               support.handleConversionFromQuickToCanonical(pattern, pattern)
            }

            logInfo(s"FinalAggregateAutomorphisms elapsed=${finalAggregateElapsed}")

            if (!support.hasEnoughSupport) {
               infrequentPatterns.add(pattern)
               patternsCur.remove()
               logInfo(s"InfrequentPattern ${pattern} support=${support} minSupport=${minSupport}")
            } else {
               logInfo(s"FrequentPattern ${pattern} support=${support} minSupport=${minSupport}")
            }
         }

      } while (!patterns.isEmpty)
   }
}
class FSMAppPatternFirstLabeled(val fractalGraph: FractalGraph,
                                commStrategy: String,
                                numPartitions: Int,
                                explorationSteps: Int,
                                supportThreshold: Int) extends FractalSparkApp {
   def execute: Unit = {
      fractalGraph.set("comm_strategy", commStrategy)
      fractalGraph.set("num_partitions", numPartitions)
      val frequentPatterns = fractalGraph.fsmpf(supportThreshold,
         explorationSteps + 1)
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

class GQueryingInducedAppSampling(val fractalGraph: FractalGraph,
                                  commStrategy: String,
                                  numPartitions: Int,
                                  explorationSteps: Int,
                                  subgraphPath: String,
                                  fraction: Double) extends FractalSparkApp {
   def execute: Unit = {
      val subgraph = new FractalGraph(
         subgraphPath, fractalGraph.fractalContext, "warn")

      val pattern = subgraph.asPattern
      pattern.setInduced(true)

      val gquerying = fractalGraph.gquerying(pattern, fraction).
         set ("comm_strategy", commStrategy).
         set ("num_partitions", numPartitions).
         explore(explorationSteps)

      val (accums, elapsed) = FractalSparkRunner.time {
         gquerying.compute()
      }

      logInfo(s"GQueryingInducedAppSampling comm=${commStrategy}" +
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

class GQueryingInducedApp(val fractalGraph: FractalGraph,
                          commStrategy: String,
                          numPartitions: Int,
                          explorationSteps: Int,
                          subgraphPath: String) extends FractalSparkApp {
   def execute: Unit = {
      val subgraph = new FractalGraph(
         subgraphPath, fractalGraph.fractalContext, "warn")

      val pattern = subgraph.asPattern
      pattern.setInduced(true)

      val gquerying = fractalGraph.gquerying(pattern).
         set ("comm_strategy", commStrategy).
         set ("num_partitions", numPartitions).
         explore(explorationSteps)

      val (accums, elapsed) = FractalSparkRunner.time {
         gquerying.compute()
      }

      logInfo(s"GQueryingInducedApp comm=${commStrategy}" +
         s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
         s" graph=${fractalGraph} subgraph=${subgraph}" +
         s" counting=${gquerying.numValidSubgraphs()} elapsed=${elapsed}"
      )
   }
}

object FractalSparkRunner extends Logging {
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
         case "fsmpf" =>
            i += 1
            val support = args(i).toInt
            new FSMAppPatternFirst(fractalGraph, commStrategy, numPartitions,
               explorationSteps, support)
         case "fsmpflabeled" =>
            i += 1
            val support = args(i).toInt
            new FSMAppPatternFirstLabeled(fractalGraph, commStrategy, numPartitions,
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
         case "gqueryinginduced" =>
            i += 1
            val subgraphPath = args(i)
            new GQueryingInducedApp(fractalGraph, commStrategy,
               numPartitions, explorationSteps, subgraphPath)
         case "gqueryingsampling" =>
            i += 1
            val subgraphPath = args(i)
            i += 1
            val fraction = args(i).toDouble
            new GQueryingAppSampling(fractalGraph, commStrategy,
               numPartitions, explorationSteps, subgraphPath, fraction)
         case "gqueryinginducedsampling" =>
            i += 1
            val subgraphPath = args(i)
            i += 1
            val fraction = args(i).toDouble
            new GQueryingInducedAppSampling(fractalGraph, commStrategy,
               numPartitions, explorationSteps, subgraphPath, fraction)
         case appName =>
            throw new RuntimeException(s"Unknown app: ${appName}")
      }

      i += 1
      while (i < args.length) {
         logInfo(s"Found config=${args(i)}")
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
