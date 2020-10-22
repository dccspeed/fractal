package br.ufmg.cs.systems.fractal

import java.util.function.BiPredicate

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.periodic.InducedPeriodicSubgraphsPFMCVC
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.subgraph.{PatternInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.ScalaFractalFuncs.CustomSubgraphCallback
import br.ufmg.cs.systems.fractal.util.{Logging, SubgraphCallback}
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

trait ApplicationRunner extends Logging

object SubgraphsListingSF extends ApplicationRunner {
   val appid: String = "subgraphs_listing_sf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int): Unit = {
      val frac = fractalGraph
         .set("num_partitions", numPartitions)
         .set("comm_strategy", commStrategy)
         .efractoid.expand(1).explore(explorationSteps)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount
      }

      logInfo(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object SubgraphsListingPF extends ApplicationRunner {
   val appid: String = "subgraphs_listing_pf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int): Unit = {
      var numSubgraphs = 0L
      val callback: Fractoid[PatternInducedSubgraph] => Unit = frac =>
         synchronized {
            val count = frac.aggregationCount
            logInfo(s"PatternCount ${frac.pattern} ${count}")
            numSubgraphs += count
         }

      val (_, elapsed) = FractalSparkRunner.time {
         fractalGraph
            .set("num_partitions", numPartitions)
            .set("comm_strategy", commStrategy)
            .subgraphsMaxEdgesPF(explorationSteps + 1, callback)
      }

      logInfo(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object InducedSubgraphsListingPFMCVC extends ApplicationRunner {
   val appid: String = "induced_subgraphs_listing_pf_mcvc"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
                      numPartitions: Int, explorationSteps: Int): Unit = {
      var numSubgraphs = 0L
      val callback: Fractoid[PatternInducedSubgraph] => Unit = frac => {
         numSubgraphs += frac.aggregationCountWithCallback(
            (s,c,cb) => s.completeMatch(c, c.getPattern, cb)
         )
      }

      val (_, elapsed) = FractalSparkRunner.time {
         fractalGraph
            .set("num_partitions", numPartitions)
            .set("comm_strategy", commStrategy)
            .inducedSubgraphsPFMCVC(explorationSteps + 1, callback)
      }

      logInfo(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object InducedSubgraphsListingPF extends ApplicationRunner {
   val appid: String = "induced_subgraphs_listing_pf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
                      numPartitions: Int, explorationSteps: Int): Unit = {
      var numSubgraphs = 0L
      val callback: Fractoid[PatternInducedSubgraph] => Unit = frac => {
         numSubgraphs += frac.aggregationCount
      }

      val (_, elapsed) = FractalSparkRunner.time {
         fractalGraph
            .set("num_partitions", numPartitions)
            .set("comm_strategy", commStrategy)
            .inducedSubgraphsPF(explorationSteps + 1, callback)
      }

      logInfo(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object InducedSubgraphsListingSF extends ApplicationRunner {
   val appid: String = "induced_subgraphs_listing_sf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
                      numPartitions: Int, explorationSteps: Int): Unit = {
      val frac = fractalGraph
         .set("num_partitions", numPartitions)
         .set("comm_strategy", commStrategy)
         .vfractoid.expand(1).explore(explorationSteps)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount
      }

      logInfo(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object InducedSubgraphsListingSampleSF extends ApplicationRunner {
   val appid: String = "induced_subgraphs_listing_sample_sf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
                      numPartitions: Int, explorationSteps: Int,
                      fraction: Double): Unit = {

      val (numSugbraphs, elapsed) = FractalSparkRunner.time {
         fractalGraph
            .set("num_partitions", numPartitions)
            .set("comm_strategy", commStrategy)
            .svfractoid(fraction).expand(1)
            .explore(explorationSteps)
            .aggregationCount
      }

      logInfo(s"numSubgraphs=${numSugbraphs}" +
         s" numSubgraphsEstimate=${numSugbraphs / fraction} elapsed=${elapsed}")
   }
}

object MotifsPF extends ApplicationRunner {
   val appid: String = "motifs_pf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int): Unit = {

      val motifsCountsRDD = fractalGraph
         .set("num_partitions", numPartitions)
         .set("comm_strategy", commStrategy)
         .motifsPF(explorationSteps + 1)
      motifsCountsRDD.cache()

      val (_, elapsed) = FractalSparkRunner.time {
         motifsCountsRDD.count() // materialize
      }

      var numSubgraphs = 0L
      var numMotifs = 0L
      val iter = motifsCountsRDD.toLocalIterator
      while (iter.hasNext) {
         val (motif, count) = iter.next()
         logInfo(s"MotifCount ${motif}: ${count}")
         numMotifs += 1
         numSubgraphs += count
      }

      motifsCountsRDD.unpersist()

      logInfo(s"numMotifs=${numMotifs} numSubgraphs=${numSubgraphs}" +
         s" elapsed=${elapsed}")
   }
}

object MotifsPFMCVC extends ApplicationRunner {
   val appid: String = "motifs_pf_mcvc"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int): Unit = {
      val motifsCountsRDD = fractalGraph
         .set("num_partitions", numPartitions)
         .set("comm_strategy", commStrategy)
         .motifsPFMCVC(explorationSteps + 1)
      motifsCountsRDD.cache()

      val (_, elapsed) = FractalSparkRunner.time {
         motifsCountsRDD.count() // materialize
      }

      var numSubgraphs = 0L
      var numMotifs = 0L
      val iter = motifsCountsRDD.toLocalIterator
      while (iter.hasNext) {
         val (motif, count) = iter.next()
         logInfo(s"MotifCount ${motif}: ${count}")
         numMotifs += 1
         numSubgraphs += count
      }

      motifsCountsRDD.unpersist()

      logInfo(s"numMotifs=${numMotifs} numSubgraphs=${numSubgraphs}" +
         s" elapsed=${elapsed}")
   }
}

object MotifsSampleSF extends ApplicationRunner {
   val appid: String = "motifs_sample_sf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int, fraction: Double)
   : Unit = {

      val motifsCountsRDD = fractalGraph
         .set("comm_strategy", commStrategy)
         .set("num_partitions", numPartitions)
         .motifsSampleSF(explorationSteps + 1, fraction)
      motifsCountsRDD.cache()

      val (_, elapsed) = FractalSparkRunner.time {
         motifsCountsRDD.count() // materialize
      }

      var numSubgraphs = 0L
      var numMotifs = 0L
      val iter = motifsCountsRDD.toLocalIterator
      while (iter.hasNext) {
         val (motif, count) = iter.next()
         logInfo(s"MotifCount ${motif}: ${count}" +
            s" estimatedCount=${count / fraction}")
         numMotifs += 1
         numSubgraphs += count
      }

      motifsCountsRDD.unpersist()

      logInfo(s"numMotifs=${numMotifs} numSubgraphs=${numSubgraphs}" +
         s" numSubgraphsEstimate=${numSubgraphs / fraction} elapsed=${elapsed}")
   }
}

object MotifsSF extends ApplicationRunner {
   val appid: String = "motifs_sf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int): Unit = {

      val motifsCountsRDD = fractalGraph
         .set("comm_strategy", commStrategy)
         .set("num_partitions", numPartitions)
         .motifsSF(explorationSteps + 1)
      motifsCountsRDD.cache()

      val (_, elapsed) = FractalSparkRunner.time {
         motifsCountsRDD.count() // materialize
      }

      var numSubgraphs = 0L
      var numMotifs = 0L
      val iter = motifsCountsRDD.toLocalIterator
      while (iter.hasNext) {
         val (motif, count) = iter.next()
         logInfo(s"MotifCount ${motif}: ${count}")
         numMotifs += 1
         numSubgraphs += count
      }

      motifsCountsRDD.unpersist()

      logInfo(s"numMotifs=${numMotifs} numSubgraphs=${numSubgraphs}" +
         s" elapsed=${elapsed}")
   }

}

object CliquesKClistSF extends ApplicationRunner {
   val appid: String = "cliques_kclist_sf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int): Unit = {
      val frac = fractalGraph
         .set("num_partitions", numPartitions)
         .set("comm_strategy", commStrategy)
         .cliquesKClistSF(explorationSteps + 1)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount
      }

      logInfo(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object CliquesSF extends ApplicationRunner {
   val appid: String = "cliques_sf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int): Unit = {
      val frac = fractalGraph
         .set("num_partitions", numPartitions)
         .set("comm_strategy", commStrategy)
         .cliquesSF(explorationSteps + 1)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount
      }

      logInfo(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object MaximalCliquesPF extends ApplicationRunner {
   val appid: String = "maximal_cliques_pf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int): Unit = {

      var numSubgraphs = 0L
      val callback: Fractoid[PatternInducedSubgraph] => Unit = frac => {
         numSubgraphs += frac.aggregationCount
      }

      val (_, elapsed) = FractalSparkRunner.time {
         fractalGraph
            .set("num_partitions", numPartitions)
            .set("comm_strategy", commStrategy)
            .maximalCliquesPF(explorationSteps + 1, callback)
      }

      logInfo(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object MaximalCliquesQuickSF extends ApplicationRunner {
   val appid: String = "maximal_cliques_quick_sf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int): Unit = {

      val maxNumVertices = explorationSteps + 1

      val frac = fractalGraph
         .set("num_partitions", numPartitions)
         .set("comm_strategy", commStrategy)
         .maximalCliquesQuickSF(maxNumVertices)

      val callback: CustomSubgraphCallback[VertexInducedSubgraph] =
         (s, c, cb) => {
            if (s.getNumVertices <= maxNumVertices) {
               cb.apply(s, c)
            }
         }

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCountWithCallback(callback)
      }

      logInfo(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object QuasiCliquesSF extends ApplicationRunner {
   val appid: String = "quasi_cliques_sf"
   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int, minDensity: Double)
   : Unit = {
      // TODO
   }
}

object FSMSF extends ApplicationRunner {
   val appid: String = "fsm_sf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int, minSupport: Int)
   : Unit = {

      // we create the fractoid inside the timer because there is actual work
      // being done to obtain the RDD
      val (patternsSupportsRDD, elapsed) = FractalSparkRunner.time {
         val patternsSupportsRDD = fractalGraph
            .set("num_partitions", numPartitions)
            .set("comm_strategy", commStrategy)
            .fsmSF(minSupport, explorationSteps + 1)
         patternsSupportsRDD.cache()
         patternsSupportsRDD.count() // materialize
         patternsSupportsRDD
      }

      var numSubgraphs = 0L
      var numPatterns = 0L
      val iter = patternsSupportsRDD.toLocalIterator
      while (iter.hasNext) {
         val (pattern, support) = iter.next()
         logInfo(s"PatternSupport ${pattern} ${support}")
         numSubgraphs += support.getNumSubgraphsAggregated
         numPatterns += 1
      }

      patternsSupportsRDD.unpersist()

      logInfo(s"numSubgraphs=${numSubgraphs}" +
         s" numPatterns=${numPatterns} elapsed=${elapsed}")
   }
}

object FSMPF extends ApplicationRunner {
   val appid: String = "fsm_pf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int, minSupport: Int)
   : Unit = {

      // we create the fractoid inside the timer because there is actual work
      // being done to obtain the RDD
      val (patternsSupportsRDD, elapsed) = FractalSparkRunner.time {
         val patternsSupportsRDD = fractalGraph
            .set("num_partitions", numPartitions)
            .set("comm_strategy", commStrategy)
            .fsmPF(minSupport, explorationSteps + 1)
         patternsSupportsRDD.cache()
         patternsSupportsRDD.count() // materialize
         patternsSupportsRDD
      }

      var numSubgraphs = 0L
      var numPatterns = 0L
      val iter = patternsSupportsRDD.toLocalIterator
      while (iter.hasNext) {
         val (pattern, support) = iter.next()
         logInfo(s"PatternSupport ${pattern} ${support}")
         numSubgraphs += support.getNumSubgraphsAggregated
         numPatterns += 1
      }

      patternsSupportsRDD.unpersist()

      logInfo(s"numSubgraphs=${numSubgraphs}" +
         s" numPatterns=${numPatterns} elapsed=${elapsed}")
   }
}

object FSMPFMCVC extends ApplicationRunner {
   val appid: String = "fsm_pf_mcvc"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int, minSupport: Int)
   : Unit = {

      // we create the fractoid inside the timer because there is actual work
      // being done to obtain the RDD
      val (patternsSupportsRDD, elapsed) = FractalSparkRunner.time {
         val patternsSupportsRDD = fractalGraph
            .set("num_partitions", numPartitions)
            .set("comm_strategy", commStrategy)
            .fsmPFMCVC(minSupport, explorationSteps + 1)
         patternsSupportsRDD.cache()
         patternsSupportsRDD.count() // materialize
         patternsSupportsRDD
      }

      var numSubgraphs = 0L
      var numPatterns = 0L
      val iter = patternsSupportsRDD.toLocalIterator
      while (iter.hasNext) {
         val (pattern, support) = iter.next()
         logInfo(s"PatternSupport ${pattern} ${support}")
         numSubgraphs += support.getNumSubgraphsAggregated
         numPatterns += 1
      }

      patternsSupportsRDD.unpersist()

      logInfo(s"numSubgraphs=${numSubgraphs}" +
         s" numPatterns=${numPatterns} elapsed=${elapsed}")
   }
}

object PatternMatchingPFMCVC extends ApplicationRunner {
   val appid: String = "pattern_matching_pf_mcvc"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int, subgraphPath: String)
   : Unit = {

      val pattern = new FractalGraph(
         subgraphPath, fractalGraph.fractalContext).asPattern

      val fracs = fractalGraph
         .set("num_partitions", numPartitions)
         .set("comm_strategy", commStrategy)
         .patternMatchingPFMCVC(pattern)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         var numSubgraphs = 0L
         for (frac <- fracs) {
            numSubgraphs += frac.aggregationCountWithCallback(
               (s,c,cb) => s.completeMatch(c, c.getPattern, cb)
            )
         }
         numSubgraphs
      }

      logInfo(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object PatternMatchingSamplePF extends ApplicationRunner {
   val appid: String = "pattern_matching_sample_pf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int, subgraphPath: String,
             fraction: Double): Unit = {
      val pattern = new FractalGraph(
         subgraphPath, fractalGraph.fractalContext).asPattern

      val frac = fractalGraph
         .set("num_partitions", numPartitions)
         .set("comm_strategy", commStrategy)
         .patternMatchingPF(pattern, fraction)
         .explore(pattern.getNumberOfVertices - 1)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount
      }

      logInfo(s"numSubgraphs=${numSubgraphs}" +
         s" numSubgraphsEstimated=${numSubgraphs / fraction}" +
         s" elapsed=${elapsed}")
   }
}

object PatternMatchingInducedSamplePF extends ApplicationRunner {
   val appid: String = "pattern_matching_induced_sample_pf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int, subgraphPath: String,
             fraction: Double): Unit = {
      val pattern = new FractalGraph(
         subgraphPath, fractalGraph.fractalContext).asPattern

      // induced pattern
      pattern.setInduced(true)

      val frac = fractalGraph
         .set("num_partitions", numPartitions)
         .set("comm_strategy", commStrategy)
         .patternMatchingPF(pattern, fraction)
         .explore(pattern.getNumberOfVertices - 1)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount
      }

      logInfo(s"numSubgraphs=${numSubgraphs}" +
         s" numSubgraphsEstimated=${numSubgraphs / fraction}" +
         s" elapsed=${elapsed}")
   }
}

object PatternMatchingPF extends ApplicationRunner {
   val appid: String = "pattern_matching_pf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int, subgraphPath: String)
   : Unit = {
      val pattern = new FractalGraph(
         subgraphPath, fractalGraph.fractalContext).asPattern

      val frac = fractalGraph
         .set("num_partitions", numPartitions)
         .set("comm_strategy", commStrategy)
         .patternMatchingPF(pattern)
         .explore(pattern.getNumberOfVertices - 1)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount
      }

      logInfo(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object PatternMatchingInducedPF extends ApplicationRunner {
   val appid: String = "pattern_matching_induced_pf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int, subgraphPath: String)
   : Unit = {
      val pattern = new FractalGraph(
         subgraphPath, fractalGraph.fractalContext).asPattern

      // induced pattern
      pattern.setInduced(true)

      val frac = fractalGraph
         .set("num_partitions", numPartitions)
         .set("comm_strategy", commStrategy)
         .patternMatchingPF(pattern)
         .explore(pattern.getNumberOfVertices - 1)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount
      }

      logInfo(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object PeriodicSubgraphsInducedPF extends ApplicationRunner {
   val appid: String = "periodic_subgraphs_induced_pf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int,
             periodicThreshold: Int): Unit = {

      val ec = ExecutionContext.global

      var numSubgraphs = 0L
      val callback: (Pattern, Fractoid[PatternInducedSubgraph]) => Unit =
         (pattern, frac) => {
            Future(frac.aggregationCount)(ec).onComplete {
               case Success(value) =>
                  numSubgraphs += value
               case Failure(exception) =>
                  throw exception
            }(ec)
            //numSubgraphs += frac.aggregationCount
         }


      val (_, elapsed) = FractalSparkRunner.time {
         fractalGraph
            .set("num_partitions", numPartitions)
            .set("comm_strategy", commStrategy)
            .periodicInducedSubgraphsPF(
               periodicThreshold, explorationSteps + 1, callback)
      }

      logInfo(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object PeriodicSubgraphsInducedPFMCVC extends ApplicationRunner {
   val appid: String = "periodic_subgraphs_induced_pf_mcvc"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int,
             periodicThreshold: Int): Unit = {
      var numSubgraphs = 0L
      val callback = (app: InducedPeriodicSubgraphsPFMCVC,
                      pattern: Pattern,
                      frac: Fractoid[PatternInducedSubgraph]) => {

         val predicate = new BiPredicate[PatternInducedSubgraph,
            Computation[PatternInducedSubgraph]] with Serializable {
            override def test(s: PatternInducedSubgraph,
                              c: Computation[PatternInducedSubgraph])
            : Boolean = {
               app.periodicFilter(s, c)
            }
         }

         val aggCallback: (
            PatternInducedSubgraph,
               Computation[PatternInducedSubgraph],
               SubgraphCallback[PatternInducedSubgraph]) => Unit =
            (s,c,cb) => {
               s.completeMatch(c, c.getPattern, predicate, cb)
            }

         numSubgraphs += frac.aggregationCountWithCallback(aggCallback)
      }

      val (_, elapsed) = FractalSparkRunner.time {
         fractalGraph
            .set("num_partitions", numPartitions)
            .set("comm_strategy", commStrategy)
            .periodicInducedSubgraphsPFMCVC(
               periodicThreshold, explorationSteps + 1, callback)
      }

      logInfo(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object PeriodicSubgraphsInducedSF extends ApplicationRunner {
   val appid: String = "periodic_subgraphs_induced_sf"

   def apply(fractalGraph: FractalGraph, commStrategy: String,
             numPartitions: Int, explorationSteps: Int,
             periodicThreshold: Int): Unit = {
      val frac = fractalGraph
         .set("num_partitions", numPartitions)
         .set("comm_strategy", commStrategy)
         .periodicInducedSubgraphsSF(periodicThreshold)
         .explore(explorationSteps)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount
      }

      logInfo(s" numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
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
      def nextArg: String = {
         if (i < args.length) {
            i += 1
            args(i - 1)
         } else {
            null
         }
      }

      val graphClass = nextArg match {
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
      val graphPath = nextArg
      val algorithm = nextArg
      val commStrategy = nextArg
      val numPartitions = nextArg.toInt
      val explorationSteps = nextArg.toInt
      val logLevel = nextArg

      val conf = new SparkConf()
      conf.set("spark.scheduler.mode", "FIFO")
      val sc = new SparkContext(conf)

      if (!sc.isLocal) {
         // TODO: this is ugly but have to make sure all spark executors are up by
         // the time we start executing fractal applications
         Thread.sleep(10000)
      }

      val fc = new FractalContext(sc, logLevel)
      var fractalGraph = fc.textFile (graphPath, graphClass = graphClass)

      def setRemainingConfigs(): Unit = {
         var arg = nextArg
         while (arg != null) {
            logInfo(s"Found config=${arg}")
            val kv = arg.split(":")
            if (kv.length == 2) {
               fractalGraph = fractalGraph.set(kv(0), kv(1))
            }
            arg = nextArg
         }
      }

      algorithm.toLowerCase match {
         case SubgraphsListingSF.appid =>
            setRemainingConfigs()
            SubgraphsListingSF(fractalGraph, commStrategy,
               numPartitions, explorationSteps)

         case SubgraphsListingPF.appid =>
            setRemainingConfigs()
            SubgraphsListingPF(fractalGraph, commStrategy,
               numPartitions, explorationSteps)

         case InducedSubgraphsListingPFMCVC.appid =>
            setRemainingConfigs()
            InducedSubgraphsListingPFMCVC(fractalGraph, commStrategy,
               numPartitions, explorationSteps)

         case InducedSubgraphsListingPF.appid =>
            setRemainingConfigs()
            InducedSubgraphsListingPF(fractalGraph, commStrategy,
               numPartitions, explorationSteps)

         case InducedSubgraphsListingSF.appid =>
            setRemainingConfigs()
            InducedSubgraphsListingSF(fractalGraph, commStrategy,
               numPartitions, explorationSteps)

         case InducedSubgraphsListingSampleSF.appid =>
            val fraction = nextArg.toDouble
            setRemainingConfigs()
            InducedSubgraphsListingSampleSF(fractalGraph, commStrategy,
               numPartitions, explorationSteps, fraction)

         case MotifsPF.appid =>
            setRemainingConfigs()
            MotifsPF(fractalGraph, commStrategy, numPartitions,
               explorationSteps)

         case MotifsPFMCVC.appid =>
            setRemainingConfigs()
            MotifsPFMCVC(fractalGraph, commStrategy, numPartitions,
               explorationSteps)

         case MotifsSampleSF.appid =>
            val fraction = nextArg.toDouble
            setRemainingConfigs()
            MotifsSampleSF(fractalGraph, commStrategy,
               numPartitions, explorationSteps, fraction)

         case MotifsSF.appid =>
            setRemainingConfigs()
            MotifsSF(fractalGraph, commStrategy, numPartitions,
               explorationSteps)

         case CliquesKClistSF.appid =>
            setRemainingConfigs()
            CliquesKClistSF(fractalGraph, commStrategy,numPartitions,
               explorationSteps)

         case CliquesSF.appid =>
            setRemainingConfigs()
            CliquesSF(fractalGraph, commStrategy, numPartitions,
               explorationSteps)

         case MaximalCliquesQuickSF.appid =>
            setRemainingConfigs()
            MaximalCliquesQuickSF(fractalGraph, commStrategy, numPartitions,
               explorationSteps)

         case MaximalCliquesPF.appid =>
            setRemainingConfigs()
            MaximalCliquesPF(fractalGraph, commStrategy, numPartitions,
               explorationSteps)

         case QuasiCliquesSF.appid =>
            val minDensity = nextArg.toDouble
            setRemainingConfigs()
            QuasiCliquesSF(fractalGraph, commStrategy, numPartitions,
               explorationSteps, minDensity)

         case FSMSF.appid =>
            val support = nextArg.toInt
            setRemainingConfigs()
            FSMSF(fractalGraph, commStrategy, numPartitions, explorationSteps,
               support)

         case FSMPF.appid =>
            val support = nextArg.toInt
            setRemainingConfigs()
            FSMPF(fractalGraph, commStrategy, numPartitions, explorationSteps,
               support)

         case FSMPFMCVC.appid =>
            val support = nextArg.toInt
            setRemainingConfigs()
            FSMPFMCVC(fractalGraph, commStrategy, numPartitions, explorationSteps,
               support)

         //case KeywordSearchSF.appid =>
         //   var arg = nextArg
         //   var queryWords = List.empty[String]
         //   while (arg != null) {
         //      queryWords = arg :: queryWords
         //      arg = nextArg
         //   }
         //   setRemainingConfigs()
         //   KeywordSearchSF(fractalGraph, commStrategy, numPartitions,
         //      explorationSteps, queryWords.toArray)

         case PatternMatchingPFMCVC.appid =>
            val subgraphPath = nextArg
            setRemainingConfigs()
            PatternMatchingPFMCVC(fractalGraph, commStrategy, numPartitions,
               explorationSteps, subgraphPath)

         case PatternMatchingPF.appid =>
            val subgraphPath = nextArg
            setRemainingConfigs()
            PatternMatchingPF(fractalGraph, commStrategy, numPartitions,
               explorationSteps, subgraphPath)

         case PatternMatchingInducedPF.appid =>
            val subgraphPath = nextArg
            setRemainingConfigs()
            PatternMatchingInducedPF(fractalGraph, commStrategy,
               numPartitions, explorationSteps, subgraphPath)

         case PatternMatchingSamplePF.appid =>
            val subgraphPath = nextArg
            val fraction = nextArg.toDouble
            setRemainingConfigs()
            PatternMatchingSamplePF(fractalGraph, commStrategy,
               numPartitions, explorationSteps, subgraphPath, fraction)

         case PatternMatchingInducedSamplePF.appid =>
            val subgraphPath = nextArg
            val fraction = nextArg.toDouble
            setRemainingConfigs()
            PatternMatchingInducedSamplePF(fractalGraph, commStrategy,
               numPartitions, explorationSteps, subgraphPath, fraction)

         case PeriodicSubgraphsInducedSF.appid =>
            val periodicThreshold = nextArg.toInt
            setRemainingConfigs()
            PeriodicSubgraphsInducedSF(fractalGraph, commStrategy,
               numPartitions, explorationSteps, periodicThreshold)

         case PeriodicSubgraphsInducedPF.appid =>
            val periodicThreshold = nextArg.toInt
            setRemainingConfigs()
            PeriodicSubgraphsInducedPF(fractalGraph, commStrategy,
               numPartitions, explorationSteps, periodicThreshold)

         case PeriodicSubgraphsInducedPFMCVC.appid =>
            val periodicThreshold = nextArg.toInt
            setRemainingConfigs()
            PeriodicSubgraphsInducedPFMCVC(fractalGraph, commStrategy,
               numPartitions, explorationSteps, periodicThreshold)

         case appName =>
            throw new RuntimeException(s"Unknown app: ${appName}")
      }

      fc.stop()
      sc.stop()
   }
}
