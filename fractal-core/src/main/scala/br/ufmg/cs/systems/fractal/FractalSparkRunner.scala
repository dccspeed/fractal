package br.ufmg.cs.systems.fractal

import br.ufmg.cs.systems.fractal.callback.SubgraphCallback
import br.ufmg.cs.systems.fractal.computation.{Computation, SamplingEnumerator}
import br.ufmg.cs.systems.fractal.gmlib.periodic.InducedPeriodicSubgraphsPAMCVC
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternExplorationPlan, PatternExplorationPlanOrderingHeuristic, PatternUtils, PatternUtilsRDD}
import br.ufmg.cs.systems.fractal.subgraph.{PatternInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.ReportFuncs._
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import com.koloboke.collect.map.hash.HashIntIntMaps
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import java.util.function.BiPredicate
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object GeneratePatternsAndPlansHeuristic extends Logging {
   val appid: String = "patterns_and_plans_heuristic"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int, induced: Boolean): Unit = {
      val sc = fractalGraph.fractalContext.sparkContext
      val numVertices = explorationSteps + 1
      val patternsIter = PatternUtilsRDD.getOrGenerateVertexPatternsRDD(sc, numVertices).toLocalIterator
      while (patternsIter.hasNext) {
         val pattern = patternsIter.next()
         pattern.setVertexLabeled(false)
         pattern.setEdgeLabeled(false)
         pattern.setInduced(induced)
         val patternWithPlan = PatternExplorationPlanOrderingHeuristic.apply(
            pattern, HashIntIntMaps.newUpdatableMap()).getLast
         logApp(s"PatternWithPlan ${patternWithPlan} plan=${patternWithPlan.explorationPlan()}" +
           s" lowerBound=${patternWithPlan.vsymmetryBreakerLowerBound()}" +
           s" upperBound=${patternWithPlan.vsymmetryBreakerUpperBound()}")
      }
   }
}

object SubgraphsListingPO extends Logging {
   val appid: String = "subgraphs_listing_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int): Unit = {
      val frac = fractalGraph
         .efractoid.extend(1).explore(explorationSteps)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object SubgraphsListingPA extends Logging {
   val appid: String = "subgraphs_listing_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int): Unit = {
      var numSubgraphs = 0L
      val callback: Fractoid[PatternInducedSubgraph] => Unit = frac =>
         synchronized {
            val count = frac.aggregationCount
            logApp(s"PatternCount ${frac.pattern} ${count}")
            numSubgraphs += count
         }

      val (_, elapsed) = FractalSparkRunner.time {
         fractalGraph.subgraphsMaxEdgesPA(explorationSteps + 1, callback)
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object InducedSubgraphsListingPAMCVC extends Logging {
   val appid: String = "induced_subgraphs_listing_pa_mcvc"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int): Unit = {
      var numSubgraphs = 0L
      val callback: Fractoid[PatternInducedSubgraph] => Unit = frac => {
         numSubgraphs += frac.aggregationCountWithCallback(
            (s,c,cb) => s.completeMatch(c, c.getPattern, cb)
         )
      }

      val (_, elapsed) = FractalSparkRunner.time {
         fractalGraph.inducedSubgraphsPAMCVC(explorationSteps + 1, callback)
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object InducedSubgraphsListingPA extends Logging {
   val appid: String = "induced_subgraphs_listing_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int): Unit = {
      var numSubgraphs = 0L
      val callback: Fractoid[PatternInducedSubgraph] => Unit = frac => {
         numSubgraphs += frac.aggregationCount
      }

      val (_, elapsed) = FractalSparkRunner.time {
         fractalGraph.inducedSubgraphsPA(explorationSteps + 1, callback)
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object InducedSubgraphsListingPO extends Logging {
   val appid: String = "induced_subgraphs_listing_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int): Unit = {
      val frac = fractalGraph.vfractoid.extend(1).explore(explorationSteps)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object InducedSubgraphsListingSamplePA extends Logging {
   val appid: String = "induced_subgraphs_listing_sample_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             fraction: Double): Unit = {

      val sc = fractalGraph.fractalContext.sparkContext

      val fractionKey = "sampling_fraction"
      val senumClass = classOf[SamplingEnumerator[PatternInducedSubgraph]]

      val (numSubgraphs, elapsedMs) = FractalSparkRunner.time {

         val patterns = PatternUtilsRDD.getOrGenerateVertexPatternsRDD(sc,
            explorationSteps + 1)
         val patternsIter = patterns.toLocalIterator

         var numSubgraphsTotal = 0L
         while (patternsIter.hasNext) {
            val pattern = patternsIter.next()
            pattern.setInduced(true)
            val fractoid = fractalGraph
               .set(fractionKey, fraction)
               .pfractoid(pattern)
               .extend(1, senumClass)
               .explore(explorationSteps)
            val (numSubgraphs, elapsedMs) = FractalSparkRunner.time {
               fractoid.aggregationCount
            }
            numSubgraphsTotal += numSubgraphs
            logApp(s"StepResult fractoid=${fractoid}" +
               s" numSubgraphs=${numSubgraphs} elapsedMs=${elapsedMs}" +
               s" throughput=${numSubgraphs / elapsedMs.toDouble}")
         }
         numSubgraphsTotal
      }

      logApp(s"FinalResult" +
         s" numSubgraphs=${numSubgraphs} elapsedMs=${elapsedMs}" +
         s" thoughput=${numSubgraphs / elapsedMs.toDouble}")

   }
}

object InducedSubgraphsListingSamplePO extends Logging {
   val appid: String = "induced_subgraphs_listing_sample_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             fraction: Double): Unit = {

      val fractionKey = "sampling_fraction"
      val senumClass = classOf[SamplingEnumerator[VertexInducedSubgraph]]

      val (numSugbraphs, elapsed) = FractalSparkRunner.time {
         fractalGraph
            .set(fractionKey, fraction)
            .vfractoid
            .extend(1, senumClass)
            .explore(explorationSteps)
            .aggregationCount
      }

      logApp(s"numSubgraphs=${numSugbraphs}" +
         s" numSubgraphsEstimate=${numSugbraphs / fraction} elapsed=${elapsed}" +
         s" throughput=${numSugbraphs / elapsed.toDouble}")
   }
}

object MotifsPA extends Logging {
   val appid: String = "motifs_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int): Unit = {

      val motifsCountsRDD = fractalGraph.motifsPA(explorationSteps + 1)
      motifsCountsRDD.cache()

      val (_, elapsed) = FractalSparkRunner.time {
         motifsCountsRDD.count() // materialize
      }

      var numSubgraphs = 0L
      var numMotifs = 0L
      val iter = motifsCountsRDD.toLocalIterator
      while (iter.hasNext) {
         val (motif, count) = iter.next()
         logApp(s"MotifCount ${motif}: ${count}")
         numMotifs += 1
         numSubgraphs += count
      }

      motifsCountsRDD.unpersist()

      logApp(s"numMotifs=${numMotifs} numSubgraphs=${numSubgraphs}" +
         s" elapsed=${elapsed}")
   }
}

object MotifsPAMCVC extends Logging {
   val appid: String = "motifs_pa_mcvc"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int): Unit = {
      val motifsCountsRDD = fractalGraph.motifsPAMCVC(explorationSteps + 1)
      motifsCountsRDD.cache()

      val (_, elapsed) = FractalSparkRunner.time {
         motifsCountsRDD.count() // materialize
      }

      var numSubgraphs = 0L
      var numMotifs = 0L
      val iter = motifsCountsRDD.toLocalIterator
      while (iter.hasNext) {
         val (motif, count) = iter.next()
         logApp(s"MotifCount ${motif}: ${count}")
         numMotifs += 1
         numSubgraphs += count
      }

      motifsCountsRDD.unpersist()

      logApp(s"numMotifs=${numMotifs} numSubgraphs=${numSubgraphs}" +
         s" elapsed=${elapsed}")
   }
}

object MotifsSamplePA extends Logging {
   val appid: String = "motifs_sample_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int, fraction: Double)
   : Unit = {

      val sc = fractalGraph.fractalContext.sparkContext

      val (numSubgraphs, elapsedMs) = FractalSparkRunner.time {

         val patterns = PatternUtilsRDD.getOrGenerateVertexPatternsRDD(sc,
            explorationSteps + 1)
         val patternsIter = patterns.toLocalIterator

         var numSubgraphsTotal = 0L
         while (patternsIter.hasNext) {
            val pattern = patternsIter.next()
            pattern.setInduced(true)
            val fractoid = fractalGraph
               .set("sampling_fraction", fraction)
               .pfractoid(pattern)
               .extend(pattern.getNumberOfVertices,
                  classOf[SamplingEnumerator[PatternInducedSubgraph]])

            val (numSubgraphs, elapsedMs) = FractalSparkRunner.time {
               fractoid.aggregationCount
            }

            numSubgraphsTotal += numSubgraphs

            logApp(s"StepResult fractoid=${fractoid}" +
               s" numSubgraphs=${numSubgraphs} elapsedMs=${elapsedMs}" +
               s" throughput=${numSubgraphs / elapsedMs.toDouble}")
         }

         numSubgraphsTotal
      }

      logApp(s"FinalResult" +
         s" numSubgraphs=${numSubgraphs}" +
         s" numSubgraphsEstimate=${numSubgraphs / fraction} " +
         s" elapsedMs=${elapsedMs}" +
         s" throughput=${numSubgraphs / elapsedMs.toDouble}")
   }
}

object MotifsSamplePO extends Logging {
   val appid: String = "motifs_sample_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int, fraction: Double)
   : Unit = {

      val motifsCountsRDD = fractalGraph.motifsSamplePO(explorationSteps + 1, fraction)
      motifsCountsRDD.cache()

      val (_, elapsed) = FractalSparkRunner.time {
         motifsCountsRDD.count() // materialize
      }

      var numSubgraphs = 0L
      var numMotifs = 0L
      val iter = motifsCountsRDD.toLocalIterator
      while (iter.hasNext) {
         val (motif, count) = iter.next()
         logApp(s"MotifCount ${motif}: ${count}" +
            s" estimatedCount=${count / fraction}")
         numMotifs += 1
         numSubgraphs += count
      }

      motifsCountsRDD.unpersist()

      logApp(s"FinalResult numMotifs=${numMotifs}" +
         s" numSubgraphs=${numSubgraphs}" +
         s" numSubgraphsEstimate=${numSubgraphs / fraction} elapsed=${elapsed}" +
         s" throughput=${numSubgraphs / elapsed.toDouble}")
   }
}

object MotifsPO extends Logging {
   val appid: String = "motifs_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int): Unit = {

      val motifsCountsRDD = fractalGraph.motifsPO(explorationSteps + 1)
      motifsCountsRDD.cache()

      val (_, elapsed) = FractalSparkRunner.time {
         motifsCountsRDD.count() // materialize
      }

      var numSubgraphs = 0L
      var numMotifs = 0L
      val iter = motifsCountsRDD.toLocalIterator
      while (iter.hasNext) {
         val (motif, count) = iter.next()
         logApp(s"MotifCount ${motif}: ${count}")
         numMotifs += 1
         numSubgraphs += count
      }

      motifsCountsRDD.unpersist()

      logApp(s"numMotifs=${numMotifs} numSubgraphs=${numSubgraphs}" +
         s" elapsed=${elapsed}")
   }

}

object CliquesCustomKClist extends Logging {
   val appid: String = "cliques_custom_kclist"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int): Unit = {
      val frac = fractalGraph.cliquesCustomKClist(explorationSteps + 1)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount(COUNT_AGG_REPORT)
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object CliquesPO extends Logging {
   val appid: String = "cliques_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int): Unit = {
      val frac = fractalGraph.cliquesPO(explorationSteps + 1)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount(COUNT_AGG_REPORT)
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object CliquesPA extends Logging {
   val appid: String = "cliques_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int): Unit = {
      val frac = fractalGraph.cliquesPA(explorationSteps + 1)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount(COUNT_AGG_REPORT)
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object MaximalCliquesPO extends Logging {
   val appid: String = "maximal_cliques_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int): Unit = {

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         fractalGraph.maximalCliquesPO(explorationSteps + 1)
            .aggregationCount(COUNT_AGG_REPORT)
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object MaximalCliquesPA extends Logging {
   val appid: String = "maximal_cliques_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int): Unit = {

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         fractalGraph.maximalCliquesPA(explorationSteps + 1)
            .aggregationCount(COUNT_AGG_REPORT)
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object MaximalCliquesCustomQuick extends Logging {
   val appid: String = "maximal_cliques_custom_quick"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int): Unit = {

      val maxNumVertices = explorationSteps + 1

      val frac = fractalGraph.maximalCliquesCustomQuick(maxNumVertices)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount(COUNT_AGG_REPORT)
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object QuasiCliquesPO extends Logging {
   val appid: String = "quasi_cliques_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int, minDensity: Double)
   : Unit = {
      val fc = fractalGraph.fractalContext
      val numVertices = explorationSteps + 1

      val numSteps = 1
      val timeLimitMs = fractalGraph.config.getTotalTimeLimitMs
      val (numSubgraphs, elapsedMs) = FractalSparkRunner.time {
         val stepTimeLimitMs = timeLimitMs
         val fractoid = fractalGraph.quasiCliquesPO(numVertices, minDensity)
            .withStepTimeLimit(stepTimeLimitMs)
         val (numSubgraphsTry, elapsedMs) = FractalSparkRunner.time {
            fc.trySubmitFractalStep(fractoid)(
               f => f.aggregationCount(COUNT_AGG_REPORT))
         }

         val (numSubgraphs, exception) = numSubgraphsTry match {
            case Success(numSubgraphs) => (numSubgraphs, null)
            case Failure(e) => (0L, e)
         }

         logApp(s"StepResult fractoid=${fractoid}" +
            s" exception=${exception}" +
            s" numVertices=${numVertices}" +
            s" minDensity=${minDensity}" +
            s" numSteps=${numSteps}" +
            s" stepTimeLimitMs=${stepTimeLimitMs}" +
            s" numSubgraphs=${numSubgraphs}" +
            s" elapsedMs=${elapsedMs}" +
            s" throughput=${numSubgraphs / elapsedMs.toDouble}")

         numSubgraphs
      }

      logApp(s"FinalResult" +
         s" numVertices=${numVertices}" +
         s" minDensity=${minDensity}" +
         s" numSteps=${numSteps}" +
         s" numSubgraphs=${numSubgraphs}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughput=${numSubgraphs / elapsedMs.toDouble}")
   }
}

object QuasiCliquesPA extends Logging {
   val appid: String = "quasi_cliques_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int, minDensity: Double)
   : Unit = {
      val numVertices = explorationSteps + 1
      val fc = fractalGraph.fractalContext

      val ((numSteps,numSubgraphs), elapsedMs) = FractalSparkRunner.time {
         val (numSteps, fractoidsIter) = fractalGraph
            .quasiCliquesPA(numVertices, minDensity)
         var remainingTime = fractalGraph.config.getTotalTimeLimitMs
         var remainingSteps = numSteps
         var numSubgraphsTotal = 0L
         while (fractoidsIter.hasNext) {
            val stepTimeLimitMs = remainingTime / remainingSteps
            val fractoid = fractoidsIter.next().withStepTimeLimit(stepTimeLimitMs)

            val (numSubgraphsTry, elapsedMs) = FractalSparkRunner.time {
               fc.trySubmitFractalStep(fractoid)(
                  f => f.aggregationCount(COUNT_AGG_REPORT))
            }

            val (numSubgraphs, exception) = numSubgraphsTry match {
               case Success(numSubgraphs) => (numSubgraphs, null)
               case Failure(e) => (0L, e)
            }

            numSubgraphsTotal += numSubgraphs
            remainingTime -= elapsedMs
            remainingSteps -= 1

            logApp(s"StepResult fractoid=${fractoid}" +
               s" remainingTimeMs=${remainingTime}" +
               s" remainingSteps=${remainingSteps}" +
               s" exception=${exception}" +
               s" numVertices=${numVertices}" +
               s" minDensity=${minDensity}" +
               s" numSteps=${numSteps}" +
               s" stepTimeLimitMs=${stepTimeLimitMs}" +
               s" numSubgraphs=${numSubgraphs}" +
               s" elapsedMs=${elapsedMs}" +
               s" throughput=${numSubgraphs / elapsedMs.toDouble}")
         }

         (numSteps, numSubgraphsTotal)
      }

      logApp(s"FinalResult" +
         s" numVertices=${numVertices}" +
         s" minDensity=${minDensity}" +
         s" numSteps=${numSteps}" +
         s" numSubgraphs=${numSubgraphs}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughput=${numSubgraphs / elapsedMs.toDouble}")
   }
}

object QuasiCliquesPAPO extends Logging {
   val appid: String = "quasi_cliques_pa_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int, minDensity: Double)
   : Unit = {
      val fc = fractalGraph.fractalContext
      val numVertices = explorationSteps + 1

      val ((numSteps,numSubgraphs), elapsedMs) = FractalSparkRunner.time {
         val (numSteps, fractoidsIter) = fractalGraph
            .quasiCliquesPAPO(numVertices, minDensity)
         var remainingTime = fractalGraph.config.getTotalTimeLimitMs
         var remainingSteps = numSteps
         var numSubgraphsTotal = 0L
         while (fractoidsIter.hasNext) {
            val stepTimeLimitMs = remainingTime / remainingSteps
            val fractoid = fractoidsIter.next().withStepTimeLimit(stepTimeLimitMs)
            val (numSubgraphsTry, elapsedMs) = FractalSparkRunner.time {
               fc.trySubmitFractalStep(fractoid)(
                  f => f.aggregationCount(COUNT_AGG_REPORT))
            }

            val (numSubgraphs, exception) = numSubgraphsTry match {
               case Success(numSubgraphs) => (numSubgraphs, null)
               case Failure(e) => (0L, e)
            }

            numSubgraphsTotal += numSubgraphs
            remainingTime -= elapsedMs
            remainingSteps -= 1

            logApp(s"StepResult fractoid=${fractoid}" +
               s" remainingTimeMs=${remainingTime}" +
               s" remainingSteps=${remainingSteps}" +
               s" exception=${exception}" +
               s" numVertices=${numVertices}" +
               s" minDensity=${minDensity}" +
               s" numSteps=${numSteps}" +
               s" stepTimeLimitMs=${stepTimeLimitMs}" +
               s" numSubgraphs=${numSubgraphs}" +
               s" elapsedMs=${elapsedMs}" +
               s" throughput=${numSubgraphs / elapsedMs.toDouble}")
         }

         (numSteps, numSubgraphsTotal)
      }

      logApp(s"FinalResult" +
         s" numVertices=${numVertices}" +
         s" minDensity=${minDensity}" +
         s" numSteps=${numSteps}" +
         s" numSubgraphs=${numSubgraphs}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughput=${numSubgraphs / elapsedMs.toDouble}")
   }
}

object QuerySpecializationPOEstimate extends Logging {
   val appid: String = "query_specialization_po_estimate"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             patternPath: String, timeLimitMs: Long): Unit = {

      val fc = fractalGraph.fractalContext
      val pattern = PatternUtils.fromFS(patternPath)
      pattern.setInduced(false)
      pattern.setVertexLabeled(true)
      pattern.setEdgeLabeled(false)
      val numSteps = 1

      val (throughputEstimate, elapsedMs) = FractalSparkRunner.time {
         val stepTimeLimitMs = fractalGraph.config.getStepTimeLimitMs
         val fractoid = fractalGraph.querySpecializationPO(pattern)
         val throughputEstimate = fc.estimateThroughput(Seq(fractoid), timeLimitMs, stepTimeLimitMs)
         throughputEstimate
      }

      logApp(s"FinalResult" +
        s" numVertices=${pattern.getNumberOfVertices}" +
        s" patternQuery=${pattern}" +
        s" numSteps=${numSteps}" +
        s" elapsedMs=${elapsedMs}" +
        s" throughputEstimate=${throughputEstimate}")
   }
}

object QuerySpecializationPO extends Logging {
   val appid: String = "query_specialization_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             patternPath: String): Unit = {

      val fc = fractalGraph.fractalContext
      val pattern = PatternUtils.fromFS(patternPath)
      pattern.setInduced(false)
      pattern.setVertexLabeled(true)
      pattern.setEdgeLabeled(false)
      val numSteps = 1
      val timeLimitMs = fractalGraph.config.getTotalTimeLimitMs
      val (numSubgraphs, elapsedMs) = FractalSparkRunner.time {
         val stepTimeLimitMs = timeLimitMs
         val fractoid = fractalGraph.querySpecializationPO(pattern)
            .withStepTimeLimit(stepTimeLimitMs)
         val (numSubgraphsTry, elapsedMs) = FractalSparkRunner.time {
            fc.trySubmitFractalStep(fractoid)(
               f => f.aggregationCount(COUNT_AGG_REPORT))
         }

         val (numSubgraphs, exception) = numSubgraphsTry match {
            case Success(numSubgraphs) => (numSubgraphs, null)
            case Failure(e) => (0L, e)
         }

         logApp(s"StepResult fractoid=${fractoid}" +
            s" exception=${exception}" +
            s" numVertices=${pattern.getNumberOfVertices}" +
            s" patternQuery=${pattern}" +
            s" numSteps=${numSteps}" +
            s" stepTimeLimitMs=${stepTimeLimitMs}" +
            s" numSubgraphs=${numSubgraphs}" +
            s" elapsedMs=${elapsedMs}" +
            s" throughput=${numSubgraphs / elapsedMs.toDouble}")

         numSubgraphs
      }

      logApp(s"FinalResult" +
         s" numVertices=${pattern.getNumberOfVertices}" +
         s" patternQuery=${pattern}" +
         s" numSteps=${numSteps}" +
         s" numSubgraphs=${numSubgraphs}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughput=${numSubgraphs / elapsedMs.toDouble}")
   }
}

object QuerySpecializationPAEstimate extends Logging {
   val appid: String = "query_specialization_pa_estimate"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             patternPath: String, timeLimitMs: Long): Unit = {

      val fc = fractalGraph.fractalContext
      val pattern = PatternUtils.fromFS(patternPath)
      pattern.setInduced(false)
      pattern.setVertexLabeled(true)
      pattern.setEdgeLabeled(false)

      val ((numSteps, throughputEstimate), elapsedMs) = FractalSparkRunner.time {
         val (numSteps, fractoidsIter) = fractalGraph
           .querySpecializationPA(pattern)
         val stepTimeLimitMs = fractalGraph.config.getStepTimeLimitMs
         val throughputEstimate = fc.estimateThroughput(fractoidsIter.toSeq, timeLimitMs, stepTimeLimitMs)
         (numSteps, throughputEstimate)
      }

      logApp(s"FinalResult" +
        s" numVertices=${pattern.getNumberOfVertices}" +
        s" patternQuery=${pattern}" +
        s" numSteps=${numSteps}" +
        s" elapsedMs=${elapsedMs}" +
        s" throughputEstimate=${throughputEstimate}"
      )
   }
}

object QuerySpecializationPA extends Logging {
   val appid: String = "query_specialization_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             patternPath: String): Unit = {

      val fc = fractalGraph.fractalContext
      val pattern = PatternUtils.fromFS(patternPath)
      pattern.setInduced(false)
      pattern.setVertexLabeled(true)
      pattern.setEdgeLabeled(false)

      val ((numSteps,numSubgraphs), elapsedMs) = FractalSparkRunner.time {
         val (numSteps, fractoidsIter) = fractalGraph
            .querySpecializationPA(pattern)
         var remainingTime = fractalGraph.config.getTotalTimeLimitMs
         var remainingSteps = numSteps
         var numSubgraphsTotal = 0L
         while (fractoidsIter.hasNext) {
            val stepTimeLimitMs = remainingTime / remainingSteps
            val fractoid = fractoidsIter.next().withStepTimeLimit(stepTimeLimitMs)

            val (numSubgraphsTry, elapsedMs) = FractalSparkRunner.time {
               fc.trySubmitFractalStep(fractoid)(
                  f => f.aggregationCount(COUNT_AGG_REPORT))
            }

            val (numSubgraphs, exception) = numSubgraphsTry match {
               case Success(numSubgraphs) => (numSubgraphs, null)
               case Failure(e) => (0L, e)
            }

            numSubgraphsTotal += numSubgraphs
            remainingTime -= elapsedMs
            remainingSteps -= 1

            logApp(s"StepResult fractoid=${fractoid}" +
               s" remainingTimeMs=${remainingTime}" +
               s" remainingSteps=${remainingSteps}" +
               s" exception=${exception}" +
               s" numVertices=${pattern.getNumberOfVertices}" +
               s" patternQuery=${pattern}" +
               s" numSteps=${numSteps}" +
               s" stepTimeLimitMs=${stepTimeLimitMs}" +
               s" numSubgraphs=${numSubgraphs}" +
               s" elapsedMs=${elapsedMs}" +
               s" throughput=${numSubgraphs / elapsedMs.toDouble}")
         }

         (numSteps, numSubgraphsTotal)
      }

      logApp(s"FinalResult" +
         s" numVertices=${pattern.getNumberOfVertices}" +
         s" patternQuery=${pattern}" +
         s" numSteps=${numSteps}" +
         s" numSubgraphs=${numSubgraphs}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughput=${numSubgraphs / elapsedMs.toDouble}")
   }
}

object QuerySpecializationPAPOEstimate extends Logging {
   val appid: String = "query_specialization_pa_po_estimate"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             patternPath: String, timeLimitMs: Long): Unit = {

      val fc = fractalGraph.fractalContext
      val pattern = PatternUtils.fromFS(patternPath)
      pattern.setInduced(false)
      pattern.setVertexLabeled(true)
      pattern.setEdgeLabeled(false)
      val numSteps = 1

      val (throughputEstimate, elapsedMs) = FractalSparkRunner.time {
         val stepTimeLimitMs = fractalGraph.config.getStepTimeLimitMs
         val fractoid = fractalGraph.querySpecializationPAPO(pattern)
         val throughputEstimate = fc.estimateThroughput(Seq(fractoid), timeLimitMs, stepTimeLimitMs)
         throughputEstimate
      }

      logApp(s"FinalResult" +
        s" numVertices=${pattern.getNumberOfVertices}" +
        s" patternQuery=${pattern}" +
        s" numSteps=${numSteps}" +
        s" elapsedMs=${elapsedMs}" +
        s" throughputEstimate=${throughputEstimate}")
   }
}

object QuerySpecializationPAPO extends Logging {
   val appid: String = "query_specialization_pa_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             patternPath: String): Unit = {

      val fc = fractalGraph.fractalContext
      val pattern = PatternUtils.fromFS(patternPath)
      pattern.setInduced(false)
      pattern.setVertexLabeled(true)
      pattern.setEdgeLabeled(false)
      val numSteps = 1
      val timeLimitMs = fractalGraph.config.getTotalTimeLimitMs
      val (numSubgraphs, elapsedMs) = FractalSparkRunner.time {
         val stepTimeLimitMs = timeLimitMs
         val fractoid = fractalGraph.querySpecializationPAPO(pattern)
            .withStepTimeLimit(stepTimeLimitMs)
         val (numSubgraphsTry, elapsedMs) = FractalSparkRunner.time {
            fc.trySubmitFractalStep(fractoid)(
               f => f.aggregationCount(COUNT_AGG_REPORT))
         }

         val (numSubgraphs, exception) = numSubgraphsTry match {
            case Success(numSubgraphs) => (numSubgraphs, null)
            case Failure(e) => (0L, e)
         }

         logApp(s"StepResult fractoid=${fractoid}" +
            s" exception=${exception}" +
            s" numVertices=${pattern.getNumberOfVertices}" +
            s" patternQuery=${pattern}" +
            s" numSteps=${numSteps}" +
            s" stepTimeLimitMs=${stepTimeLimitMs}" +
            s" numSubgraphs=${numSubgraphs}" +
            s" elapsedMs=${elapsedMs}" +
            s" throughput=${numSubgraphs / elapsedMs.toDouble}")

         numSubgraphs
      }

      logApp(s"FinalResult" +
         s" numVertices=${pattern.getNumberOfVertices}" +
         s" patternQuery=${pattern}" +
         s" numSteps=${numSteps}" +
         s" numSubgraphs=${numSubgraphs}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughput=${numSubgraphs / elapsedMs.toDouble}")
   }
}

object FSMPO extends Logging {
   val appid: String = "fsm_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int, minSupport: Int)
   : Unit = {

      val numEdges = explorationSteps + 1

      // we create the fractoid inside the timer because there is actual work
      // being done to obtain the RDD
      val ((patternsSupportsRDD,numPatterns), elapsedMs) =
      FractalSparkRunner.time {
         val patternsSupportsRDD = fractalGraph.fsmPO(minSupport, numEdges)
         patternsSupportsRDD.cache()
         val numPatterns = patternsSupportsRDD.count() // materialize
         (patternsSupportsRDD, numPatterns)
      }

      val numSubgraphs = patternsSupportsRDD.values
         .map(_.getNumSubgraphsAggregated)
         .fold(0)(_ + _)
      patternsSupportsRDD.unpersist()

      logApp(s"FinalResult" +
         s" numEdges=${numEdges}" +
         s" minSupport=${minSupport}" +
         s" numSteps=${numEdges}" +
         s" numSubgraphs=${numSubgraphs}" +
         s" numPatterns=${numPatterns}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughput=${numSubgraphs / elapsedMs.toDouble}")
   }
}

object FSMPA extends Logging {
   val appid: String = "fsm_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int, minSupport: Int)
   : Unit = {

      val numEdges = explorationSteps + 1

      // we create the fractoid inside the timer because there is actual work
      // being done to obtain the RDD
      val ((patternsSupportsRDD,numPatterns), elapsedMs) = FractalSparkRunner.time {
         val patternsSupportsRDD = fractalGraph
            .fsmPA(minSupport, numEdges)
         patternsSupportsRDD.cache()
         val numPatterns = patternsSupportsRDD.count() // materialize
         (patternsSupportsRDD, numPatterns)
      }

      val numSubgraphs = patternsSupportsRDD.values
         .map(_.getNumSubgraphsAggregated)
         .fold(0)(_ + _)
      patternsSupportsRDD.unpersist()

      logApp(s"FinalResult" +
         s" numEdges=${numEdges}" +
         s" minSupport=${minSupport}" +
         s" numSteps=${numEdges}" +
         s" numSubgraphs=${numSubgraphs}" +
         s" numPatterns=${numPatterns}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughput=${numSubgraphs / elapsedMs.toDouble}")
   }
}

object FSMPAMCVC extends Logging {
   val appid: String = "fsm_pa_mcvc"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int, minSupport: Int)
   : Unit = {

      // we create the fractoid inside the timer because there is actual work
      // being done to obtain the RDD
      val ((patternsSupportsRDD,numPatterns), elapsed) = FractalSparkRunner.time {
         val patternsSupportsRDD = fractalGraph
            .fsmPAMCVC(minSupport, explorationSteps + 1)
         patternsSupportsRDD.cache()
         val numPatterns = patternsSupportsRDD.count() // materialize
         (patternsSupportsRDD, numPatterns)
      }

      val numSubgraphs = patternsSupportsRDD.values
         .map(_.getNumSubgraphsAggregated)
         .reduce(_ + _)

      logApp(s"numSubgraphs=${numSubgraphs}" +
         s" numPatterns=${numPatterns} elapsed=${elapsed}")

      val iter = patternsSupportsRDD.toLocalIterator
      while (iter.hasNext) {
         val (pattern, support) = iter.next()
         logApp(s"PatternSupport ${pattern} ${support}")
      }

      patternsSupportsRDD.unpersist()
   }
}

object FSMPAPO extends Logging {
   val appid: String = "fsm_pa_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int, minSupport: Int)
   : Unit = {

      val numEdges = explorationSteps + 1

      // we create the fractoid inside the timer because there is actual work
      // being done to obtain the RDD
      val ((patternsSupportsRDD,numPatterns), elapsedMs) = FractalSparkRunner.time {
         val patternsSupportsRDD = fractalGraph.fsmPAPO(minSupport, numEdges)
         patternsSupportsRDD.cache()
         val numPatterns = patternsSupportsRDD.count() // materialize
         (patternsSupportsRDD, numPatterns)
      }

      val numSubgraphs = patternsSupportsRDD.values
         .map(_.getNumSubgraphsAggregated)
         .fold(0)(_ + _)
      patternsSupportsRDD.unpersist()

      logApp(s"FinalResult" +
         s" numEdges=${numEdges}" +
         s" minSupport=${minSupport}" +
         s" numSteps=${numEdges}" +
         s" numSubgraphs=${numSubgraphs}" +
         s" numPatterns=${numPatterns}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughput=${numSubgraphs / elapsedMs.toDouble}")
   }
}

object PatternQueryingInducedPAMCVC extends Logging {
   val appid: String = "pattern_querying_induced_pa_mcvc"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             subgraphPath: String): Unit = {

      val pattern = PatternUtils.fromFS(subgraphPath)
      pattern.setInduced(true)

      val fracs = fractalGraph.patternQueryingPAMCVC(pattern)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         var numSubgraphs = 0L
         for (frac <- fracs) {
            numSubgraphs += frac.aggregationCount
         }
         numSubgraphs
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}



object PatternQueryingSamplePA extends Logging {
   val appid: String = "pattern_querying_sample_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             subgraphPath: String, fraction: Double): Unit = {

      val pattern = PatternUtils.fromFS(subgraphPath)

      val frac = fractalGraph.patternQueryingPA(pattern, fraction)
         .explore(pattern.getNumberOfVertices - 1)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount
      }

      logApp(s"numSubgraphs=${numSubgraphs}" +
         s" numSubgraphsEstimated=${numSubgraphs / fraction}" +
         s" elapsed=${elapsed}")
   }
}

object PatternQueryingInducedSamplePA extends Logging {
   val appid: String = "pattern_querying_induced_sample_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             subgraphPath: String, fraction: Double): Unit = {

      val pattern = PatternUtils.fromFS(subgraphPath)
      pattern.setInduced(true)

      val frac = fractalGraph.patternQueryingPA(pattern, fraction)
         .explore(pattern.getNumberOfVertices - 1)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount
      }

      logApp(s"numSubgraphs=${numSubgraphs}" +
         s" numSubgraphsEstimated=${numSubgraphs / fraction}" +
         s" elapsed=${elapsed}")
   }
}

object PatternQueryingPO extends Logging {
   val appid: String = "pattern_querying_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             subgraphPath: String, plabeling: String)
   : Unit = {

      val fc = fractalGraph.fractalContext

      val pattern = plabeling match {
         case "n" =>
            val pattern = PatternUtils.fromFS(subgraphPath, false, false)
            pattern.setVertexLabeled(false)
            pattern.setEdgeLabeled(false)
            pattern
         case "v" =>
            val pattern = PatternUtils.fromFS(subgraphPath, true, false)
            pattern.setVertexLabeled(true)
            pattern.setEdgeLabeled(false)
            pattern
         case "e" =>
            val pattern = PatternUtils.fromFS(subgraphPath, false, true)
            pattern.setVertexLabeled(false)
            pattern.setEdgeLabeled(true)
            pattern
         case "ve" =>
            val pattern = PatternUtils.fromFS(subgraphPath, true, true)
            pattern.setVertexLabeled(true)
            pattern.setEdgeLabeled(true)
            pattern
         case other =>
            logError(s"Invalid pattern labeling: ${plabeling}")
            return
      }

      val numSteps = 1
      val timeLimitMs = fractalGraph.config.getTotalTimeLimitMs
      val (numSubgraphs, elapsedMs) = FractalSparkRunner.time {
         val stepTimeLimitMs = timeLimitMs
         val fractoid = fractalGraph.patternQueryingPO(pattern)
            .explore(pattern.getNumberOfEdges - 1)
            .withStepTimeLimit(stepTimeLimitMs)
         val (numSubgraphsTry, elapsedMs) = FractalSparkRunner.time {
            fc.trySubmitFractalStep(fractoid)(
               f => f.aggregationCount(COUNT_AGG_REPORT))
         }

         val (numSubgraphs, exception) = numSubgraphsTry match {
            case Success(numSubgraphs) => (numSubgraphs, null)
            case Failure(e) => (0L, e)
         }

         logApp(s"StepResult fractoid=${fractoid}" +
            s" exception=${exception}" +
            s" numVertices=${pattern.getNumberOfVertices}" +
            s" query=${pattern}" +
            s" numSteps=${numSteps}" +
            s" stepTimeLimitMs=${stepTimeLimitMs}" +
            s" numSubgraphs=${numSubgraphs}" +
            s" elapsedMs=${elapsedMs}" +
            s" throughput=${numSubgraphs / elapsedMs.toDouble}")

         numSubgraphs
      }

      logApp(s"FinalResult" +
         s" numVertices=${pattern.getNumberOfVertices}" +
         s" query=${pattern}" +
         s" numSteps=${numSteps}" +
         s" numSubgraphs=${numSubgraphs}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughput=${numSubgraphs / elapsedMs.toDouble}")
   }
}

object PatternQueryingPA extends Logging {
   val appid: String = "pattern_querying_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             subgraphPath: String, plabeling: String): Unit = {
      val fc = fractalGraph.fractalContext

      val pattern = plabeling match {
         case "n" =>
            val pattern = PatternUtils.fromFS(subgraphPath, false, false)
            pattern.setVertexLabeled(false)
            pattern.setEdgeLabeled(false)
            pattern
         case "v" =>
            val pattern = PatternUtils.fromFS(subgraphPath, true, false)
            pattern.setVertexLabeled(true)
            pattern.setEdgeLabeled(false)
            pattern
         case "e" =>
            val pattern = PatternUtils.fromFS(subgraphPath, false, true)
            pattern.setVertexLabeled(false)
            pattern.setEdgeLabeled(true)
            pattern
         case "ve" =>
            val pattern = PatternUtils.fromFS(subgraphPath, true, true)
            pattern.setVertexLabeled(true)
            pattern.setEdgeLabeled(true)
            pattern
         case other =>
            logError(s"Invalid pattern labeling: ${plabeling}")
            return
      }

      val numSteps = 1
      val timeLimitMs = fractalGraph.config.getTotalTimeLimitMs
      val (numSubgraphs, elapsedMs) = FractalSparkRunner.time {
         val stepTimeLimitMs = timeLimitMs
         val fractoid = fractalGraph.patternQueryingPA(pattern)
            .explore(pattern.getNumberOfVertices - 1)
            .withStepTimeLimit(stepTimeLimitMs)
         val (numSubgraphsTry, elapsedMs) = FractalSparkRunner.time {
            fc.trySubmitFractalStep(fractoid)(
               f => f.aggregationCount(COUNT_AGG_REPORT))
         }

         val (numSubgraphs, exception) = numSubgraphsTry match {
            case Success(numSubgraphs) => (numSubgraphs, null)
            case Failure(e) => (0L, e)
         }

         logApp(s"StepResult fractoid=${fractoid}" +
            s" exception=${exception}" +
            s" numVertices=${pattern.getNumberOfVertices}" +
            s" query=${pattern}" +
            s" numSteps=${numSteps}" +
            s" stepTimeLimitMs=${stepTimeLimitMs}" +
            s" numSubgraphs=${numSubgraphs}" +
            s" elapsedMs=${elapsedMs}" +
            s" throughput=${numSubgraphs / elapsedMs.toDouble}")

         numSubgraphs
      }

      logApp(s"FinalResult" +
         s" numVertices=${pattern.getNumberOfVertices}" +
         s" query=${pattern}" +
         s" numSteps=${numSteps}" +
         s" numSubgraphs=${numSubgraphs}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughput=${numSubgraphs / elapsedMs.toDouble}")
   }
}

object PatternQueryingPAMCVC extends Logging {
   val appid: String = "pattern_querying_pa_mcvc"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             subgraphPath: String, plabeling: String): Unit = {
      val fc = fractalGraph.fractalContext

      val pattern = plabeling match {
         case "n" =>
            val pattern = PatternUtils.fromFS(subgraphPath, false, false)
            pattern.setVertexLabeled(false)
            pattern.setEdgeLabeled(false)
            pattern
         case "v" =>
            val pattern = PatternUtils.fromFS(subgraphPath, true, false)
            pattern.setVertexLabeled(true)
            pattern.setEdgeLabeled(false)
            pattern
         case "e" =>
            val pattern = PatternUtils.fromFS(subgraphPath, false, true)
            pattern.setVertexLabeled(false)
            pattern.setEdgeLabeled(true)
            pattern
         case "ve" =>
            val pattern = PatternUtils.fromFS(subgraphPath, true, true)
            pattern.setVertexLabeled(true)
            pattern.setEdgeLabeled(true)
            pattern
         case other =>
            logError(s"Invalid pattern labeling: ${plabeling}")
            return
      }

      val fracs = fractalGraph.patternQueryingPAMCVC(pattern)

      val (results, elapsed) = FractalSparkRunner.time {
         fc.trySubmitFractalSteps(fracs.toIndexedSeq)(
            f => f.aggregationCount(COUNT_AGG_REPORT))
      }

      val failure = results.exists(_.isFailure)
      val numSubgraphs = results.filter(_.isSuccess).map(_.get).sum

      logApp(s"failure=${failure} numSubgraphs=${numSubgraphs} " +
         s"elapsed=${elapsed}")
   }
}

object PatternQueryingPAMCVCOld extends Logging {
   val appid: String = "pattern_querying_pa_mcvc_old"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             subgraphPath: String, plabeling: String): Unit = {

      val fc = fractalGraph.fractalContext

      val pattern = plabeling match {
         case "n" =>
            val pattern = PatternUtils.fromFS(subgraphPath, false, false)
            pattern.setVertexLabeled(false)
            pattern.setEdgeLabeled(false)
            pattern
         case "v" =>
            val pattern = PatternUtils.fromFS(subgraphPath, true, false)
            pattern.setVertexLabeled(true)
            pattern.setEdgeLabeled(false)
            pattern
         case "e" =>
            val pattern = PatternUtils.fromFS(subgraphPath, false, true)
            pattern.setVertexLabeled(false)
            pattern.setEdgeLabeled(true)
            pattern
         case "ve" =>
            val pattern = PatternUtils.fromFS(subgraphPath, true, true)
            pattern.setVertexLabeled(true)
            pattern.setEdgeLabeled(true)
            pattern
         case other =>
            logError(s"Invalid pattern labeling: ${plabeling}")
            return
      }

      val ((numSteps, numSubgraphs), elapsed) = FractalSparkRunner.time {
         val (numSteps, fractoidsIter) = fractalGraph.patternQueryingPAMCVC_old(pattern)
         var remainingTime = fractalGraph.config.getTotalTimeLimitMs
         var remainingSteps = numSteps
         var numSubgraphsTotal = 0L
         while (fractoidsIter.hasNext) {
            val stepTimeLimitMs = remainingTime / remainingSteps
            val fractoid = fractoidsIter.next().withStepTimeLimit(stepTimeLimitMs)
            val (numSubgraphsTry, elapsedMs) = FractalSparkRunner.time {
               fc.trySubmitFractalStep(fractoid)(
                  f => f.aggregationCountWithCallback(
                     (s,c,cb) => s.completeMatch(c, c.getPattern, cb),
                     COUNT_AGG_REPORT))
            }

            val (numSubgraphs, exception) = numSubgraphsTry match {
               case Success(numSubgraphs) => (numSubgraphs, null)
               case Failure(e) => (0L, e)
            }

            numSubgraphsTotal += numSubgraphs
            remainingTime -= elapsedMs
            remainingSteps -= 1

            logApp(s"StepResult fractoid=${fractoid}" +
               s" remainingTimeMs=${remainingTime}" +
               s" remainingSteps=${remainingSteps}" +
               s" exception=${exception}" +
               s" numVertices=${pattern.getNumberOfVertices}" +
               s" query=${pattern}" +
               s" explorationPlan=${fractoid.pattern.explorationPlan()}" +
               s" numSteps=${numSteps}" +
               s" stepTimeLimitMs=${stepTimeLimitMs}" +
               s" numSubgraphs=${numSubgraphs}" +
               s" elapsedMs=${elapsedMs}" +
               s" throughput=${numSubgraphs / elapsedMs.toDouble}")
         }

         (numSteps, numSubgraphsTotal)
      }

      logApp(s"FinalResult" +
         s" numVertices=${pattern.getNumberOfVertices}" +
         s" query=${pattern}" +
         s" numSteps=${numSteps}" +
         s" numSubgraphs=${numSubgraphs}" +
         s" elapsedMs=${elapsed}" +
         s" throughput=${numSubgraphs / elapsed.toDouble}")
   }
}

object PatternQueryingInducedPA extends Logging {
   val appid: String = "pattern_querying_induced_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int, subgraphPath: String)
   : Unit = {
      val pattern = PatternUtils.fromFS(subgraphPath)
      pattern.setInduced(true)

      val frac = fractalGraph.patternQueryingPA(pattern)
         .explore(pattern.getNumberOfVertices - 1)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object PeriodicSubgraphsInducedPA extends Logging {
   val appid: String = "periodic_subgraphs_induced_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             periodicThreshold: Int): Unit = {

      val ec = ExecutionContext.global

      var numSubgraphs = 0L
      val callback: (Pattern, Fractoid[PatternInducedSubgraph]) => Unit =
         (pattern, frac) => {
            numSubgraphs += frac.aggregationCount
         }

      val (_, elapsed) = FractalSparkRunner.time {
         fractalGraph.periodicInducedSubgraphsPA(
            periodicThreshold, explorationSteps + 1, callback)
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object PeriodicSubgraphsInducedPAMCVC extends Logging {
   val appid: String = "periodic_subgraphs_induced_pa_mcvc"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             periodicThreshold: Int): Unit = {
      var numSubgraphs = 0L
      val callback = (app: InducedPeriodicSubgraphsPAMCVC,
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
         fractalGraph.periodicInducedSubgraphsPAMCVC(
            periodicThreshold, explorationSteps + 1, callback)
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object PeriodicSubgraphsInducedPO extends Logging {
   val appid: String = "periodic_subgraphs_induced_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             periodicThreshold: Int): Unit = {
      val frac = fractalGraph.periodicInducedSubgraphsPO(periodicThreshold)
         .explore(explorationSteps)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount
      }

      logApp(s" numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}
object LabelSearchPA extends Logging {
   val appid: String = "label_search_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             labelsSet: Set[Int], gfiltering: Boolean): Unit = {

      val fc = fractalGraph.fractalContext
      val numVertices = explorationSteps + 1

      val fg = if (gfiltering) {
         fractalGraph.filterEdges(
            (u,uLabels,v,vLabels,e,eLabels) => {
               labelsSet.contains(uLabels.getu(0)) &&
                 labelsSet.contains(vLabels.getu(0))
            })
      } else {
         fractalGraph
      }


      val ((numSteps, numSubgraphs), elapsed) = FractalSparkRunner.time {
         val (numSteps, fractoidsIter) = fg.labelSearchPA(labelsSet, numVertices)
         var remainingTime = fractalGraph.config.getTotalTimeLimitMs
         var remainingSteps = numSteps
         var numSubgraphsTotal = 0L
         while (fractoidsIter.hasNext) {
            val stepTimeLimitMs = remainingTime / remainingSteps
            val fractoid = fractoidsIter.next().withStepTimeLimit(stepTimeLimitMs)
            //val stepTimeLimitMs = fractoid.config.getStepTimeLimitMs
            val (numSubgraphsTry, elapsedMs) = FractalSparkRunner.time {
               fc.trySubmitFractalStep(fractoid)(
                  f => f.aggregationCount(COUNT_AGG_REPORT))
            }

            val (numSubgraphs, exception) = numSubgraphsTry match {
               case Success(numSubgraphs) => (numSubgraphs, null)
               case Failure(e) => (0L, e)
            }

            numSubgraphsTotal += numSubgraphs
            remainingTime -= elapsedMs
            remainingSteps -= 1

            logApp(s"StepResult fractoid=${fractoid}" +
              s" remainingTimeMs=${remainingTime}" +
              s" remainingSteps=${remainingSteps}" +
              s" exception=${exception}" +
              s" numVertices=${numVertices}" +
              s" labelsSet=${labelsSet.toArray.sorted.mkString(",")}" +
              s" gfiltering=${gfiltering}" +
              s" numSteps=${numSteps}" +
              s" stepTimeLimitMs=${stepTimeLimitMs}" +
              s" numSubgraphs=${numSubgraphs}" +
              s" elapsedMs=${elapsedMs}" +
              s" throughput=${numSubgraphs / elapsedMs.toDouble}")
         }

         (numSteps, numSubgraphsTotal)
      }

      logApp(s"FinalResult" +
        s" numVertices=${numVertices}" +
        s" labelsSet=${labelsSet.toArray.sorted.mkString(",")}" +
        s" gfiltering=${gfiltering}" +
        s" numSteps=${numSteps}" +
        s" numSubgraphs=${numSubgraphs}" +
        s" elapsedMs=${elapsed}" +
        s" throughput=${numSubgraphs / elapsed.toDouble}")
   }
}

object LabelSearchPAEstimate extends Logging {
   val appid: String = "label_search_pa_estimate"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             labelsSet: Set[Int], gfiltering: Boolean, timeBudgetMs: Long): Unit = {

      val fc = fractalGraph.fractalContext
      val numVertices = explorationSteps + 1

      val fg = if (gfiltering) {
         fractalGraph.filterEdges(
            (u,uLabels,v,vLabels,e,eLabels) => {
               labelsSet.contains(uLabels.getu(0)) &&
                  labelsSet.contains(vLabels.getu(0))
            })
      } else {
         fractalGraph
      }


      val ((numSteps, throughputEstimate), elapsedMs) = FractalSparkRunner.time {
         val (numSteps, fractoidsIter) = fg.labelSearchPA(labelsSet, numVertices)
         val stepTimeLimitMs = fractalGraph.config.getStepTimeLimitMs
         val throughputEstimate = fc.estimateThroughput(fractoidsIter.toSeq, timeBudgetMs, stepTimeLimitMs)
         (numSteps, throughputEstimate)
      }

      logApp(s"FinalResult" +
         s" numVertices=${numVertices}" +
         s" labelsSet=${labelsSet.toArray.sorted.mkString(",")}" +
         s" gfiltering=${gfiltering}" +
         s" numSteps=${numSteps}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughputEstimate=${throughputEstimate}")
   }
}
object LabelSearchPO extends Logging {
   val appid: String = "label_search_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             labelsSet: Set[Int], gfiltering: Boolean): Unit = {

      val fc = fractalGraph.fractalContext
      val numVertices = explorationSteps + 1

      val fg = if (gfiltering) {
         fractalGraph.filterEdges(
            (u,uLabels,v,vLabels,e,eLabels) => {
               labelsSet.contains(uLabels.getu(0)) &&
                 labelsSet.contains(vLabels.getu(0))
            })
      } else {
         fractalGraph
      }

      val numSteps = 1
      val timeLimitMs = fg.config.getTotalTimeLimitMs
      val (numSubgraphs, elapsedMs) = FractalSparkRunner.time {
         val stepTimeLimitMs = timeLimitMs
         val fractoid = fg.labelSearchPO(labelsSet, numVertices)
           .withStepTimeLimit(stepTimeLimitMs)
         val (numSubgraphsTry, elapsedMs) = FractalSparkRunner.time {
            fc.trySubmitFractalStep(fractoid)(
               f => f.aggregationCount(COUNT_AGG_REPORT))
         }

         val (numSubgraphs, exception) = numSubgraphsTry match {
            case Success(numSubgraphs) => (numSubgraphs, null)
            case Failure(e) => (0L, e)
         }

         logApp(s"StepResult fractoid=${fractoid}" +
           s" exception=${exception}" +
           s" numVertices=${numVertices}" +
           s" labelsSet=${labelsSet.toArray.sorted.mkString(",")}" +
           s" gfiltering=${gfiltering}" +
           s" numSteps=${numSteps}" +
           s" stepTimeLimitMs=${stepTimeLimitMs}" +
           s" numSubgraphs=${numSubgraphs}" +
           s" elapsedMs=${elapsedMs}" +
           s" throughput=${numSubgraphs / elapsedMs.toDouble}")

         numSubgraphs
      }

      logApp(s"FinalResult" +
        s" numVertices=${numVertices}" +
        s" labelsSet=${labelsSet.toArray.sorted.mkString(",")}" +
        s" gfiltering=${gfiltering}" +
        s" numSteps=${numSteps}" +
        s" numSubgraphs=${numSubgraphs}" +
        s" elapsedMs=${elapsedMs}" +
        s" throughput=${numSubgraphs / elapsedMs.toDouble}")
   }
}

object LabelSearchPOEstimate extends Logging {
   val appid: String = "label_search_po_estimate"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             labelsSet: Set[Int], gfiltering: Boolean, timeBudgetMs: Long): Unit = {

      val fc = fractalGraph.fractalContext
      val numVertices = explorationSteps + 1

      val fg = if (gfiltering) {
         fractalGraph.filterEdges(
            (u,uLabels,v,vLabels,e,eLabels) => {
               labelsSet.contains(uLabels.getu(0)) &&
                  labelsSet.contains(vLabels.getu(0))
            })
      } else {
         fractalGraph
      }

      val numSteps = 1
      val (throughputEstimate, elapsedMs) = FractalSparkRunner.time {
         val stepTimeLimitMs = fg.config.getStepTimeLimitMs
         val fractoid = fg.labelSearchPO(labelsSet, numVertices)
         fc.estimateThroughput(Seq(fractoid), timeBudgetMs, stepTimeLimitMs)
      }

      logApp(s"FinalResult" +
         s" numVertices=${numVertices}" +
         s" labelsSet=${labelsSet.toArray.sorted.mkString(",")}" +
         s" gfiltering=${gfiltering}" +
         s" numSteps=${numSteps}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughputEstimate=${throughputEstimate}")
   }
}

object KeywordSearchPO extends Logging {
   val appid: String = "keyword_search_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             keywords: Set[Int], gfiltering: Boolean): Unit = {

      val fg = if (gfiltering) {
         fractalGraph.filterEdges(
            (u,uLabels,v,vLabels,e,eLabels) => {
               keywords.contains(uLabels.get(0)) ||
                  keywords.contains(vLabels.get(0))
            })
      } else {
         fractalGraph
      }

      val frac = fg.keywordSearchPO(keywords, explorationSteps + 1)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount(COUNT_AGG_REPORT)
      }

      logApp(s"labelsSet=${keywords} gfiltering=${gfiltering}" +
         s" numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object MinimalKeywordSearchPO extends Logging {
   val appid: String = "minimal_keyword_search_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             keywords: Set[Int], gfiltering: Boolean): Unit = {

      val fc = fractalGraph.fractalContext
      val numVertices = explorationSteps + 1

      val fg = if (gfiltering) {
         fractalGraph.filterEdges(
            (u,uLabels,v,vLabels,e,eLabels) => {
               val uLabel = uLabels.getu(0)
               val vLabel = vLabels.getu(0)
               uLabel != vLabel || keywords.contains(uLabel)
            })
      } else {
         fractalGraph
      }

      val numSteps = 1
      val timeLimitMs = fg.config.getTotalTimeLimitMs
      val (numSubgraphs, elapsedMs) = FractalSparkRunner.time {
         val stepTimeLimitMs = timeLimitMs
         val fractoid = fg.minimalKeywordSearchPO(keywords, numVertices)
            .withStepTimeLimit(stepTimeLimitMs)
         val (numSubgraphsTry, elapsedMs) = FractalSparkRunner.time {
            fc.trySubmitFractalStep(fractoid)(
               f => f.aggregationCount(COUNT_AGG_REPORT))
         }

         val (numSubgraphs, exception) = numSubgraphsTry match {
            case Success(numSubgraphs) => (numSubgraphs, null)
            case Failure(e) => (0L, e)
         }

         logApp(s"StepResult fractoid=${fractoid}" +
            s" exception=${exception}" +
            s" numVertices=${numVertices}" +
            s" keywords=${keywords.toArray.sorted.mkString(",")}" +
            s" gfiltering=${gfiltering}" +
            s" numSteps=${numSteps}" +
            s" stepTimeLimitMs=${stepTimeLimitMs}" +
            s" numSubgraphs=${numSubgraphs}" +
            s" elapsedMs=${elapsedMs}" +
            s" throughput=${numSubgraphs / elapsedMs.toDouble}")

         numSubgraphs
      }

      logApp(s"FinalResult" +
         s" numVertices=${numVertices}" +
         s" keywords=${keywords.toArray.sorted.mkString(",")}" +
         s" gfiltering=${gfiltering}" +
         s" numSteps=${numSteps}" +
         s" numSubgraphs=${numSubgraphs}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughput=${numSubgraphs / elapsedMs.toDouble}")
   }
}

object MinimalKeywordSearchPOEstimate extends Logging {
   val appid: String = "minimal_keyword_search_po_estimate"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             keywords: Set[Int], gfiltering: Boolean, timeBudgetMs: Long): Unit = {

      val fc = fractalGraph.fractalContext
      val numVertices = explorationSteps + 1

      val fg = if (gfiltering) {
         fractalGraph.filterEdges(
            (u,uLabels,v,vLabels,e,eLabels) => {
               val uLabel = uLabels.getu(0)
               val vLabel = vLabels.getu(0)
               uLabel != vLabel || keywords.contains(uLabel)
            })
      } else {
         fractalGraph
      }

      val ((numSteps, throughputEstimate), elapsedMs) = FractalSparkRunner.time {
         val numSteps = 1
         val fractoid = fg.minimalKeywordSearchPO(keywords, numVertices)
         val stepTimeLimitMs = fractalGraph.config.getStepTimeLimitMs
         val throughputEstimate = fc.estimateThroughput(Seq(fractoid),
            timeBudgetMs, stepTimeLimitMs)
         (numSteps, throughputEstimate)
      }

      logApp(s"FinalResult" +
         s" numVertices=${numVertices}" +
         s" keywords=${keywords.toArray.sorted.mkString(",")}" +
         s" gfiltering=${gfiltering}" +
         s" numSteps=${numSteps}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughputEstimate=${throughputEstimate}")
   }
}

object MinimalKeywordSearchPA extends Logging {
   val appid: String = "minimal_keyword_search_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             keywords: Set[Int], gfiltering: Boolean): Unit = {

      val fc = fractalGraph.fractalContext
      val numVertices = explorationSteps + 1

      val fg = if (gfiltering) {
         fractalGraph.filterEdges(
            (u,uLabels,v,vLabels,e,eLabels) => {
               val uLabel = uLabels.getu(0)
               val vLabel = vLabels.getu(0)
               uLabel != vLabel || keywords.contains(uLabel)
            })
      } else {
         fractalGraph
      }

      val ((numSteps, numSubgraphs), elapsed) = FractalSparkRunner.time {
         val (numSteps, fractoidsIter) = fg.minimalKeywordSearchPA(keywords, numVertices)
         var remainingTime = fractalGraph.config.getTotalTimeLimitMs
         var remainingSteps = numSteps
         var numSubgraphsTotal = 0L
         while (fractoidsIter.hasNext) {
            val stepTimeLimitMs = remainingTime / remainingSteps
            val fractoid = fractoidsIter.next().withStepTimeLimit(stepTimeLimitMs)
            //val stepTimeLimitMs = fractoid.config.getStepTimeLimitMs
            val (numSubgraphsTry, elapsedMs) = FractalSparkRunner.time {
               fc.trySubmitFractalStep(fractoid)(
                  f => f.aggregationCount(COUNT_AGG_REPORT))
            }

            val (numSubgraphs, exception) = numSubgraphsTry match {
               case Success(numSubgraphs) => (numSubgraphs, null)
               case Failure(e) => (0L, e)
            }

            numSubgraphsTotal += numSubgraphs
            remainingTime -= elapsedMs
            remainingSteps -= 1

            logApp(s"StepResult fractoid=${fractoid}" +
               s" remainingTimeMs=${remainingTime}" +
               s" remainingSteps=${remainingSteps}" +
               s" exception=${exception}" +
               s" numVertices=${numVertices}" +
               s" keywords=${keywords.toArray.sorted.mkString(",")}" +
               s" gfiltering=${gfiltering}" +
               s" numSteps=${numSteps}" +
               s" stepTimeLimitMs=${stepTimeLimitMs}" +
               s" numSubgraphs=${numSubgraphs}" +
               s" elapsedMs=${elapsedMs}" +
               s" throughput=${numSubgraphs / elapsedMs.toDouble}")
         }

         (numSteps, numSubgraphsTotal)
      }

      logApp(s"FinalResult" +
         s" numVertices=${numVertices}" +
         s" keywords=${keywords.toArray.sorted.mkString(",")}" +
         s" gfiltering=${gfiltering}" +
         s" numSteps=${numSteps}" +
         s" numSubgraphs=${numSubgraphs}" +
         s" elapsedMs=${elapsed}" +
         s" throughput=${numSubgraphs / elapsed.toDouble}")
   }
}

object MinimalKeywordSearchPAEstimate extends Logging {
   val appid: String = "minimal_keyword_search_pa_estimate"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             keywords: Set[Int], gfiltering: Boolean, timeBudgetMs: Long): Unit = {

      val fc = fractalGraph.fractalContext
      val numVertices = explorationSteps + 1

      val fg = if (gfiltering) {
         fractalGraph.filterEdges(
            (u,uLabels,v,vLabels,e,eLabels) => {
               val uLabel = uLabels.getu(0)
               val vLabel = vLabels.getu(0)
               uLabel != vLabel || keywords.contains(uLabel)
            })
      } else {
         fractalGraph
      }

      val ((numSteps, throughputEstimate), elapsedMs) = FractalSparkRunner.time {
         val (numSteps, fractoidsIter) = fg.minimalKeywordSearchPA(keywords, numVertices)
         val stepTimeLimitMs = fractalGraph.config.getStepTimeLimitMs
         val throughputEstimate = fc.estimateThroughput(fractoidsIter.toSeq, timeBudgetMs, stepTimeLimitMs)
         (numSteps, throughputEstimate)
      }

      logApp(s"FinalResult" +
         s" numVertices=${numVertices}" +
         s" keywords=${keywords.toArray.sorted.mkString(",")}" +
         s" gfiltering=${gfiltering}" +
         s" numSteps=${numSteps}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughputEstimate=${throughputEstimate}")
   }
}

object PatternQueryGenerator extends Logging {
   val appid: String = "pattern_query_generator"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             fraction: Double, seed: Long, topk: Int,
             outputDir: String): Unit = {

      val fg = fractalGraph
      val fc = fg.fractalContext
      val sc = fc.sparkContext
      val senumClass = classOf[SamplingEnumerator[PatternInducedSubgraph]]
      val random = new java.util.Random(seed)
      val numVertices = explorationSteps + 1
      val maxEdges = numVertices * (numVertices - 1) / 2
      val minEdges = numVertices - 1

      val patterns = PatternUtilsRDD.getOrGenerateVertexPatternsRDD(sc, numVertices)
         .map(pattern => {
            pattern.setVertexLabeled(false)
            pattern.setInduced(false)
            pattern.setEdgeLabeled(false)
            PatternExplorationPlan.apply(pattern).get(0)
         })

      val fractoids = patterns.collect()
         .map(pattern => {
            val fractionKey = "sampling_fraction"
            val seedKey = "sampling_seed"
            fg.set(fractionKey, fraction).set(seedKey, seed)
               .pfractoid(pattern).extend(numVertices, senumClass)
         })

      val numQueryPatterns = fractoids.size
      logApp(s"NumJobs ${numQueryPatterns}")

      val resultsArray = ArrayBuffer.empty[Array[(Pattern, Long)]]
      var leastDenseLeastFrequent: (Pattern, Long) = null
      var leastDenseMostFrequent: (Pattern, Long) = null
      var mostDenseLeastFrequent: (Pattern, Long) = null
      var mostDenseMostFrequent: (Pattern, Long) = null

      var i = 0
      val step = 10
      var numPartialPatterns = 0
      while (i < fractoids.length) {
         val fractoidsBatch = fractoids.slice(i, i + step)
         val resultsBatch = fc.submitFractalSteps(fractoidsBatch.toIndexedSeq)(f => {
            val pattern = f.pattern
            f.aggregationCanonicalPatternLong(
               s => s.applyLabels(pattern), 0, _ => 1L, _ + _)
         })

         val partialResult = sc.union(resultsBatch)
         val partialResultFiltered = if(numVertices <= 3) {
            partialResult
         } else if (numVertices == 4) {
            partialResult.filter(kv => kv._2 > 0
               && kv._1.getNumberOfEdges != maxEdges)
         } else {
            partialResult.filter(kv => kv._2 > 0
               && kv._1.getNumberOfEdges != maxEdges
               && kv._1.getNumberOfEdges != minEdges)
         }

         partialResultFiltered.cache()
         var patternsSet = Set.empty[Pattern]

         // least dense, least frequent
         val ldlfTake = partialResultFiltered
            .sortBy(kv => (kv._1.getNumberOfEdges, kv._2))
            .take(topk)
         if (ldlfTake.nonEmpty) {
            val ldlf = ldlfTake.apply(random.nextInt(topk))
            patternsSet += ldlf._1
            leastDenseLeastFrequent = Array(leastDenseLeastFrequent, ldlf)
               .filter(kv => kv != null)
               .minBy(kv => (kv._1.getNumberOfEdges, kv._2))
         }

         val ldmfTake = partialResultFiltered
            .filter(kv => !patternsSet.contains(kv._1))
            .sortBy(kv => (kv._1.getNumberOfEdges, -kv._2))
            .take(topk)
         if (ldmfTake.nonEmpty) {
            val ldmf = ldmfTake.apply(random.nextInt(topk))
            patternsSet += ldmf._1
            leastDenseMostFrequent = Array(leastDenseMostFrequent, ldmf)
               .filter(kv => kv != null)
               .minBy(kv => (kv._1.getNumberOfEdges, -kv._2))
         }

         val mdlfTake = partialResult
            .filter(kv => !patternsSet.contains(kv._1))
            .sortBy(kv => (-kv._1.getNumberOfEdges, kv._2))
            .take(topk)
         if (mdlfTake.nonEmpty) {
            val mdlf = mdlfTake.apply(random.nextInt(topk))
            patternsSet += mdlf._1
            mostDenseLeastFrequent = Array(mostDenseLeastFrequent, mdlf)
               .filter(kv => kv != null)
               .minBy(kv => (-kv._1.getNumberOfEdges, kv._2))
         }

         val mdmfTake = partialResult
            .filter(kv => !patternsSet.contains(kv._1))
            .sortBy(kv => (-kv._1.getNumberOfEdges, -kv._2))
            .take(topk)
         if (mdmfTake.nonEmpty) {
            val mdmf = mdmfTake.apply(random.nextInt(topk))
            patternsSet += mdmf._1
            mostDenseMostFrequent = Array(mostDenseMostFrequent, mdmf)
               .filter(kv => kv != null)
               .minBy(kv => (-kv._1.getNumberOfEdges, -kv._2))
         }

         partialResultFiltered.unpersist()

         numPartialPatterns += resultsBatch.size
         logApp(s"PartialResultPatternQueries" +
            s" ${numPartialPatterns}/${numQueryPatterns}" +
            s" leastDenseLeastFrequent=${leastDenseLeastFrequent}" +
            s" leastDenseMostFrequent=${leastDenseMostFrequent}" +
            s" mostDenseLeastFrequent=${mostDenseLeastFrequent}" +
            s" mostDenseMostFrequent=${mostDenseMostFrequent}")
         i += step
      }

      logApp(s"LeastDenseLeastFrequent ${leastDenseLeastFrequent}")
      logApp(s"LeastDenseMostFrequent ${leastDenseMostFrequent}")
      logApp(s"MostDenseLeastFrequent ${mostDenseLeastFrequent}")
      logApp(s"MostDenseMostFrequent ${mostDenseMostFrequent}")

      // write sampled patterns to file
      val prefix = s"${fg.name}-k${numVertices}-fraction${fraction}" +
         s"-seed${seed}-top${topk}"

      val leastDenseLeastFrequentPath =
         new File(outputDir, s"${prefix}-least-dense-least-freq").getPath
      leastDenseLeastFrequent._1.setVertexLabeled(true)
      PatternUtils.toFS(leastDenseLeastFrequent._1, leastDenseLeastFrequentPath)

      val leastDenseMostFrequentPath =
         new File(outputDir, s"${prefix}-least-dense-most-freq").getPath
      leastDenseMostFrequent._1.setVertexLabeled(true)
      PatternUtils.toFS(leastDenseMostFrequent._1, leastDenseMostFrequentPath)

      val mostDenseLeastFrequentPath =
         new File(outputDir, s"${prefix}-most-dense-least-freq").getPath
      mostDenseLeastFrequent._1.setVertexLabeled(true)
      PatternUtils.toFS(mostDenseLeastFrequent._1, mostDenseLeastFrequentPath)

      val mostDenseMostFrequentPath =
         new File(outputDir, s"${prefix}-most-dense-most-freq").getPath
      mostDenseMostFrequent._1.setVertexLabeled(true)
      PatternUtils.toFS(mostDenseMostFrequent._1, mostDenseMostFrequentPath)

      logApp(s"Sampled patterns written to: ${outputDir}")
   }
}

object LabelQueryGenerator extends Logging {
   val appid: String = "label_query_generator"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             fraction: Double, seed: Long, topk: Int): Unit = {
      val numVertices = explorationSteps + 1
      val random = new java.util.Random(seed)
      val fractionKey = "sampling_fraction"
      val seedKey = "sampling_seed"
      val senumClass = classOf[SamplingEnumerator[VertexInducedSubgraph]]

      val getLabels: VertexInducedSubgraph => IntArrayList = s => {
         val graph = s.getMainGraph
         val vertices = s.getVertices
         val numVertices = vertices.size()
         var labelSet = Set.empty[Int]
         var i = 0
         while (i < numVertices) {
            labelSet += graph.firstVertexLabel(vertices.getu(i))
            i += 1
         }
         val arr = new IntArrayList(labelSet.toArray)
         arr.sort()
         arr
      }

      val labelsCounts = fractalGraph
         .set(fractionKey, fraction)
         .set(seedKey, seed)
         .vfractoid
         .extend(1, senumClass)
         .filter((s, _) => getLabels(s).size() == s.getNumVertices)
         .explore(numVertices - 1)
         .aggregationObjLong(getLabels, 0L, _ => 1L, _ + _)

      labelsCounts.cache()

      val leastFreq = labelsCounts.top(topk)(
         new Ordering[(IntArrayList, Long)] {
            override def compare(x: (IntArrayList, Long),
                                 y: (IntArrayList, Long)): Int = {
               if (x._2 < y._2) 1 else if (x._2 > y._2) -1 else 0
            }
         }).apply(random.nextInt(topk))
      logApp(s"leastFreqLabels=${leastFreq._1} count=${leastFreq._2}")

      val mid = labelsCounts.count() / 2
      val medianFreq = labelsCounts
         .sortBy(_._2)
         .zipWithIndex()
         .filter(kv => kv._2 >= mid && kv._2 <= mid + topk)
         .keys
         .collect()
         .apply(random.nextInt(topk))
      logApp(s"medianFreqLabels=${medianFreq._1} count=${medianFreq._2}")

      val mostFreq = labelsCounts.top(topk)(
         new Ordering[(IntArrayList, Long)] {
            override def compare(x: (IntArrayList, Long),
                                 y: (IntArrayList, Long)): Int = {
               if (x._2 < y._2) -1 else if (x._2 > y._2) 1 else 0
            }
         }).apply(random.nextInt(topk))
      logApp(s"mostFreqLabels=${mostFreq._1} count=${mostFreq._2}")

      labelsCounts.unpersist()
   }
}

object CanonicalPatternsGeneratorByVertex extends Logging {
   val appid: String = "canonical_pattern_generator_vertex"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             outputPath: String): Unit = {
      val fc = fractalGraph.fractalContext
      val sc = fc.sparkContext
      val numVertices = explorationSteps + 1
      val patternsRDD = PatternUtilsRDD.getOrGenerateVertexPatternsRDD(sc,
         numVertices, outputPath)
      logApp(s"npatterns=${patternsRDD.count()}\n${patternsRDD.collect().mkString("\n")}")
      logApp(s"numVertices=${numVertices} outputPath=${outputPath}")
   }
}

object FractalSparkRunner extends Logging {
   def time[R](block: => R): (R, Long) = {
      val t0 = System.currentTimeMillis()
      val result = block    // call-by-name
      val t1 = System.currentTimeMillis()
      (result, t1 - t0)
   }

   def main(args: Array[String]): Unit = {
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
         case "n" =>
            "br.ufmg.cs.systems.fractal.graph.UnlabeledMainGraph"
         case "v" =>
            "br.ufmg.cs.systems.fractal.graph.VLabeledMainGraph"
         case "e" =>
            "br.ufmg.cs.systems.fractal.graph.ELabeledMainGraph"
         case "ve" =>
            "br.ufmg.cs.systems.fractal.graph.VELabeledMainGraph"
         case other =>
            throw new RuntimeException(s"Input graph format '${other}' is invalid")
      }
      val graphPath = nextArg
      val algorithm = nextArg
      val numPartitions = nextArg.toInt
      val explorationSteps = nextArg.toInt
      val logLevel = nextArg

      val conf = new SparkConf()
      conf.set("spark.scheduler.mode", "FIFO")
      conf.set("spark.scheduler.minRegisteredResourcesRatio", "1.0")
      val sc = new SparkContext(conf)

      logInfo(s"\nSparkApplicationId ${sc.applicationId}")

      if (!sc.isLocal) {
         // TODO: this is ugly but have to make sure all spark executors are up by
         // the time we start executing fractal applications
         Thread.sleep(10000)
      }

      val fc = new FractalContext(sc, logLevel)
      var fractalGraph = fc
         .textFile(graphPath, graphClass = graphClass)
         .set("num_partitions", numPartitions)

      // set time limit per step
      val stepTimeLimit = nextArg.toLong
      fractalGraph = fractalGraph.set("step_time_limit", stepTimeLimit)

      // set start time and time limit
      val timeLimit = nextArg.toLong
      fractalGraph = fractalGraph.set("time_limit", timeLimit)
      fractalGraph = fractalGraph.set("start_time", System.currentTimeMillis())

      // schedule termination according to time limit (if configured)
      //var terminated = false
      //import scala.concurrent.ExecutionContext.Implicits._
      //val timeLimitFuture = Future {
      //   if (timeLimit > 0) {
      //      Thread.sleep(timeLimit + 10000)
      //      terminated = true
      //      logInfo(s"TimeLimitReached timelimit=${timeLimit}")
      //      fc.terminate()
      //      fc.stop()
      //      sc.cancelAllJobs()
      //      sc.stop()
      //      System.exit(0)
      //   }
      //}

      def setRemainingConfigs(): Unit = {
         var arg = nextArg
         while (arg != null) {
            val kv = arg.split(":")
            logApp(s"Found config=${arg}")
            if (kv.length == 2) {
               fractalGraph = fractalGraph.set(kv(0), kv(1))
            }
            arg = nextArg
         }
      }

      algorithm.toLowerCase match {
         case SubgraphsListingPO.appid =>
            setRemainingConfigs()
            SubgraphsListingPO(fractalGraph, explorationSteps)

         case SubgraphsListingPA.appid =>
            setRemainingConfigs()
            SubgraphsListingPA(fractalGraph, explorationSteps)

         case InducedSubgraphsListingPAMCVC.appid =>
            setRemainingConfigs()
            InducedSubgraphsListingPAMCVC(fractalGraph, explorationSteps)

         case InducedSubgraphsListingPA.appid =>
            setRemainingConfigs()
            InducedSubgraphsListingPA(fractalGraph, explorationSteps)

         case InducedSubgraphsListingPO.appid =>
            setRemainingConfigs()
            InducedSubgraphsListingPO(fractalGraph, explorationSteps)

         case InducedSubgraphsListingSamplePO.appid =>
            val fraction = nextArg.toDouble
            setRemainingConfigs()
            InducedSubgraphsListingSamplePO(fractalGraph, explorationSteps,
               fraction)

         case InducedSubgraphsListingSamplePA.appid =>
            val fraction = nextArg.toDouble
            setRemainingConfigs()
            InducedSubgraphsListingSamplePA(fractalGraph, explorationSteps,
               fraction)

         case MotifsPA.appid =>
            setRemainingConfigs()
            MotifsPA(fractalGraph, explorationSteps)

         case MotifsPAMCVC.appid =>
            setRemainingConfigs()
            MotifsPAMCVC(fractalGraph, explorationSteps)

         case MotifsSamplePO.appid =>
            val fraction = nextArg.toDouble
            setRemainingConfigs()
            MotifsSamplePO(fractalGraph, explorationSteps, fraction)

         case MotifsSamplePA.appid =>
            val fraction = nextArg.toDouble
            setRemainingConfigs()
            MotifsSamplePA(fractalGraph, explorationSteps, fraction)

         case MotifsPO.appid =>
            setRemainingConfigs()
            MotifsPO(fractalGraph, explorationSteps)

         case CliquesCustomKClist.appid =>
            setRemainingConfigs()
            CliquesCustomKClist(fractalGraph, explorationSteps)

         case CliquesPO.appid =>
            setRemainingConfigs()
            CliquesPO(fractalGraph, explorationSteps)

         case CliquesPA.appid =>
            setRemainingConfigs()
            CliquesPA(fractalGraph, explorationSteps)

         case MaximalCliquesCustomQuick.appid =>
            setRemainingConfigs()
            MaximalCliquesCustomQuick(fractalGraph, explorationSteps)

         case MaximalCliquesPA.appid =>
            setRemainingConfigs()
            MaximalCliquesPA(fractalGraph, explorationSteps)

         case MaximalCliquesPO.appid =>
            setRemainingConfigs()
            MaximalCliquesPO(fractalGraph, explorationSteps)

         case QuasiCliquesPO.appid =>
            val minDensity = nextArg.toDouble
            setRemainingConfigs()
            QuasiCliquesPO(fractalGraph, explorationSteps, minDensity)

         case QuasiCliquesPA.appid =>
            val minDensity = nextArg.toDouble
            setRemainingConfigs()
            QuasiCliquesPA(fractalGraph, explorationSteps, minDensity)

         case QuasiCliquesPAPO.appid =>
            val minDensity = nextArg.toDouble
            setRemainingConfigs()
            QuasiCliquesPAPO(fractalGraph, explorationSteps, minDensity)

         case FSMPO.appid =>
            val support = nextArg.toInt
            setRemainingConfigs()
            FSMPO(fractalGraph, explorationSteps, support)

         case FSMPA.appid =>
            val support = nextArg.toInt
            setRemainingConfigs()
            FSMPA(fractalGraph, explorationSteps, support)

         case FSMPAMCVC.appid =>
            val support = nextArg.toInt
            setRemainingConfigs()
            FSMPAMCVC(fractalGraph, explorationSteps, support)

         case FSMPAPO.appid =>
            val support = nextArg.toInt
            setRemainingConfigs()
            FSMPAPO(fractalGraph, explorationSteps, support)

         case PatternQueryingPAMCVCOld.appid =>
            val subgraphPath = nextArg
            val plabeling = nextArg
            setRemainingConfigs()
            PatternQueryingPAMCVCOld(fractalGraph, explorationSteps,
               subgraphPath, plabeling)

         case PatternQueryingInducedPAMCVC.appid =>
            val subgraphPath = nextArg
            setRemainingConfigs()
            PatternQueryingInducedPAMCVC(fractalGraph, explorationSteps,
               subgraphPath)

         case PatternQueryingPO.appid =>
            val subgraphPath = nextArg
            val plabeling = nextArg
            setRemainingConfigs()
            PatternQueryingPO(fractalGraph, explorationSteps, subgraphPath, plabeling)

         case PatternQueryingPA.appid =>
            val subgraphPath = nextArg
            val plabeling = nextArg
            setRemainingConfigs()
            PatternQueryingPA(fractalGraph, explorationSteps, subgraphPath, plabeling)

         case PatternQueryingPAMCVC.appid =>
            val subgraphPath = nextArg
            val plabeling = nextArg
            setRemainingConfigs()
            PatternQueryingPAMCVC(fractalGraph, explorationSteps,
               subgraphPath, plabeling)

         case PatternQueryingInducedPA.appid =>
            val subgraphPath = nextArg
            setRemainingConfigs()
            PatternQueryingInducedPA(fractalGraph, explorationSteps,
               subgraphPath)

         case PatternQueryingSamplePA.appid =>
            val subgraphPath = nextArg
            val fraction = nextArg.toDouble
            setRemainingConfigs()
            PatternQueryingSamplePA(fractalGraph, explorationSteps,
               subgraphPath, fraction)

         case PatternQueryingInducedSamplePA.appid =>
            val subgraphPath = nextArg
            val fraction = nextArg.toDouble
            setRemainingConfigs()
            PatternQueryingInducedSamplePA(fractalGraph, explorationSteps,
               subgraphPath, fraction)


         case PeriodicSubgraphsInducedPO.appid =>
            val periodicThreshold = nextArg.toInt
            setRemainingConfigs()
            PeriodicSubgraphsInducedPO(fractalGraph, explorationSteps,
               periodicThreshold)

         case PeriodicSubgraphsInducedPA.appid =>
            val periodicThreshold = nextArg.toInt
            setRemainingConfigs()
            PeriodicSubgraphsInducedPA(fractalGraph, explorationSteps,
               periodicThreshold)

         case PeriodicSubgraphsInducedPAMCVC.appid =>
            val periodicThreshold = nextArg.toInt
            setRemainingConfigs()
            PeriodicSubgraphsInducedPAMCVC(fractalGraph, explorationSteps,
               periodicThreshold)

         case LabelSearchPO.appid =>
            val labelsSet = nextArg.split(",").map(str => str.trim.toInt).toSet
            val gfiltering = nextArg.toBoolean
            setRemainingConfigs()
            LabelSearchPO(fractalGraph, explorationSteps, labelsSet,
               gfiltering)

         case LabelSearchPOEstimate.appid =>
            val labelsSet = nextArg.split(",").map(str => str.trim.toInt).toSet
            val gfiltering = nextArg.toBoolean
            val timeBudgetMs = nextArg.toLong
            setRemainingConfigs()
            LabelSearchPOEstimate(fractalGraph, explorationSteps, labelsSet,
               gfiltering, timeBudgetMs)

         case LabelSearchPA.appid =>
            val labelsSet = nextArg.split(",").map(str => str.trim.toInt).toSet
            val gfiltering = nextArg.toBoolean
            setRemainingConfigs()
            LabelSearchPA(fractalGraph, explorationSteps, labelsSet,
               gfiltering)

         case LabelSearchPAEstimate.appid =>
            val labelsSet = nextArg.split(",").map(str => str.trim.toInt).toSet
            val gfiltering = nextArg.toBoolean
            val timeBudgetMs = nextArg.toLong
            setRemainingConfigs()
            LabelSearchPAEstimate(fractalGraph, explorationSteps, labelsSet,
               gfiltering, timeBudgetMs)

         case KeywordSearchPO.appid =>
            val keywords = nextArg.split(",").map(str => str.trim.toInt).toSet
            val gfiltering = nextArg.toBoolean
            setRemainingConfigs()
            KeywordSearchPO(fractalGraph, explorationSteps, keywords,
               gfiltering)

         case MinimalKeywordSearchPO.appid =>
            val keywords = nextArg.split(",").map(str => str.trim.toInt).toSet
            val gfiltering = nextArg.toBoolean
            setRemainingConfigs()
            MinimalKeywordSearchPO(fractalGraph, explorationSteps, keywords,
               gfiltering)

         case MinimalKeywordSearchPOEstimate.appid =>
            val keywords = nextArg.split(",").map(str => str.trim.toInt).toSet
            val gfiltering = nextArg.toBoolean
            val timeBudgetMs = nextArg.toLong
            setRemainingConfigs()
            MinimalKeywordSearchPOEstimate(fractalGraph, explorationSteps,
               keywords, gfiltering, timeBudgetMs)

         case MinimalKeywordSearchPA.appid =>
            val keywords = nextArg.split(",").map(str => str.trim.toInt).toSet
            val gfiltering = nextArg.toBoolean
            setRemainingConfigs()
            MinimalKeywordSearchPA(fractalGraph, explorationSteps, keywords,
               gfiltering)

         case MinimalKeywordSearchPAEstimate.appid =>
            val keywords = nextArg.split(",").map(str => str.trim.toInt).toSet
            val gfiltering = nextArg.toBoolean
            val timeBudgetMs = nextArg.toLong
            setRemainingConfigs()
            MinimalKeywordSearchPAEstimate(fractalGraph, explorationSteps, keywords,
               gfiltering, timeBudgetMs)

         case PatternQueryGenerator.appid =>
            val fraction = nextArg.toDouble
            val seed = nextArg.toLong
            val topk = nextArg.toInt
            val outputDir = nextArg
            setRemainingConfigs()
            PatternQueryGenerator(fractalGraph, explorationSteps, fraction,
               seed, topk, outputDir)

         case LabelQueryGenerator.appid =>
            val fraction = nextArg.toDouble
            val seed = nextArg.toLong
            val topk = nextArg.toInt
            setRemainingConfigs()
            LabelQueryGenerator(fractalGraph, explorationSteps, fraction,
               seed, topk)

         case QuerySpecializationPO.appid =>
            val patternPath = nextArg
            setRemainingConfigs()
            QuerySpecializationPO(fractalGraph, explorationSteps,
               patternPath)

         case QuerySpecializationPA.appid =>
            val patternPath = nextArg
            setRemainingConfigs()
            QuerySpecializationPA(fractalGraph, explorationSteps,
               patternPath)

         case QuerySpecializationPAPO.appid =>
            val patternPath = nextArg
            setRemainingConfigs()
            QuerySpecializationPAPO(fractalGraph, explorationSteps,
               patternPath)

         case QuerySpecializationPOEstimate.appid =>
            val patternPath = nextArg
            val timeLimitMs = nextArg.toLong
            setRemainingConfigs()
            QuerySpecializationPOEstimate(fractalGraph, explorationSteps,
               patternPath, timeLimitMs)

         case QuerySpecializationPAEstimate.appid =>
            val patternPath = nextArg
            val timeLimitMs = nextArg.toLong
            setRemainingConfigs()
            QuerySpecializationPAEstimate(fractalGraph, explorationSteps,
               patternPath, timeLimitMs)

         case QuerySpecializationPAPOEstimate.appid =>
            val patternPath = nextArg
            val timeLimitMs = nextArg.toLong
            setRemainingConfigs()
            QuerySpecializationPAPOEstimate(fractalGraph, explorationSteps,
               patternPath, timeLimitMs)

         case CanonicalPatternsGeneratorByVertex.appid =>
            val outputPath = nextArg
            setRemainingConfigs()
            CanonicalPatternsGeneratorByVertex(fractalGraph, explorationSteps, outputPath)

         case GeneratePatternsAndPlansHeuristic.appid =>
            val induced = nextArg.toBoolean
            setRemainingConfigs()
            GeneratePatternsAndPlansHeuristic(fractalGraph, explorationSteps, induced)


         case appName =>
            throw new RuntimeException(s"Unknown app: ${appName}")
      }

      //if (terminated) {
      //   Await.result(timeLimitFuture, Duration.Inf)
      //}

      fc.stop()
      sc.stop()
   }
}
