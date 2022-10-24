package br.ufmg.cs.systems.fractal

import br.ufmg.cs.systems.fractal.callback.SubgraphCallback
import br.ufmg.cs.systems.fractal.computation.{Computation, ExecutionEngine, SamplingEnumerator}
import br.ufmg.cs.systems.fractal.gmlib.periodic.InducedPeriodicSubgraphsPAMCVC
import br.ufmg.cs.systems.fractal.graph.VELabeledMainGraph
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternExplorationPlan, PatternUtils, PatternUtilsRDD}
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, PatternInducedSubgraph, Subgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.{Logging, ReflectionSerializationUtils}
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import br.ufmg.cs.systems.fractal.util.ReportFuncs._
import org.apache.hadoop.io.compress.{BZip2Codec, SnappyCodec}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import java.util.Base64
import java.util.function.BiPredicate
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{Duration, pairIntToDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

object SubgraphsListingPO extends Logging {
   val appid: String = "subgraphs_listing_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int): Unit = {
      val frac = fractalGraph
         .efractoid.expand(1).explore(explorationSteps)

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
      val frac = fractalGraph.vfractoid.expand(1).explore(explorationSteps)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
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
            .expand(1, senumClass)
            .explore(explorationSteps)
            .aggregationCount
      }

      logApp(s"numSubgraphs=${numSugbraphs}" +
         s" numSubgraphsEstimate=${numSugbraphs / fraction} elapsed=${elapsed}")
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

      logApp(s"numMotifs=${numMotifs} numSubgraphs=${numSubgraphs}" +
         s" numSubgraphsEstimate=${numSubgraphs / fraction} elapsed=${elapsed}")
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
      val numVertices = explorationSteps + 1
      val frac = fractalGraph.quasiCliquesPO(numVertices, minDensity)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount(COUNT_AGG_REPORT)
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object QuasiCliquesPA extends Logging {
   val appid: String = "quasi_cliques_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int, minDensity: Double)
   : Unit = {
      val fc = fractalGraph.fractalContext
      val numVertices = explorationSteps + 1
      val fracs = fractalGraph.quasiCliquesPA(numVertices, minDensity)

      val (results, elapsed) = FractalSparkRunner.time {
         fc.trySubmitFractalSteps(fracs)(f => {
            val numSubgraphs = f.aggregationCount(COUNT_AGG_REPORT)
            logApp(s"PartialResult fractoid=${f} numSubgraphs=${numSubgraphs}")
            numSubgraphs
         })
      }

      val failure = results.exists(_.isFailure)
      val numSubgraphs = results.filter(_.isSuccess).map(_.get).sum

      logApp(s"failure=${failure} numSubgraphs=${numSubgraphs} " +
         s"elapsed=${elapsed}")
   }
}

object QuasiCliquesPAPO extends Logging {
   val appid: String = "quasi_cliques_pa_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int, minDensity: Double)
   : Unit = {
      val fc = fractalGraph.fractalContext
      val numVertices = explorationSteps + 1
      val fracs = fractalGraph.quasiCliquesPAPO(numVertices, minDensity)

      val (results, elapsed) = FractalSparkRunner.time {
         fc.trySubmitFractalSteps(fracs)(f => {
            val numSubgraphs = f.aggregationCount(COUNT_AGG_REPORT)
            logApp(s"PartialResult fractoid=${f} numSubgraphs=${numSubgraphs}")
            numSubgraphs
            //String path = getMainGraphPath();
         })
      }

      val failure = results.exists(_.isFailure)
      val numSubgraphs = results.filter(_.isSuccess).map(_.get).sum

      logApp(s"failure=${failure} numSubgraphs=${numSubgraphs}" +
         s" elapsed=${elapsed}")
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

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         val fractoid = fractalGraph.querySpecializationPO(pattern)
         fractoid.aggregationCount(COUNT_AGG_REPORT)
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
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

      val (results, elapsed) = FractalSparkRunner.time {
         val fractoids = fractalGraph.querySpecializationPA(pattern)
         fc.trySubmitFractalSteps(fractoids)(f => {
            val numSubgraphs = f.aggregationCount(COUNT_AGG_REPORT)
            logApp(s"PartialResult fractoid=${f} numSubgraphs=${numSubgraphs}")
            numSubgraphs
         })
      }

      val failure = results.exists(_.isFailure)
      val numSubgraphs = results.filter(_.isSuccess).map(_.get).sum

      logApp(s"failure=${failure} numSubgraphs=${numSubgraphs}" +
         s" elapsed=${elapsed}")
   }
}

object QuerySpecializationPAPO extends Logging {
   val appid: String = "query_specialization_pa_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             patternPath: String): Unit = {

      val pattern = PatternUtils.fromFS(patternPath)
      pattern.setInduced(false)
      pattern.setVertexLabeled(true)
      pattern.setEdgeLabeled(false)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         val frac = fractalGraph.querySpecializationPAPO(pattern)
         frac.aggregationCount(COUNT_AGG_REPORT)
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object FSMPO extends Logging {
   val appid: String = "fsm_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int, minSupport: Int)
   : Unit = {

      // we create the fractoid inside the timer because there is actual work
      // being done to obtain the RDD
      val ((patternsSupportsRDD,numPatterns), elapsed) =
      FractalSparkRunner.time {
         val patternsSupportsRDD = fractalGraph
            .fsmPO(minSupport, explorationSteps + 1)
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

object FSMPA extends Logging {
   val appid: String = "fsm_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int, minSupport: Int)
   : Unit = {

      // we create the fractoid inside the timer because there is actual work
      // being done to obtain the RDD
      val ((patternsSupportsRDD,numPatterns), elapsed) = FractalSparkRunner.time {
         val patternsSupportsRDD = fractalGraph
            .fsmPA(minSupport, explorationSteps + 1)
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

      // we create the fractoid inside the timer because there is actual work
      // being done to obtain the RDD
      val ((patternsSupportsRDD,numPatterns), elapsed) = FractalSparkRunner.time {
         val patternsSupportsRDD = fractalGraph
            .fsmPAPO(minSupport, explorationSteps + 1)
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

      val frac = fractalGraph.patternQueryingPO(pattern)
         .explore(pattern.getNumberOfEdges - 1)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount(COUNT_AGG_REPORT)
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object PatternQueryingPA extends Logging {
   val appid: String = "pattern_querying_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             subgraphPath: String, plabeling: String): Unit = {
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

      val frac = fractalGraph.patternQueryingPA(pattern)
         .explore(pattern.getNumberOfVertices - 1)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount(COUNT_AGG_REPORT)
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
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
         fc.trySubmitFractalSteps(fracs)(
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

      val fracs = fractalGraph.patternQueryingPAMCVC_old(pattern)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         var numSubgraphs = 0L
         for (frac <- fracs) {
            numSubgraphs += frac.aggregationCountWithCallback(
               (s,c,cb) => s.completeMatch(c, c.getPattern, cb)
            )
         }
         numSubgraphs
      }

      logApp(s"numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
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

      val fg = if (gfiltering) {
         fractalGraph.filterEdges(
            (u,uLabels,v,vLabels,e,eLabels) => {
               labelsSet.contains(uLabels.getu(0)) &&
                  labelsSet.contains(vLabels.getu(0))
            })
      } else {
         fractalGraph
      }

      val fractoidsIter = fg.labelSearchPA(labelsSet, explorationSteps + 1)
      val fractoidsGroupedIter = fractoidsIter.grouped(10)

      val (results, elapsed) = FractalSparkRunner.time {
         val results = ArrayBuffer.empty[Try[Long]]
         while (fractoidsGroupedIter.hasNext) {
            val fractoids = fractoidsGroupedIter.next()
            val partialResults = fg.fractalContext.trySubmitFractalSteps(
               fractoids)(f => f.aggregationCount(COUNT_AGG_REPORT))
            results ++= partialResults
         }
         results
      }

      val failure = results.exists(_.isFailure)
      val numSubgraphs = results.filter(_.isSuccess).map(_.get).sum

      logApp(s"failure=${failure} labelsSet=${labelsSet}" +
         s" gfiltering=${gfiltering} numSubgraphs=${numSubgraphs}" +
         s" elapsed=${elapsed}")
   }
}

object LabelSearchPO extends Logging {
   val appid: String = "label_search_po"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             labelsSet: Set[Int], gfiltering: Boolean): Unit = {

      val fg = if (gfiltering) {
         fractalGraph.filterEdges(
            (u,uLabels,v,vLabels,e,eLabels) => {
               labelsSet.contains(uLabels.getu(0)) &&
                  labelsSet.contains(vLabels.getu(0))
            })
      } else {
         fractalGraph
      }

      val frac = fg.labelSearchPO(labelsSet, explorationSteps + 1)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount(COUNT_AGG_REPORT)
      }

      logApp(s"labelsSet=${labelsSet} gfiltering=${gfiltering}" +
         s" numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
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

      val frac = fg.minimalKeywordSearchPO(keywords, explorationSteps + 1)

      val (numSubgraphs, elapsed) = FractalSparkRunner.time {
         frac.aggregationCount(COUNT_AGG_REPORT)
      }

      logApp(s"labelsSet=${keywords} gfiltering=${gfiltering}" +
         s" numSubgraphs=${numSubgraphs} elapsed=${elapsed}")
   }
}

object MinimalKeywordSearchPA extends Logging {
   val appid: String = "minimal_keyword_search_pa"

   def apply(fractalGraph: FractalGraph, explorationSteps: Int,
             keywords: Set[Int], gfiltering: Boolean): Unit = {

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

      val fractoidsIter = fg.minimalKeywordSearchPA(keywords, numVertices)
      val fractoidsGroupedIter = fractoidsIter.grouped(10)

      val (results, elapsed) = FractalSparkRunner.time {
         val results = ArrayBuffer.empty[Try[Long]]
         while (fractoidsGroupedIter.hasNext) {
            val fractoids = fractoidsGroupedIter.next()
            val partialResults = fg.fractalContext.trySubmitFractalSteps(
               fractoids)(f => f.aggregationCount(COUNT_AGG_REPORT))
            results ++= partialResults
         }
         results
      }

      val failure = results.exists(_.isFailure)
      val numSubgraphs = results.filter(_.isSuccess).map(_.get).sum

      logApp(s"failure=${failure} labelsSet=${keywords}" +
         s" gfiltering=${gfiltering} numSubgraphs=${numSubgraphs}" +
         s" elapsed=${elapsed}")
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
               .pfractoid(pattern).expand(numVertices, senumClass)
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
         val resultsBatch = fc.submitFractalSteps(fractoidsBatch)(f => {
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
         .expand(1, senumClass)
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
      logApp(s"npatterns=${patternsRDD.count}\n${patternsRDD.collect().mkString("\n")}")
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

      // set start time and time limit
      val timeLimit = nextArg.toLong
      fractalGraph = fractalGraph.set("time_limit", timeLimit)
      fractalGraph = fractalGraph.set("start_time", System.currentTimeMillis())

      // schedule termination according to time limit (if configured)
      var terminated = false
      import scala.concurrent.ExecutionContext.Implicits._
      val timeLimitFuture = Future {
         if (timeLimit > 0) {
            Thread.sleep(timeLimit)
            terminated = true
            logInfo(s"TimeLimitReached timelimit=${timeLimit}")
            fc.terminate()
            fc.stop()
            sc.cancelAllJobs()
            sc.stop()
            System.exit(0)
         }
      }

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
            val labelsSet = nextArg.split(",").map(str => str.trim.toInt)
               .toSet
            val gfiltering = nextArg.toBoolean
            setRemainingConfigs()
            LabelSearchPO(fractalGraph, explorationSteps, labelsSet,
               gfiltering)

         case LabelSearchPA.appid =>
            val labelsSet = nextArg.split(",").map(str => str.trim.toInt)
               .toSet
            val gfiltering = nextArg.toBoolean
            setRemainingConfigs()
            LabelSearchPA(fractalGraph, explorationSteps, labelsSet,
               gfiltering)

         case KeywordSearchPO.appid =>
            val keywords = nextArg.split(",").map(str => str.trim.toInt)
               .toSet
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

         case MinimalKeywordSearchPA.appid =>
            val keywords = nextArg.split(",").map(str => str.trim.toInt).toSet
            val gfiltering = nextArg.toBoolean
            setRemainingConfigs()
            MinimalKeywordSearchPA(fractalGraph, explorationSteps, keywords,
               gfiltering)

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

         case CanonicalPatternsGeneratorByVertex.appid =>
            val outputPath = nextArg
            setRemainingConfigs()
            CanonicalPatternsGeneratorByVertex(fractalGraph, explorationSteps, outputPath)

         case appName =>
            throw new RuntimeException(s"Unknown app: ${appName}")
      }

      if (terminated) {
         Await.result(timeLimitFuture, Duration.Inf)
      }

      fc.stop()
      sc.stop()
   }
}
