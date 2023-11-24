package br.ufmg.cs.systems.fractal.gmlib.fsm

import br.ufmg.cs.systems.fractal.{FractalGraph, Fractoid, Primitive}
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternExplorationPlan}
import br.ufmg.cs.systems.fractal.subgraph.EdgeInducedSubgraph
import br.ufmg.cs.systems.fractal.util.ReportFuncs
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class FSMPAPO(minSupport: Int, maxNumEdges: Int)
   extends BuiltInApplication[RDD[(Pattern,MinImageSupport)]] {

   private var lastCurrentTimeMs: Long = System.currentTimeMillis()

   private var frequentPatternsCurrentTimeMs: Long = System.currentTimeMillis()

   // type aliases
   protected type PatternsSupports = RDD[(Pattern,MinImageSupport)]
   protected type Patterns = RDD[Pattern]

   // reusable support value
   protected val minImageSupport = new MinImageSupport(minSupport)

   // value function: min image support
   protected val value: EdgeInducedSubgraph => MinImageSupport = s => {
      minImageSupport.setSubgraph(s)
      minImageSupport
   }

   // aggregate function
   protected val aggregate: (MinImageSupport,MinImageSupport) => Unit =
      (s1,s2) => {
         s1.aggregate(s2)
      }

   private def getElapsedTimeMs: Long = {
      val now = System.currentTimeMillis()
      val elapsed = now - lastCurrentTimeMs
      lastCurrentTimeMs = now
      elapsed
   }

   private def getElapsedTimeFrequentPatternsMs: Long = {
      val now = System.currentTimeMillis()
      val elapsed = now - frequentPatternsCurrentTimeMs
      frequentPatternsCurrentTimeMs = now
      elapsed
   }

   private def materializeAndLogPartialResult
   (fractoid: Fractoid[EdgeInducedSubgraph],
    canonicalPatternsSupportsRDD: RDD[(Pattern, MinImageSupport)]): Long = {
      canonicalPatternsSupportsRDD.cache()
      canonicalPatternsSupportsRDD.foreachPartition(_ => {})
      val elapsedMs = getElapsedTimeMs
      val iter = canonicalPatternsSupportsRDD.toLocalIterator
      val numEdges = if (fractoid.parent == null) {
         fractoid.primitives.count(_ == Primitive.E)
      } else {
         fractoid.parent.pattern.getNumberOfEdges + 1
      }
      var numSubgraphs = 0L
      var numPatterns = 0L
      while (iter.hasNext) {
         val (pattern, support) = iter.next()
         numSubgraphs += support.getNumSubgraphsAggregated
         numPatterns += 1
         logApp(s"CandidatePattern fractoid=${fractoid} numEdges=${numEdges}" +
            s" minSupport=${minSupport} pattern=${pattern} support=${support}")
      }

      val patterns = canonicalPatternsSupportsRDD.keys.collect()
      logApp(s"StepResult fractoid=${fractoid}" +
         s" numEdges=${numEdges}" +
         s" support=${minSupport}" +
         s" numSteps=1" +
         s" numSubgraphs=${numSubgraphs}" +
         s" numPatterns=${numPatterns}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughput=${numSubgraphs / elapsedMs.toDouble}")

      numPatterns
   }

   private def materializeAndLogResult
   (fractoids: Iterable[Fractoid[EdgeInducedSubgraph]],
    freqRDD: RDD[(Pattern, MinImageSupport)]): Long = {
      freqRDD.cache()
      freqRDD.foreachPartition(_ => {})
      val elapsedMs = getElapsedTimeFrequentPatternsMs
      val iter = freqRDD.toLocalIterator
      val numEdges = if (fractoids.head.parent == null) {
         fractoids.head.primitives.count(_ == Primitive.E)
      } else {
         fractoids.head.parent.pattern.getNumberOfEdges + 1
      }
      var numSubgraphs = 0L
      var numPatterns = 0L
      while (iter.hasNext) {
         val (pattern, support) = iter.next()
         numSubgraphs += support.getNumSubgraphsAggregated
         numPatterns += 1
         logApp(s"FrequentPattern numEdges=${numEdges}" +
            s" minSupport=${minSupport} pattern=${pattern} support=${support}")
      }

      logApp(s"FrequentPatternsResult" +
         s" numEdges=${numEdges}" +
         s" support=${minSupport}" +
         s" numSteps=${fractoids.size}" +
         s" numSubgraphs=${numSubgraphs}" +
         s" numPatterns=${numPatterns}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughput=${numSubgraphs / elapsedMs}")

      numPatterns
   }

   /**
    * Matches a pattern using Fractal, obtains the quick pattern -> supports
    * aggregation, transforms this aggregation into canonical aggregation and
    * returns the final mapping patterns -> supports as an RDD
    * @param fg fractal graph to enumerate from
    * @param pattern matching pattern
    * @return RDD of canonical patterns -> supports
    */
   protected def canonicalPatternsSupports(fg: FractalGraph, pattern: Pattern)
   : (Fractoid[EdgeInducedSubgraph], PatternsSupports) = {
      val fractoid = fg.pfractoid(pattern)
         .expand(pattern.getNumberOfVertices)
         .efractoid
         .expand(1)
      val aggregation = fractoid
         .aggregationObjObj(
            s => s.quickPattern(), value, aggregate, ReportFuncs.FSM_AGG_REPORT)
         .map { case (quickPatern,supp) =>
            val canonicalPattern = quickPatern.copy()
            canonicalPattern.turnCanonical()
            supp.handleConversionFromQuickToCanonical(quickPatern,
               canonicalPattern)
            (canonicalPattern, supp)
         }
         .reduceByKey((s1,s2) => {aggregate(s1, s2); s1})
      (fractoid, aggregation)
   }

   /**
    * Filters a pattern -> support by keeping only the frequent ones
    * @param patternsSupportsRDD mapping pattern -> support
    * @return filtered mapping pattern -> supports
    */
   protected def frequentPatternsSupports
   (patternsSupportsRDD: PatternsSupports): PatternsSupports = {
      patternsSupportsRDD.filter(_._2.hasEnoughSupport)
   }

   protected def getPatternWithPlan(pattern: Pattern): Pattern = {
      PatternExplorationPlan.apply(pattern).get(0)
   }

   override def apply(fg: FractalGraph): RDD[(Pattern, MinImageSupport)] = {
      val sc = fg.fractalContext.sparkContext
      val results = ArrayBuffer.empty[RDD[(Pattern,MinImageSupport)]]
      //try {
         compute(fg, results)
      //} catch {
      //   case e: Exception =>
      //      logWarn(s"InterruptedExecution exception=${e}. " +
      //         s"Returning: ${results}")
      //}
      sc.union(results.toSeq)
   }

   def compute(fg: FractalGraph,
               results: ArrayBuffer[PatternsSupports]): Unit = {
      val sc = fg.fractalContext.sparkContext

      // patterns -> supports
      val (firstFractoid, canonicalPatternsSupportsRDD) = {
         val fractoid = fg.efractoid.expand(1)
         val rdd = fractoid
            .aggregationObjObj(
               s => s.quickPattern(), value, aggregate)
            .map { case (quickPatern,supp) =>
               val canonicalPattern = quickPatern.copy()
               canonicalPattern.turnCanonical()
               supp.handleConversionFromQuickToCanonical(quickPatern,
                  canonicalPattern)
               (canonicalPattern, supp)
            }
            .reduceByKey((s1,s2) => {aggregate(s1, s2); s1})
         (fractoid, rdd)
      }

      materializeAndLogPartialResult(firstFractoid, canonicalPatternsSupportsRDD)

      // frequent patterns -> supports
      var frequentPatternsSupportsRDD =
         frequentPatternsSupports(canonicalPatternsSupportsRDD)

      var numFrequentPatterns =
         materializeAndLogResult(Seq(firstFractoid), frequentPatternsSupportsRDD)
      results += frequentPatternsSupportsRDD

      // stop condition
      var numEdges = 1
      var continue = numEdges < maxNumEdges && numFrequentPatterns > 0

      while (continue) {
         logApp(s"Extending ${numEdges}-edge frequent patterns.")

         // get valid candidate patterns extended from previous step
         val validCandPatternsRDD = frequentPatternsSupportsRDD.keys
         val patterns = validCandPatternsRDD.collect()

         val fractoidsAndCanonicalPatternsSupportsRDDs = patterns
            .map(patternWithoutPlan => {
               val canonicalPattern = patternWithoutPlan.copy()
               canonicalPattern.turnCanonical()
               patternWithoutPlan.setVertexLabeled(true)
               val pattern = getPatternWithPlan(patternWithoutPlan)
               val (fractoid, canonicalPatternsSupportsRDD) =
                  canonicalPatternsSupports(fg, pattern)
               materializeAndLogPartialResult(fractoid, canonicalPatternsSupportsRDD)
               (fractoid, canonicalPatternsSupportsRDD)
            })

         val (fractoids, canonicalPatternsSupportsRDDs) =
            fractoidsAndCanonicalPatternsSupportsRDDs.unzip

         frequentPatternsSupportsRDD = frequentPatternsSupports(
            sc.union(canonicalPatternsSupportsRDDs.toIndexedSeq)
               .reduceByKey((s1,s2) => {
                  aggregate(s1,s2)
                  s1
               })
         )


         // accumulate into final result RDDs
         numFrequentPatterns = materializeAndLogResult(fractoids,
            frequentPatternsSupportsRDD)
         canonicalPatternsSupportsRDDs.foreach(_.unpersist())
         results += frequentPatternsSupportsRDD

         // stop condition
         numEdges += 1
         continue = numEdges < maxNumEdges && numFrequentPatterns > 0
      }
   }
}
