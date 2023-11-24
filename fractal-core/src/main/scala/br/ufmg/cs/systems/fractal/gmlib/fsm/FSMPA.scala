package br.ufmg.cs.systems.fractal.gmlib.fsm

import br.ufmg.cs.systems.fractal.{FractalGraph, Fractoid, Primitive}
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternExplorationPlan, PatternUtils, PatternUtilsRDD}
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph
import br.ufmg.cs.systems.fractal.util.ReportFuncs
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

class FSMPA(minSupport: Int, maxNumEdges: Int)
   extends BuiltInApplication[RDD[(Pattern,MinImageSupport)]] {

   private var lastCurrentTimeMs: Long = System.currentTimeMillis()

   private var frequentPatternsCurrentTimeMs: Long = System.currentTimeMillis()

   // type aliases
   protected type PatternsSupports = RDD[(Pattern,MinImageSupport)]
   protected type Patterns = RDD[Pattern]

   // reusable support value
   protected val minImageSupport = new MinImageSupport(minSupport)

   // function to get key function
   protected def key(pattern: Pattern): PatternInducedSubgraph => Pattern =
      _.applyLabels(pattern)

   // value function: min image support
   protected val value: PatternInducedSubgraph => MinImageSupport = s => {
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
   (fractoid: Fractoid[PatternInducedSubgraph],
    freqRDD: RDD[(Pattern, MinImageSupport)]): (Long, Long) = {
      freqRDD.cache()
      freqRDD.foreachPartition(_ => {})
      val elapsedMs = getElapsedTimeMs
      val iter = freqRDD.toLocalIterator
      var numEdges = fractoid.primitives.count(_ == Primitive.E)
      var numSubgraphs = 0L
      var numPatterns = 0L
      while (iter.hasNext) {
         val (pattern, support) = iter.next()
         numSubgraphs += support.getNumSubgraphsAggregated
         numPatterns += 1
         logApp(s"FrequentPattern numEdges=${numEdges}" +
            s" minSupport=${minSupport} pattern=${pattern} support=${support}")
      }

      logApp(s"StepResult fractoid=${fractoid}" +
         s" numEdges=${numEdges}" +
         s" support=${minSupport}" +
         s" numSteps=1" +
         s" numSubgraphs=${numSubgraphs}" +
         s" numPatterns=${numPatterns}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughput=${numSubgraphs / elapsedMs.toDouble}")

      (numPatterns, numSubgraphs)
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
   : (Fractoid[PatternInducedSubgraph], PatternsSupports) = {
      val fractoid = fg.pfractoid(pattern).expand(pattern.getNumberOfVertices)
      val aggregation = fractoid
         .aggregationObjObj[Pattern,MinImageSupport](
            key(pattern), value, aggregate, ReportFuncs.FSM_AGG_REPORT)
         .map { case (quickPatern,supp) =>
            val canonicalPattern = quickPatern.copy()
            canonicalPattern.turnCanonical()
            supp.handleConversionFromQuickToCanonical(quickPatern,
               canonicalPattern)
            (canonicalPattern, supp)
         }
         .reduceByKey((s1,s2) => {s1.aggregate(s2); s1})
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

   /**
    * Filters a pattern -> support mapping by keeping only the infrequent ones
    * @param patternsSupportsRDD mapping patterns -> supports
    * @return filtered mapping patterns -> supports
    */
   protected def infrequentPatterns(patternsSupportsRDD: PatternsSupports)
   : Patterns = {
      patternsSupportsRDD.filter(!_._2.hasEnoughSupport).keys
   }

   /**
    * Gets the unique vertex labels from a mapping pattern -> supports
    * @param patternsSupportsRDD mapping patterns -> supports
    * @return set of unique labels
    */
   protected def uniqueLabels(patternsSupportsRDD: PatternsSupports): RDD[Int] = {
      patternsSupportsRDD
         .keys
         .flatMap(p => {
            val pedge = p.getEdges.get(0)
            Set(pedge.getSrcLabel, pedge.getDestLabel)
         })
         .distinct()
   }

   /**
    * Obtains a mapping of valid extended patterns from a set of frequent
    * patterns, a set of infrequent patterns, and a set of frequent labels.
    * @param frequentPatternsRDD frequent patterns
    * @param infrequentPatternsRDD infrequent patterns
    * @param frequentLabelsBc frequent labels
    * @return valid extended candidates
    */
   protected def validPatternCandidates(frequentPatternsRDD: Patterns,
                                      infrequentPatternsRDD: Patterns,
                                      frequentLabelsBc: Broadcast[Array[Int]])
   : Patterns = {
      val sc = frequentPatternsRDD.sparkContext

      val candPatternsRDD = frequentPatternsRDD
         .flatMap(p => {
            var patterns: Set[Pattern] = Set.empty
            for (label <- frequentLabelsBc.value) {
               val cur = PatternUtils.extendByEdge(p, label).cursor()
               while (cur.moveNext()) patterns = patterns + cur.elem()
            }
            patterns
         })
         .distinct(3 * sc.defaultParallelism)

      val invalidCandPatternsRDD = infrequentPatternsRDD
         .flatMap(p => {
            var patterns: Set[Pattern] = Set.empty
            for (label <- frequentLabelsBc.value) {
               val cur = PatternUtils.extendByEdge(p, label).cursor()
               while (cur.moveNext()) patterns = patterns + cur.elem()
            }
            patterns
         })
         .distinct(3 * sc.defaultParallelism)

      candPatternsRDD.subtract(invalidCandPatternsRDD)
   }

   protected def getPatternWithPlan(pattern: Pattern): Pattern = {
      PatternExplorationPlan.apply(pattern).get(0)
   }

   override def apply(fg: FractalGraph): RDD[(Pattern, MinImageSupport)] = {
      val sc = fg.fractalContext.sparkContext
      val results = ArrayBuffer.empty[RDD[(Pattern,MinImageSupport)]]
      try {
         compute(fg, results)
      } catch {
         case e: Exception =>
            logWarn(s"InterruptedExecution exception=${e}. " +
               s"Returning: ${results}")
      }
      sc.union(results.toSeq)
   }

   def compute(fg: FractalGraph,
               results: ArrayBuffer[RDD[(Pattern,MinImageSupport)]]): Unit = {
      val fc = fg.fractalContext
      val sc = fc.sparkContext
      var canonicalPatternsSupportsRDDs = List.empty[PatternsSupports]
      import scala.concurrent.ExecutionContext.Implicits.global

      // Frequent edges and labels {

      // patterns -> supports
      val (firstFractoid, canonicalPatternsSupportsRDD) = {
         val patternWithoutPlan = PatternUtils.singleEdgePattern()
         patternWithoutPlan.setVertexLabeled(false)
         val pattern = getPatternWithPlan(patternWithoutPlan)
         canonicalPatternsSupports(fg, pattern)
      }
      canonicalPatternsSupportsRDD.cache()
      canonicalPatternsSupportsRDDs = canonicalPatternsSupportsRDD :: canonicalPatternsSupportsRDDs

      // frequent patterns -> supports
      var frequentPatternsSupportsRDD =
         frequentPatternsSupports(canonicalPatternsSupportsRDD)

      frequentPatternsSupportsRDD.cache()
      val (numFrequentPatternsPattern, numSubgraphs) =
         materializeAndLogPartialResult(firstFractoid, frequentPatternsSupportsRDD)
      if (numFrequentPatternsPattern > 0) {
         results += frequentPatternsSupportsRDD
      }
      val elapsedMs = getElapsedTimeFrequentPatternsMs
      var numEdges = 0
      logApp(s"FrequentPatternsResult" +
         s" numEdges=${numEdges + 1}" +
         s" support=${minSupport}" +
         s" numSteps=${1}" +
         s" numSubgraphs=${numSubgraphs}" +
         s" numPatterns=${numFrequentPatternsPattern}" +
         s" elapsedMs=${elapsedMs}" +
         s" throughput=${numSubgraphs / elapsedMs}")

      // frequent labels
      val frequentLabelsBc = sc.broadcast(
         uniqueLabels(frequentPatternsSupportsRDD).collect()
      )

      // infrequent patterns
      var infrequentPatternsRDD = infrequentPatterns(canonicalPatternsSupportsRDD)

      // } Frequent edges and labels

      // stop condition
      numEdges += 1
      var continue = numEdges < maxNumEdges && numFrequentPatternsPattern > 0

      while (continue) {
         var numFrequentPatterns = 0L
         var numSubgraphsTotal = 0L

         // get valid candidate patterns extended from previous step
         val validCandPatternsRDD = validPatternCandidates(
            frequentPatternsSupportsRDD.keys,
            infrequentPatternsRDD,
            frequentLabelsBc)

         // compute pattern/supports of all patterns in parallel
         val patterns = validCandPatternsRDD.collect()

         // uncache previous candidate pattern supports
         canonicalPatternsSupportsRDDs.foreach(_.unpersist())
         canonicalPatternsSupportsRDDs = List.empty

         // partial results (pattern by pattern)
         var lastFrequentPatternsRDDs = List.empty[PatternsSupports]
         var lastInfrequentPatternsRDDs = List.empty[Patterns]

         val iter = validCandPatternsRDD.toLocalIterator
         while (iter.hasNext) {
            val patternWithoutPlan = iter.next()
            patternWithoutPlan.setVertexLabeled(true)
            val pattern = getPatternWithPlan(patternWithoutPlan)
            val (fractoid, canonicalPatternsSupportsRDD) =
               canonicalPatternsSupports(fg, pattern)
            canonicalPatternsSupportsRDD.cache()
            val rddFreq = frequentPatternsSupports(canonicalPatternsSupportsRDD)
            rddFreq.cache()
            val rddInfreq = infrequentPatterns(canonicalPatternsSupportsRDD)
            val (numPatterns, numSubgraphs) =
               materializeAndLogPartialResult(fractoid, rddFreq)
            lastFrequentPatternsRDDs = rddFreq :: lastFrequentPatternsRDDs
            lastInfrequentPatternsRDDs = rddInfreq :: lastInfrequentPatternsRDDs
            canonicalPatternsSupportsRDDs =
               canonicalPatternsSupportsRDD :: canonicalPatternsSupportsRDDs
            if (numPatterns > 0) {
               numFrequentPatterns += numPatterns
               numSubgraphsTotal += numSubgraphs
               results.synchronized {
                  results += rddFreq
               }
            }
         }

         val elapsedMs = getElapsedTimeFrequentPatternsMs
         logApp(s"FrequentPatternsResult" +
            s" numEdges=${numEdges + 1}" +
            s" support=${minSupport}" +
            s" numSteps=${patterns.size}" +
            s" numSubgraphs=${numSubgraphsTotal}" +
            s" numPatterns=${numFrequentPatterns}" +
            s" elapsedMs=${elapsedMs}" +
            s" throughput=${numSubgraphsTotal / elapsedMs}")

         // assemble results
         frequentPatternsSupportsRDD = sc.union(lastFrequentPatternsRDDs)
         infrequentPatternsRDD = sc.union(lastInfrequentPatternsRDDs)

         // stop condition
         numEdges += 1
         continue = numEdges < maxNumEdges && numFrequentPatterns > 0
      }

      // uncache previous candidate pattern supports
      canonicalPatternsSupportsRDDs.foreach(_.unpersist())
      canonicalPatternsSupportsRDDs = List.empty
   }
}
