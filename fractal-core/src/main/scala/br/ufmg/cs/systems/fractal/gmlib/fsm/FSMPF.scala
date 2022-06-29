package br.ufmg.cs.systems.fractal.gmlib.fsm

import br.ufmg.cs.systems.fractal.FractalGraph
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternExplorationPlan, PatternUtils, PatternUtilsRDD}
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

class FSMPF(minSupport: Int, maxNumEdges: Int)
   extends BuiltInApplication[RDD[(Pattern,MinImageSupport)]] {

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

   /**
    * Matches a pattern using Fractal, obtains the quick pattern -> supports
    * aggregation, transforms this aggregation into canonical aggregation and
    * returns the final mapping patterns -> supports as an RDD
    * @param fg fractal graph to enumerate from
    * @param pattern matching pattern
    * @return RDD of canonical patterns -> supports
    */
   protected def canonicalPatternsSupports(fg: FractalGraph, pattern: Pattern)
   : PatternsSupports = {
      fg.pfractoid(pattern)
         .expand(pattern.getNumberOfVertices)
         .aggregationObjObj[Pattern,MinImageSupport](
            key(pattern), value, aggregate)
         .map { case (quickPatern,supp) =>
            val canonicalPattern = quickPatern.copy()
            canonicalPattern.turnCanonical()
            supp.handleConversionFromQuickToCanonical(quickPatern,
               canonicalPattern)
            (canonicalPattern, supp)
         }
         .reduceByKey((s1,s2) => {s1.aggregate(s2); s1})
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
      sc.union(results)
   }

   def compute(fg: FractalGraph,
               results: ArrayBuffer[RDD[(Pattern,MinImageSupport)]]): Unit = {
      val fc = fg.fractalContext
      val sc = fc.sparkContext
      var canonicalPatternsSupportsRDDs = List.empty[PatternsSupports]
      import scala.concurrent.ExecutionContext.Implicits.global

      // Frequent edges and labels {

      // patterns -> supports
      val canonicalPatternsSupportsRDD = {
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
      val numFrequentPatternsPattern = frequentPatternsSupportsRDD.count()
      if (numFrequentPatternsPattern > 0) {
         results += frequentPatternsSupportsRDD
      }

      // frequent labels
      val frequentLabelsBc = sc.broadcast(
         uniqueLabels(frequentPatternsSupportsRDD).collect()
      )

      // infrequent patterns
      var infrequentPatternsRDD = infrequentPatterns(canonicalPatternsSupportsRDD)

      // } Frequent edges and labels

      // stop condition
      var numEdges = 1
      var continue = numEdges < maxNumEdges && numFrequentPatternsPattern > 0

      while (continue) {
         var numFrequentPatterns = 0L

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

         // compute patterns and supports in parallel
         val futures = patterns
            .map(patternWithoutPlan => {
               patternWithoutPlan.setVertexLabeled(true)
               val pattern = getPatternWithPlan(patternWithoutPlan)
               val canonicalPatternsSupportsRDD = canonicalPatternsSupports(fg, pattern)
               canonicalPatternsSupportsRDD.cache()
               val rddFreq = frequentPatternsSupports(canonicalPatternsSupportsRDD)
               rddFreq.cache()
               val rddInfreq = infrequentPatterns(canonicalPatternsSupportsRDD)
               Future {
                  (canonicalPatternsSupportsRDD,
                     rddFreq, rddFreq.count(), rddInfreq)
               }
            })

         // partial results (pattern by pattern)
         var lastFrequentPatternsRDDs = List.empty[PatternsSupports]
         var lastInfrequentPatternsRDDs = List.empty[Patterns]

         // verify results
         var failure = false
         for (f <- futures) {
            Await.ready(f, Duration.Inf)
            f.value.get match {
               case Success((canonicalPatternsSupportsRDD, rddFreq,
               numFrequentPatternsPattern, rddInfreq)) =>
                  lastFrequentPatternsRDDs = rddFreq :: lastFrequentPatternsRDDs
                  lastInfrequentPatternsRDDs = rddInfreq :: lastInfrequentPatternsRDDs
                  canonicalPatternsSupportsRDDs =
                     canonicalPatternsSupportsRDD :: canonicalPatternsSupportsRDDs
                  if (numFrequentPatternsPattern > 0) {
                     numFrequentPatterns += numFrequentPatternsPattern
                     results.synchronized {
                        results += rddFreq
                     }
                  }

               case Failure(e) =>
                  logWarn(s"ExecutionFailed future=${f} exception=${e}")
                  failure = true
            }
         }

         if (failure) throw new RuntimeException("SomeJobFailed")

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
