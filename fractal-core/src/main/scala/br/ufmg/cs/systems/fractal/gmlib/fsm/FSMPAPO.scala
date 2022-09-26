package br.ufmg.cs.systems.fractal.gmlib.fsm

import br.ufmg.cs.systems.fractal.FractalGraph
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternExplorationPlan}
import br.ufmg.cs.systems.fractal.subgraph.EdgeInducedSubgraph
import br.ufmg.cs.systems.fractal.util.ReportFuncs
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class FSMPAPO(minSupport: Int, maxNumEdges: Int)
   extends BuiltInApplication[RDD[(Pattern,MinImageSupport)]] {

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
         .efractoid
         .expand(1)
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
               results: ArrayBuffer[PatternsSupports]): Unit = {
      val sc = fg.fractalContext.sparkContext

      // patterns -> supports
      val canonicalPatternsSupportsRDD = {
         val rdd = fg.efractoid.expand(1)
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
         rdd
      }

      // frequent patterns -> supports
      var frequentPatternsSupportsRDD =
         frequentPatternsSupports(canonicalPatternsSupportsRDD)

      frequentPatternsSupportsRDD.cache()
      var numFrequentPatterns = frequentPatternsSupportsRDD.count()
      results += frequentPatternsSupportsRDD

      // stop condition
      var numEdges = 1
      var continue = numEdges < maxNumEdges && numFrequentPatterns > 0

      while (continue) {
         logApp(s"Extending ${numEdges}-edge frequent patterns.")

         // get valid candidate patterns extended from previous step
         val validCandPatternsRDD = frequentPatternsSupportsRDD.keys
         val patterns = validCandPatternsRDD.collect()

         val canonicalPatternsSupportsRDDs = patterns
            .map(patternWithoutPlan => {
               val canonicalPattern = patternWithoutPlan.copy()
               canonicalPattern.turnCanonical()
               patternWithoutPlan.setVertexLabeled(true)
               val pattern = getPatternWithPlan(patternWithoutPlan)
               canonicalPatternsSupports(fg, pattern)
            })

         frequentPatternsSupportsRDD = frequentPatternsSupports(
            sc.union(canonicalPatternsSupportsRDDs)
               .reduceByKey((s1,s2) => {
                  aggregate(s1,s2)
                  s1
               })
         )

         // accumulate into final result RDDs
         frequentPatternsSupportsRDD.cache()
         numFrequentPatterns = frequentPatternsSupportsRDD.count()
         results += frequentPatternsSupportsRDD

         // stop condition
         numEdges += 1
         continue = numEdges < maxNumEdges && numFrequentPatterns > 0
      }
   }
}
