package br.ufmg.cs.systems.fractal.gmlib.fsm

import br.ufmg.cs.systems.fractal.FractalGraph
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternExplorationPlan, PatternUtilsRDD}
import br.ufmg.cs.systems.fractal.subgraph.EdgeInducedSubgraph
import br.ufmg.cs.systems.fractal.util.Logging
import com.koloboke.collect.set.ObjSet
import com.koloboke.collect.set.hash.HashObjSets
import org.apache.spark.rdd.RDD

class FSMHybrid(minSupport: Int, maxNumEdges: Int)
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
            s => s.quickPattern(), value, aggregate)
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

   protected def getPatternWithPlan(pattern: Pattern): Pattern = {
      PatternExplorationPlan.apply(pattern).get(0)
   }

   override def apply(fg: FractalGraph): RDD[(Pattern, MinImageSupport)] = {
      val sc = fg.fractalContext.sparkContext

      var frequentPatternsSupportsRDDs: List[PatternsSupports] = List.empty
      var canonicalPatternsSupportsRDDs: List[PatternsSupports] = List.empty

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
            .reduceByKey((s1,s2) => {s1.aggregate(s2); s1})

         canonicalPatternsSupportsRDDs = rdd :: canonicalPatternsSupportsRDDs
         rdd
      }

      // frequent patterns -> supports
      var frequentPatternsSupportsRDD = {
         val rdd = frequentPatternsSupports(canonicalPatternsSupportsRDD).cache()
         frequentPatternsSupportsRDDs = rdd :: frequentPatternsSupportsRDDs
         rdd
      }

      // stop condition
      var numEdges = 1
      var continue = numEdges < maxNumEdges &&
         !frequentPatternsSupportsRDD.isEmpty()

      while (continue) {

         // get valid candidate patterns extended from previous step
         val validCandPatternsRDD = frequentPatternsSupportsRDD.keys

         var canonicalPatternsSupportsRDDs = List.empty[PatternsSupports]

         val iter = PatternUtilsRDD.localIterator(validCandPatternsRDD)
         val frequentPatterns: ObjSet[Pattern] = HashObjSets.newMutableSet()
         while (iter.hasNext) {
            val patternWithoutPlan = iter.next()
            val canonicalPattern = patternWithoutPlan.copy()
            canonicalPattern.turnCanonical()

            if (!frequentPatterns.contains(canonicalPattern)) {
               // patterns -> supports
               val canonicalPatternsSupportsRDD = {
                  patternWithoutPlan.setVertexLabeled(true)
                  val pattern = getPatternWithPlan(patternWithoutPlan)
                  val rdd = canonicalPatternsSupports(fg, pattern).cache()
                  canonicalPatternsSupportsRDDs = rdd :: canonicalPatternsSupportsRDDs
                  rdd
               }

               val frequentRDD = frequentPatternsSupports(canonicalPatternsSupportsRDD).keys
               val localIter = PatternUtilsRDD.localIterator(frequentRDD)
               while (localIter.hasNext) frequentPatterns.add(localIter.next())
            }
         }

         frequentPatternsSupportsRDD = frequentPatternsSupports(
            sc.union(canonicalPatternsSupportsRDDs)
               .reduceByKey((s1,s2) => {
                  s1.aggregate(s2)
                  s1
               })
         ).cache()

         // accumulate into final result RDDs
         frequentPatternsSupportsRDDs =
            frequentPatternsSupportsRDD :: frequentPatternsSupportsRDDs

         // stop condition
         numEdges += 1
         continue = numEdges < maxNumEdges &&
            !frequentPatternsSupportsRDD.isEmpty()
      }

      // final result RDD
      val frequentPatternSupportRDD = sc.union(frequentPatternsSupportsRDDs)
         .cache()
         .setName("fsmHybrid(FrequentPatternsSupports)")

      // materialize final result to unpersist others
      frequentPatternSupportRDD.foreachPartition(_ => {})

      // unpersist any RDD cached in this call (after materialization)
      frequentPatternsSupportsRDDs.foreach(_.unpersist())
      canonicalPatternsSupportsRDDs.foreach(_.unpersist())

      frequentPatternSupportRDD
   }
}
