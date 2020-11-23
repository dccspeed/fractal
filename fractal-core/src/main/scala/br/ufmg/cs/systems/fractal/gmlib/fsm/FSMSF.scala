package br.ufmg.cs.systems.fractal.gmlib.fsm

import br.ufmg.cs.systems.fractal.{FractalGraph, Fractoid}
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternUtilsRDD}
import br.ufmg.cs.systems.fractal.subgraph.EdgeInducedSubgraph
import br.ufmg.cs.systems.fractal.util.collection.ObjSet
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class FSMSF(minSupport: Int, maxNumEdges: Int)
   extends BuiltInApplication[RDD[(Pattern,MinImageSupport)]] {

   // reusable pattern key
   private val key: EdgeInducedSubgraph => Pattern = s => s.quickPattern()

   // reusable minimum image support value
   private val minImageSupp = new MinImageSupport(minSupport)

   // reusable support value
   private val value: EdgeInducedSubgraph => MinImageSupport = s => {
      minImageSupp.setSubgraph(s)
      minImageSupp
   }

   // support aggregation function
   private val aggregate: (MinImageSupport,MinImageSupport) => Unit =
      (minImageSupp1, minImageSupp2) => {
         minImageSupp1.aggregate(minImageSupp2)
      }

   /**
    * Aggregates subgraphs by patterns (quick) and support
    * @param frac computation to search for subgraphs
    * @return collection of quick patterns -> supports
    */
   private def quickPatternsSupports(frac: Fractoid[EdgeInducedSubgraph])
   : RDD[(Pattern,MinImageSupport)] = {
      frac.aggregationObjObj[Pattern,MinImageSupport](key, value, aggregate)
   }

   /**
    * Transforms a collection of quick patterns -> supports into a collection
    * of canonical patterns -> (quick patterns, support). We maintain the
    * quick patterns for each canonical patterns for downstream filtering.
    * @param quickPatternRDD quick patterns -> supports collection
    * @return new collection of canonical patterns -> (quick patterns, support)
    */
   private def canonicalPatternsSupports
   (quickPatternRDD: RDD[(Pattern, MinImageSupport)])
   : RDD[(Pattern,(ObjSet[Pattern],MinImageSupport))] = {
      quickPatternRDD
         .map { case (quickPatern,supp) =>
            val canonicalPattern = quickPatern.copy()
            canonicalPattern.turnCanonical()
            supp.handleConversionFromQuickToCanonical(quickPatern, canonicalPattern)
            val quickPatterns = new ObjSet[Pattern]()
            quickPatterns.add(quickPatern)
            (canonicalPattern, (quickPatterns,supp))
         }
         .reduceByKey { case ((p1, s1), (p2, s2)) =>
            p1.addAll(p2)
            s1.aggregate(s2)
            (p1, s1)
         }
   }

   /**
    * Filters infrequent patterns from the collection
    * @param patternSupportRDD canonical patterns -> (quick patterns, support)
    * @return new collection
    */
   private def frequentPatternsSupports
   (patternSupportRDD: RDD[(Pattern,(ObjSet[Pattern],MinImageSupport))])
   : RDD[(Pattern,(ObjSet[Pattern],MinImageSupport))] = {
      patternSupportRDD.filter(_._2._2.hasEnoughSupport)
   }

   /**
    * Gets a collection of quick patterns
    * @param patternsSupports collection of canonical patterns -> (quick,
    *                         patterns, support)
    * @return collection of quick patterns
    */
   private def quickPatternsRDD
   (patternsSupports: RDD[(Pattern,(ObjSet[Pattern],MinImageSupport))])
   : RDD[Pattern] = {
      patternsSupports.map(kv => kv._2._1).flatMap(patterns => {
         val patternsArray = new Array[Pattern](patterns.size())
         patterns.underlying().toArray(patternsArray)
      })
   }

   /**
    * Remove quick patterns from the collection for final result
    * @param frequentPatternsSupports original collection
    * @return new collection of canonical patterns -> supports
    */
   private def frequentCanonicalPatternsSupports
   (frequentPatternsSupports: RDD[(Pattern,(ObjSet[Pattern],MinImageSupport))])
   : RDD[(Pattern,MinImageSupport)] = {
      frequentPatternsSupports.map(kv => (kv._1, kv._2._2))
   }

   /**
    * Generates a spark broadcast of quick patterns
    * @param frequentQuickPatternsRDD quick patterns collection
    * @param quickPatterns cumulative quick pattern set (local)
    * @return broadcast variable
    */
   private def broadcastQuickPatterns(frequentQuickPatternsRDD: RDD[Pattern],
                                      quickPatterns: ObjSet[Pattern])
   : Broadcast[ObjSet[Pattern]] = {
      val sc = frequentQuickPatternsRDD.sparkContext
      val iter = PatternUtilsRDD.localIterator(frequentQuickPatternsRDD)
      while (iter.hasNext) {
         quickPatterns.add(iter.next())
      }
      sc.broadcast(quickPatterns)
   }

   override def apply(fg: FractalGraph): RDD[(Pattern, MinImageSupport)] = {
      val sc = fg.fractalContext.sparkContext
      if (maxNumEdges < 1) return sc.emptyRDD

      // final frequent patterns -> supports RDDs
      var frequentPatternSupportRDDs = List.empty[RDD[(Pattern,MinImageSupport)]]

      // quick patterns to broadcast on each iteration
      val quickPatterns = new ObjSet[Pattern]()

      // cumulative fractoid or exploring edge-induced search space
      var frac = fg.efractoid.expand(1)

      // quick patterns -> supports
      var quickPatternMapRDD = quickPatternsSupports(frac)

      // canonical patterns -> (quick patterns, support)
      var stepPatternSupportRDD = canonicalPatternsSupports(quickPatternMapRDD)

      // frequent patterns
      var frequentPatternsSupportsRDD = {
         val rdd = frequentPatternsSupports(stepPatternSupportRDD).cache()
         val freqRDD = frequentCanonicalPatternsSupports(rdd).cache()
         frequentPatternSupportRDDs = freqRDD :: frequentPatternSupportRDDs
         freqRDD.foreachPartition(_ => {})
         rdd
      }

      // frequent quick patterns
      var frequentQuickPatternsRDD =
         quickPatternsRDD(frequentPatternsSupportsRDD)

      // stop condition
      var numEdges = 1
      var continue = numEdges < maxNumEdges &&
         !frequentQuickPatternsRDD.isEmpty()

      while (continue) {
         // broadcast quick patterns
         val quickPatternsBc = broadcastQuickPatterns(
            frequentQuickPatternsRDD, quickPatterns)

         // clean last quick RDD
         frequentPatternsSupportsRDD.unpersist()

         // add a new edge to candidate subgraphs
         frac = frac
            .filter((s,c) => quickPatternsBc.value.contains(s.quickPattern))
            .expand(1)

         // quick pattern -> support
         quickPatternMapRDD = quickPatternsSupports(frac)

         // canonical pattern -> (quick patterns, support)
         stepPatternSupportRDD = canonicalPatternsSupports(quickPatternMapRDD)

         // frequent patterns
         frequentPatternsSupportsRDD = {
            val rdd = frequentPatternsSupports(stepPatternSupportRDD).cache()
            val freqRDD = frequentCanonicalPatternsSupports(rdd).cache()
            frequentPatternSupportRDDs = freqRDD :: frequentPatternSupportRDDs
            freqRDD.foreachPartition(_ => {})
            rdd
         }

         // frequent quick patterns
         frequentQuickPatternsRDD =
            quickPatternsRDD(frequentPatternsSupportsRDD)

         // stop condition
         numEdges += 1
         continue = numEdges < maxNumEdges &&
            !frequentQuickPatternsRDD.isEmpty()

         // unpersist broadcast
         quickPatternsBc.unpersist()
      }

      // union of all partial results
      val frequentPatternSupportRDD = sc.union(frequentPatternSupportRDDs)
         .cache()

      // materialize result
      frequentPatternSupportRDD.foreachPartition(_ => {})

      // unpersist cached RDDs no longer necessary
      frequentPatternSupportRDDs.foreach(_.unpersist())
      frequentPatternsSupportsRDD.unpersist()

      frequentPatternSupportRDD
   }
}
