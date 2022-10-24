package br.ufmg.cs.systems.fractal.gmlib.motifs

import br.ufmg.cs.systems.fractal.FractalGraph
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternExplorationPlan, PatternUtilsRDD}
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.rdd.RDD

class MotifsHybrid(numVertices: Int)
   extends BuiltInApplication[RDD[(Pattern,Long)]] {

      override def apply(fg: FractalGraph): RDD[(Pattern, Long)] = {
         val sc = fg.fractalContext.sparkContext

         // final result accumulated in this
         var motifCountRDDs = List.empty[RDD[(Pattern,Long)]]

         // canonical patterns RDD
         val canonicalPatternsRDD = PatternUtilsRDD
            .getOrGenerateVertexPatternsRDD(sc, numVertices - 1)

         // local iterator for better memory footprint
         val iter = PatternUtilsRDD.localIterator(canonicalPatternsRDD)
         while (iter.hasNext) {
            val patternWithoutPlan = iter.next()

            /**
             * Motifs counting consider induced subgraphs, unlabeled at first
             */
            patternWithoutPlan.setInduced(true)
            patternWithoutPlan.setVertexLabeled(false)

            /**
             * Naive exploration plan
             */
            val pattern = PatternExplorationPlan.apply(patternWithoutPlan).get(0)

            pattern.updateSymmetryBreakerVertexUnlabeled()

            /**
             * Motifs counting RDD
             */
            val mappingRDD = fg.pfractoid(pattern)
               .expand(numVertices - 1)
               .vfractoid
               .expand(1)
               .aggregationCanonicalPatternLong(
                  s => {
                     s.quickPattern()
                  },0L, _ => 1L, _ + _)

            motifCountRDDs = mappingRDD :: motifCountRDDs
         }

         sc.union(motifCountRDDs)
      }
   }
