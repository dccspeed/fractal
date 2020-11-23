package br.ufmg.cs.systems.fractal.gmlib.motifs

import br.ufmg.cs.systems.fractal.FractalGraph
import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternExplorationPlan, PatternExplorationPlanMCVC, PatternUtilsRDD}
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph
import br.ufmg.cs.systems.fractal.util.SubgraphCallback
import org.apache.spark.rdd.RDD

class MotifsPFMCVC(numVertices: Int)
   extends BuiltInApplication[RDD[(Pattern,Long)]] {

      override def apply(fg: FractalGraph): RDD[(Pattern, Long)] = {
         val sc = fg.fractalContext.sparkContext

         // final result accumulated in this var
         var motifCountRDDs = List.empty[RDD[(Pattern,Long)]]

         // canonical patterns RDD
         val canonicalPatternsRDD = PatternUtilsRDD.vertexPatternsRDD(
            sc, numVertices)

         // local iterator for better memory footprint
         val iter = PatternUtilsRDD.localIterator(canonicalPatternsRDD)
         while (iter.hasNext) {
            val patternWithoutPlan = iter.next()
            patternWithoutPlan.setInduced(true)
            patternWithoutPlan.setVertexLabeled(false)

            val pattern = PatternExplorationPlanMCVC
               .apply(patternWithoutPlan).get(0)

            val mcvcSize = pattern.explorationPlan().mcvcSize()

            val callback: (
               PatternInducedSubgraph,
                  Computation[PatternInducedSubgraph],
                  SubgraphCallback[PatternInducedSubgraph]) => Unit =
               (s,c,cb) => {
                  s.completeMatch(c, pattern, cb)
               }

            val partialMapRDD = fg.pfractoid(pattern)
               .expand(mcvcSize)
               .aggregationCanonicalPatternLongWithCallback(
                  s => s.applyLabels(pattern),0L, _ => 1L,
                  _ + _, callback)

            motifCountRDDs = partialMapRDD :: motifCountRDDs
         }

         sc.union(motifCountRDDs)
      }
   }
