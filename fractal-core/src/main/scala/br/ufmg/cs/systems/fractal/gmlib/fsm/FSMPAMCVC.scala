package br.ufmg.cs.systems.fractal.gmlib.fsm

import br.ufmg.cs.systems.fractal.{FractalGraph, Fractoid}
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternExplorationPlanMCVC}
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph
import br.ufmg.cs.systems.fractal.util.ScalaFractalFuncs.CustomSubgraphCallback

class FSMPAMCVC(minSupport: Int, maxNumEdges: Int)
   extends FSMPA(minSupport, maxNumEdges) {

   /**
    * Matches a pattern using Fractal, obtains the quick pattern -> supports
    * aggregation, transforms this aggregation into canonical aggregation and
    * returns the final mapping patterns -> supports as an RDD
    * @param fg fractal graph to enumerate from
    * @param pattern matching pattern
    * @return RDD of canonical patterns -> supports
    */
   override protected def canonicalPatternsSupports(fg: FractalGraph,
      pattern: Pattern)
   : (Fractoid[PatternInducedSubgraph], PatternsSupports) = {
      val explorationPlanMCVC = pattern.explorationPlan()
      val mcvcSize = explorationPlanMCVC.mcvcSize()

      val callback: CustomSubgraphCallback[PatternInducedSubgraph] =
         (s,c,cb) => {
            s.completeMatch(c, c.getPattern, cb)
         }

      val fractoid = fg.pfractoid(pattern)
         .extend(mcvcSize)
      val aggregation = fractoid
         .aggregationObjObjWithCallback[Pattern,MinImageSupport](
            key(pattern), value, aggregate, callback)
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

   override protected def getPatternWithPlan(pattern: Pattern): Pattern = {
      val plans = PatternExplorationPlanMCVC.apply(pattern)
      if (plans == null) throw new RuntimeException(s"${plans} ${pattern}")
      plans.get(0)
   }
}
