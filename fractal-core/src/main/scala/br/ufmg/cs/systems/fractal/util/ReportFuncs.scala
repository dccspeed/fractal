package br.ufmg.cs.systems.fractal.util

import br.ufmg.cs.systems.fractal.Primitive
import br.ufmg.cs.systems.fractal.aggregation.{LongSubgraphAggregation, ObjObjSubgraphAggregation}
import br.ufmg.cs.systems.fractal.computation.ExecutionEngine
import br.ufmg.cs.systems.fractal.gmlib.fsm.MinImageSupport
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.subgraph.Subgraph
import com.koloboke.collect.map.ObjObjMap

import java.util.function.ToLongFunction

object ReportFuncs {
   val COUNT_AGG_REPORT: (ExecutionEngine[_ <: Subgraph],LongSubgraphAggregation[_ <: Subgraph]) => Unit =
      (engine, agg) => {
         val progress = agg.value()
         Logging.logApp(s"ProgressStepStageThread ${engine.getStep}" +
            s" ${engine.getStageId} ${engine.getPartitionId} ${progress}")
      }

   val FSM_AGG_REPORT
   : (ExecutionEngine[_ <: Subgraph],ObjObjSubgraphAggregation[_ <: Subgraph,Pattern,MinImageSupport]) => Unit =
      (engine,agg) => {
         val numEdges = engine.primitives().count(_ == Primitive.E)
         var progress = 0L
         var exception = false

         // try to get progress (best-effort)
         try {
            val map = agg.getKeyValueMap
            val cur = map.values().cursor()
            while (cur.moveNext()) {
               val s = cur.elem()
               if (s.hasEnoughSupport) progress += s.getNumSubgraphsAggregated
            }
         } catch {
            case _: Throwable =>
               exception = true
         }

         Logging.logApp(s"ProgressStepStageThreadEdgesException" +
            s" ${engine.getStep} ${engine.getStageId}" +
            s" ${engine.getPartitionId} ${numEdges} ${exception} ${progress}")
      }
}
