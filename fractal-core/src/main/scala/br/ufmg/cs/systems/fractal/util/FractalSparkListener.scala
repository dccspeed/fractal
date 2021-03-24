package br.ufmg.cs.systems.fractal.util

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}

abstract class FractalSparkListener extends SparkListener with Logging {

   override final def onStageCompleted
   (stageCompleted: SparkListenerStageCompleted): Unit = {
      val iter = stageCompleted.stageInfo.accumulables.valuesIterator
      while (iter.hasNext) {
         val accumInfo = iter.next()
         if (accumInfo.name.getOrElse("") == "THREAD_STATS") {
            val threadStats = accumInfo.value.get
               .asInstanceOf[java.util.List[ThreadStats]]
            val threadStatsArray = threadStats
               .toArray(new Array[ThreadStats](threadStats.size()))
            onFractalStepCompleted(threadStatsArray)
         }
      }
   }

   def onFractalStepCompleted(stageThreadStatuses: Array[ThreadStats]): Unit
}
