package br.ufmg.cs.systems.fractal.util

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}

abstract class FractalSparkListener(val threadStatsKey: String)
   extends SparkListener with Logging {

   override final def onStageCompleted
   (stageCompleted: SparkListenerStageCompleted): Unit = {
      val accumId = FractalSparkListener.threadStatsKeyToAccumId.get(threadStatsKey)
      val accums = stageCompleted.stageInfo.accumulables
      accums.get(accumId) match {
         case Some(accumInfo) =>
            val threadStats = accumInfo.value.get
               .asInstanceOf[java.util.List[FractalThreadStats]]
            val threadStatsArray = threadStats
               .toArray(new Array[FractalThreadStats](threadStats.size()))
            onFractalStepCompleted(threadStatsArray)
            FractalSparkListener.threadStatsKeyToAccumId.remove(threadStatsKey)
         case None =>
      }
   }

   def onFractalStepCompleted(stageThreadStatuses: Array[FractalThreadStats]): Unit
}

object FractalSparkListener {
   val threadStatsKeyToAccumId: ConcurrentHashMap[String,Long] =
      new ConcurrentHashMap[String,Long]()
}
