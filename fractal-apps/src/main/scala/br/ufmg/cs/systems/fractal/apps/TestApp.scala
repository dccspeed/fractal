package br.ufmg.cs.systems.fractal.apps

import java.util

import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternExplorationPlan, PatternUtils, PatternUtilsRDD}
import br.ufmg.cs.systems.fractal.profiler.StackTrace
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph
import br.ufmg.cs.systems.fractal.util.{FractalSparkListener, Logging, ThreadStats}
import br.ufmg.cs.systems.fractal.util.collection.{IntArrayList, ObjArrayList}
import com.koloboke.collect.map.hash.{HashObjIntMap, HashObjIntMaps}
import one.profiler.{AsyncProfiler, Events}
import org.apache.spark.deploy.{SparkSubmit, SparkSubmitUtils}
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{ExecutorPlugin, SparkConf, SparkContext, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.HashSet
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration.Duration

object TestApp extends Logging {
   def main(args: Array[String]): Unit = {
      // environment setup
      val conf = new SparkConf().setAppName("MyMotifsApp")
      val sc = new SparkContext(conf)
      val fc = new FractalContext(sc, "info")
      val graphPath = args(0) // input graph
      val numVertices = args(1).toInt // number of vertices
      val fgraph = fc.textFile(graphPath)
         .set("ws_internal", true)
         .set("ws_external", false)

      sc.addSparkListener(
         new FractalSparkListener {
            override def onFractalStepCompleted
            (stageThreadStatuses: Array[ThreadStats]): Unit = {
               logApp(s"ThreadStatuses\n${stageThreadStatuses.mkString("\n")}")
            }
         }
      )

      val rdd = fgraph.vfractoid.expand(3)
         .aggregationLongLong(s => 1L, 0L, s => 1L, _ + _)

      rdd.count()
      rdd.count()

      // environment cleaning
      fc.stop()
      sc.stop()
   }
}
