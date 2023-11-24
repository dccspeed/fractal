package br.ufmg.cs.systems.fractal

import br.ufmg.cs.systems.fractal.computation.{ActorMessageSystem, LocalComputationStore}
import br.ufmg.cs.systems.fractal.conf.Configuration
import br.ufmg.cs.systems.fractal.graph.MainGraphStore
import br.ufmg.cs.systems.fractal.profiler.FractalProfiler
import br.ufmg.cs.systems.fractal.subgraph.Subgraph
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList
import br.ufmg.cs.systems.fractal.util.{FractalPerformanceDiagnose, FractalSparkListener, FractalThreadStats, Logging}
import one.profiler.Events
import org.apache.spark.SparkContext
import spire.ClassTag

import java.util.Random
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

/**
 * Starting point for Fractal execution engine (currently Spark)
 * @param sc Spark context
 * @param logLevel log level: "error", "warn", "info"
 */
class FractalContext(sc: SparkContext, logLevel: String = "warn")
   extends Logging {

   {
      val schedulerMode = sc.getConf.get("spark.scheduler.mode", "FIFO")
      if (schedulerMode != "FIFO") {
         throw new RuntimeException(s"Fractal only supports Spark's FIFO Job" +
            s" Scheduling. Found: spark.scheduler.mode=${schedulerMode}." +
            s" " +
            s"Require: spark.scheduler.mode=FIFO")
      }
   }

   setLogLevel(logLevel)

   private var _acceptingNewJobs: Boolean = true
   def acceptingNewJobs: Boolean = _acceptingNewJobs

   def sparkContext: SparkContext = sc

   def estimateThroughput[S <: Subgraph : ClassTag]
   (_fractoids: Seq[Fractoid[S]], timeLimitMs: Long, _timePerStepMs: Long): Double = {
      val random = new Random()
      val fractoids = _fractoids.toArray

      //val timePerStepMs = Math.max(_timePerStepMs, timeLimitMs / _fractoids.length)
      val timePerStepMs = _timePerStepMs

      // force graph reading
      _fractoids.head.fractalGraph.vfractoid.expand(1).aggregationCount

      var numSubgraphs = 0L
      var remainingTimeMs = timeLimitMs
      var throughputEstimateSum = 0.0
      var numEstimates = 0
      while (remainingTimeMs >= timePerStepMs) {
         val start = System.currentTimeMillis()
         val randomIdx = random.nextInt(fractoids.length)
         val fractoid = fractoids(randomIdx)
           .withStepTimeLimit(timePerStepMs)
           .withNumThreads(1)

         //val computationContainer: ComputationContainer[S] = _fractoid
         //  .computationContainer
         //  .shallowCopy(subgraphEnumeratorClassOpt = Some(classOf[AroundWordEnumerator[S]]))
         //val fractoid = _fractoid.copy(computationContainer = computationContainer)

         val stepNumSubgraphs = fractoid.aggregationCount
         numSubgraphs += stepNumSubgraphs
         val elapsedMs = System.currentTimeMillis() - start
         remainingTimeMs -= elapsedMs
         throughputEstimateSum += (stepNumSubgraphs / elapsedMs.toDouble)
         numEstimates += 1
         logApp(s"StepEstimate fractoid=${fractoid} elapsedMs=${elapsedMs} numSubgraphs=${stepNumSubgraphs}" +
           s" throughputEstimate=${stepNumSubgraphs / elapsedMs.toDouble} remainingTime=${remainingTimeMs}")
      }

      throughputEstimateSum / numEstimates.toDouble
   }

   def submitFractalStep[S <: Subgraph, T]
   (fractoid: Fractoid[S])(callback: Fractoid[S] => T): T = {
      callback(fractoid)
   }

   def submitFractalStepWithElapsedTimeMs[S <: Subgraph, T]
   (fractoid: Fractoid[S])(callback: Fractoid[S] => T): (T,Long) = {
      val start = System.currentTimeMillis()
      val result = callback(fractoid)
      val elapsed = System.currentTimeMillis() - start
      (result, elapsed)
   }

   def trySubmitFractalStep[S <: Subgraph, T]
   (fractoid: Fractoid[S])(callback: Fractoid[S] => T): Try[T] = {
      import scala.concurrent.ExecutionContext.Implicits.global
      val future = Future(callback(fractoid))
      Await.ready(future, Duration.Inf)
      future.value.get
   }

   def trySubmitFractalSteps[S <: Subgraph, T]
   (fractoids: Seq[Fractoid[S]])(callback: Fractoid[S] => T): Seq[Try[T]] = {
      import scala.concurrent.ExecutionContext.Implicits.global
      fractoids.map(fractoid => {
         val future = Future(callback(fractoid))
         Await.ready(future, Duration.Inf)
         future.value.get
      })
   }

   def trySubmitFractalStepsWithElapsedTimeMs[S <: Subgraph, T]
   (fractoids: Seq[Fractoid[S]])(callback: Fractoid[S] => T): Seq[(Try[T],Long)]
   = {
      import scala.concurrent.ExecutionContext.Implicits.global
      fractoids.map(fractoid => {
         val start = System.currentTimeMillis()
         val future = Future(callback(fractoid))
         Await.ready(future, Duration.Inf)
         val elapsed = System.currentTimeMillis() - start
         (future.value.get, elapsed)
      })
   }

   def submitFractalSteps[S <: Subgraph, T]
   (fractoids: Seq[Fractoid[S]])(callback: Fractoid[S] => T): Seq[T] = {
      import scala.concurrent.ExecutionContext.Implicits.global
      val futures = fractoids.map(fractoid => Future(callback(fractoid)))
      val future = Future.sequence(futures)
      Await.result(future, Duration.Inf)
   }

   def submitAndDiagnoseFractalStep[S <: Subgraph, T]
   (fractoid: Fractoid[S])(callback: Fractoid[S] => T)
   : (T, FractalPerformanceDiagnose) = synchronized {
      val (newFractoid, threadStatsKey) = fractoid.asDiagnosticsFractoid
      var threadStatuses: Array[FractalThreadStats] = null
      val listener = new FractalSparkListener(threadStatsKey) {
         override def onFractalStepCompleted
         (stageThreadStatuses: Array[FractalThreadStats]): Unit = {
            threadStatuses = stageThreadStatuses
         }
      }

      sparkContext.addSparkListener(listener)

      val profHandler = FractalProfiler.start(Events.CPU, 5000000)
      val output = callback(newFractoid)
      val profilingResult = profHandler.stop()

      while (threadStatuses == null) {}

      val diag = new FractalPerformanceDiagnose(newFractoid.numPrimitives)
      diag.addThreadStatuses(fractoid, threadStatuses)
      diag.addProfilingResult(profilingResult)

      sparkContext.removeSparkListener(listener)

      (output, diag)
   }

   def submitAndDiagnoseFractalSteps[S <: Subgraph, T : ClassTag]
   (fractoids: Seq[Fractoid[S]])(callback: Fractoid[S] => T)
   : (Seq[T], FractalPerformanceDiagnose) = synchronized {
      import scala.concurrent.ExecutionContext.Implicits.global

      val numFractoids = fractoids.size
      val threadStatuses = new ObjArrayList[Array[FractalThreadStats]]
      val futures = new Array[Future[T]](numFractoids)
      val listeners = new Array[FractalSparkListener](numFractoids)

      val profHandler = FractalProfiler.start(Events.CPU, 5000000)

      var i = 0
      var maxNumPrimitives = Integer.MIN_VALUE
      while (i < numFractoids) {
         threadStatuses.add(null) // placeholder
         val fractoid = fractoids(i)
         val (newFractoid, threadStatsKey) = fractoid.asDiagnosticsFractoid
         val listener = new FractalSparkListener(threadStatsKey) {
            private val fractoidIdx = i
            override def onFractalStepCompleted
            (stageThreadStatuses: Array[FractalThreadStats]): Unit = {
               threadStatuses.set(fractoidIdx, stageThreadStatuses)
            }
         }
         listeners(i) = listener
         sparkContext.addSparkListener(listener)
         futures(i) = Future(callback(newFractoid))
         maxNumPrimitives = Math.max(maxNumPrimitives, newFractoid.numPrimitives)
         i += 1
      }

      val diag = new FractalPerformanceDiagnose(maxNumPrimitives)

      val results = new Array[T](numFractoids)

      i = 0
      while (i < numFractoids) {
         results(i) = Await.result(futures(i), Duration.Inf)
         i += 1
      }

      val profilingResult = profHandler.stop()
      diag.addProfilingResult(profilingResult)

      i = 0
      while (i < numFractoids) {
         var threadStats = threadStatuses.get(i)
         while (threadStats == null) {
            Thread.sleep(100)
            threadStats = threadStatuses.get(i)
         }
         diag.addThreadStatuses(fractoids(i), threadStats)
         sparkContext.removeSparkListener(listeners(i))
         i += 1
      }

      (results.toIndexedSeq, diag)
   }

   /**
    * Read graph from text file
    * @param path
    * @param graphClass specifies how the graph is read (default adj. lists)
    * @return fractal graph
    */
   def textFile(path: String,
                graphClass: String = Configuration.CONF_MAINGRAPH_CLASS_DEFAULT)
   : FractalGraph = {
      new FractalGraph(path, graphClass, this, logLevel)
   }

   /**
    * Stop this context, cleaning the temporary directory
    */
   def stop() = {
      _acceptingNewJobs = false
      ActorMessageSystem.shutdown()
      MainGraphStore.shutdown()
      LocalComputationStore.shutdown()
   }

   /**
    * Early termination
    */
   def terminate(): Unit = {
      _acceptingNewJobs = false
      ActorMessageSystem.terminate()
   }
}