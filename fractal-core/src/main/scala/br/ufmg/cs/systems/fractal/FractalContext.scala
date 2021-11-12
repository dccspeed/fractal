package br.ufmg.cs.systems.fractal

import br.ufmg.cs.systems.fractal.computation.{ActorMessageSystem, LocalComputationStore}
import br.ufmg.cs.systems.fractal.conf.Configuration
import br.ufmg.cs.systems.fractal.graph.MainGraphStore
import br.ufmg.cs.systems.fractal.subgraph.Subgraph
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.SparkContext

import scala.concurrent.Future

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

   def sparkContext: SparkContext = sc

   def submitFractalSteps[S <: Subgraph, T]
   (fractoids: Seq[Fractoid[S]],
    callback: Fractoid[S] => T): Future[Seq[T]] = {
      import scala.concurrent.ExecutionContext.Implicits.global
      val futures = fractoids.map(fractoid => Future(callback(fractoid)))
      Future.sequence(futures)
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
      ActorMessageSystem.shutdown()
      MainGraphStore.shutdown()
      LocalComputationStore.shutdown()
   }
}
