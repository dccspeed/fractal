package br.ufmg.cs.systems.fractal.computation

import java.io.Serializable

import br.ufmg.cs.systems.fractal.Fractoid
import br.ufmg.cs.systems.fractal.aggregation._
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

trait SparkMasterEngine [S <: Subgraph]
   extends CommonMasterExecutionEngine with Logging {

   import SparkMasterEngine._

   val step: Int

   var sc: SparkContext = _

   def config: SparkConfiguration

   var masterComputation: MasterComputation = _

   var stepRDD: RDD[Unit] = _

   var aggAccums: Map[String,LongAccumulator] = _

   var validSubgraphsAccum: LongAccumulator = _

   /* */

   def init(computation: Computation[S]): Unit = {
      if (!config.isInitialized()) {
         config.initialize(isMaster = true)
      }

      // set log level
      logInfo (s"Setting log level to ${config.getLogLevel}")
      setLogLevel (config.getLogLevel)
      sc.setLogLevel (config.getLogLevel.toUpperCase)
      logInfo (s"Setting num_partitions to " +
         s"${config.getInteger("num_partitions", sc.defaultParallelism)}")
      //config.setIfUnset ("num_partitions", sc.defaultParallelism)
      //config.setHadoopConfig (sc.hadoopConfiguration)

      // garantees that outputPath does not exist
      if (config.isOutputActive) {
         val fs = FileSystem.get(sc.hadoopConfiguration)
         val outputPath = new Path(s"${config.getOutputPath}/*")
         if (fs.exists (outputPath))
            throw new RuntimeException (
               s"Output path ${config.getOutputPath} exists. Choose another one."
            )
      }

      // master computation
      masterComputation = config.createMasterComputation()
      masterComputation.setUnderlyingExecutionEngine(this)
      masterComputation.init()

      // master must know aggregators metadata
      //val computation = config.createComputation [S]
      var currComp = computation
      while (currComp != null) {
         currComp.initAggregations(config)
         currComp = currComp.nextComputation()
      }

      // default accumulators
      aggAccums = Map.empty
      aggAccums.update (AGG_SUBGRAPHS_OUTPUT,
         sc.longAccumulator (AGG_SUBGRAPHS_OUTPUT))

      // set initial state
      stepRDD = sc.makeRDD(Seq.empty[Unit], numPartitions)
   }

   /**
    * Report the accumulators configured in this step
    */
   def reportAccumulators: Unit = {
      aggAccums.toArray.sortBy(_._1).foreach { case (name, accum) =>
         logInfo (s"FractalStep[${step}][${name}][${accum}]: ${accum.value}")
      }
   }

   def longRDD
   (defaultValue: Long, value: S => Long, reduce: (Long,Long) => Long)
   : RDD[Long]

   def objLongRDD[K <: Serializable : ClassTag]
   (key: S => K, defaultValue: Long, value: S => Long,
    reduce: (Long,Long) => Long)
   : RDD[(K,Long)]

   def objObjRDD[K <: Serializable, V <: Serializable]
   (key: S => K, value: S => V, aggregate: (V,V) => Unit)
   : RDD[(K,V)]

   def execEnginesRDD: RDD[SparkEngine[S]]

   def numPartitions: Int = config.numPartitions

   override def getStep(): Int = step

   override def toString: String = {
      s"${this.getClass.getName}(${step})"
   }
}

object SparkMasterEngine {
   import Configuration._
   import SparkConfiguration._

   // macros for spark accumulators
   val AGG_SUBGRAPHS_OUTPUT = "subgraphs_output"

   def apply[S <: Subgraph]
   (frac: Fractoid[S]): SparkMasterEngine[S] =
      frac.config.getString(CONF_COMM_STRATEGY, CONF_COMM_STRATEGY_DEFAULT)
      match {
         case COMM_FROM_SCRATCH =>
            new SparkFromScratchMasterEngineAggregation[S](frac)
      }
}
