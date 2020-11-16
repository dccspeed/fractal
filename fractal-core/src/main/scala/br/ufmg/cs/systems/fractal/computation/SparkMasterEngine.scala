package br.ufmg.cs.systems.fractal.computation

import java.io.Serializable

import br.ufmg.cs.systems.fractal.Fractoid
import br.ufmg.cs.systems.fractal.aggregation.{LongLongSubgraphAggregation, LongObjSubgraphAggregation, LongSubgraphAggregation, ObjLongSubgraphAggregation, ObjObjSubgraphAggregation}
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.Logging
import com.koloboke.collect.map.LongLongMap
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait SparkMasterEngine[S <: Subgraph] extends Logging {

   val step: Int
   var sc: SparkContext = _

   def config: SparkConfiguration

   var stepRDD: RDD[Unit] = _

   def init(computation: Computation[S]): Unit = {
      if (!config.isInitialized()) {
         config.initialize(isMaster = true)
      }

      // set log level
      logInfo(s"Setting log level to ${config.getLogLevel}")
      setLogLevel(config.getLogLevel)
      sc.setLogLevel(config.getLogLevel.toUpperCase)
      logInfo(s"Setting num_partitions to " +
         s"${config.getInteger("num_partitions", sc.defaultParallelism)}")

      // garantees that outputPath does not exist
      if (config.isOutputActive) {
         val fs = FileSystem.get(sc.hadoopConfiguration)
         val outputPath = new Path(s"${config.getOutputPath}/*")
         if (fs.exists(outputPath))
            throw new RuntimeException(
               s"Output path ${config.getOutputPath} exists. Choose another " +
                  s"one."
            )
      }

      // master must know aggregators metadata
      var currComp = computation
      while (currComp != null) {
         currComp.initAggregations(config)
         currComp = currComp.nextComputation()
      }

      // set initial state
      stepRDD = sc.makeRDD(Seq.empty[Unit], numPartitions)
   }

   def longRDD
   (longSubgraphAggregation: LongSubgraphAggregation[S])
   : RDD[Long]

   def objLongRDD[K <: Serializable : ClassTag]
   (objLongSubgraphAggregation: ObjLongSubgraphAggregation[S, K])
   : RDD[(K, Long)]

   def objObjRDD[K <: Serializable, V <: Serializable]
   (objObjSubgraphAggregation: ObjObjSubgraphAggregation[S, K, V])
   : RDD[(K, V)]

   def execEnginesRDD: RDD[SparkEngine[S]]

   def numPartitions: Int = config.numPartitions

   def getStep(): Int = step

   override def toString: String = {
      s"${this.getClass.getName}(${step})"
   }

   def longLongRDD
   (longLongSubgraphAggregation: LongLongSubgraphAggregation[S])
   : RDD[(Long, Long)]

   def longObjRDD[V <: Serializable : ClassTag]
   (longObjSubgraphAggregation: LongObjSubgraphAggregation[S, V])
   : RDD[(Long, V)]
}

object SparkMasterEngine {

   import Configuration._
   import SparkConfiguration._

   def apply[S <: Subgraph]
   (frac: Fractoid[S]): SparkMasterEngine[S] =
      frac.config.getString(CONF_COMM_STRATEGY, CONF_COMM_STRATEGY_DEFAULT)
      match {
         case COMM_FROM_SCRATCH =>
            new SparkFromScratchMasterEngineAggregation[S](frac)
      }
}
