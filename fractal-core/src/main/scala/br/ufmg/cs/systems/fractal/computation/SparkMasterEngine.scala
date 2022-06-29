package br.ufmg.cs.systems.fractal.computation

import java.io.Serializable

import br.ufmg.cs.systems.fractal.Fractoid
import br.ufmg.cs.systems.fractal.aggregation.{IntIntSubgraphAggregation, LongLongSubgraphAggregation, LongObjSubgraphAggregation, LongSubgraphAggregation, ObjLongSubgraphAggregation, ObjObjSubgraphAggregation}
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

   val fractoid: Fractoid[S]

   def sc: SparkContext

   def config: SparkConfiguration

   var stepRDD: RDD[Unit] = _

   def computation: Computation[S]

   def init(): Unit = {
      if (!config.isInitialized()) {
         config.initialize(isMaster = true)
      }

      // set log level
      logInfo(s"Setting num_partitions to " +
         s"${config.getInteger("num_partitions", sc.defaultParallelism)}")

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

   def intIntRDD
   (intIngSubgraphAggregation: IntIntSubgraphAggregation[S])
   : RDD[(Int,Int)]
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
