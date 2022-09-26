package br.ufmg.cs.systems.fractal.computation

import br.ufmg.cs.systems.fractal.Fractoid
import br.ufmg.cs.systems.fractal.aggregation._
import br.ufmg.cs.systems.fractal.conf.SparkConfiguration
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.Serializable
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
      logDebug(s"Setting num_partitions to " +
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
   def apply[S <: Subgraph](frac: Fractoid[S]): SparkMasterEngine[S] =
      new SparkFromScratchMasterEngineAggregation[S](frac)
}
