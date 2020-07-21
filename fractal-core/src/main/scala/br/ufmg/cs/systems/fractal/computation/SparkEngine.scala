package br.ufmg.cs.systems.fractal.computation

import java.io

import br.ufmg.cs.systems.fractal.Primitive
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.hadoop.io.LongWritable
import org.apache.spark.TaskContext
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.Map

trait SparkEngine[S <: Subgraph]
   extends CommonExecutionEngine[S] with Serializable with Logging {

   val stageId: Int = TaskContext.get().stageId()

   val partitionId: Int
   val step: Int
   val accums: Map[String, LongAccumulator]
   val validSubgraphsAccum: LongAccumulator

   def configuration: SparkConfiguration

   setLogLevel(configuration.getLogLevel)

   def computation: Computation[S]

   def computationCopy: Computation[S]

   private val _primitives: Array[Primitive] = computation.primitives()

   override def primitives: Array[Primitive] = _primitives

   /**
    * We assume the number of requested executor cores as the default number of
    * partitions
    */
   def numPartitions: Int = configuration.numPartitions

   var numSubgraphsOutput: Long = _

   def init(): Unit = {
      if (configuration.getSubgraphClass() == null) {
         configuration.setSubgraphClass(computation.getSubgraphClass())
      }
      computation.init(this, configuration)
   }

   override def getConfig(): Configuration = configuration

   override def getStageId: Int = stageId

   override def finalize() = {
   }

   /**
    * Any Spark accumulator used for stats accounting is flushed here
    */
   def flushStatsAccumulators: Unit = {
      accums(SparkMasterEngine.AGG_SUBGRAPHS_OUTPUT).add(numSubgraphsOutput)
      accums.foreach { case (name, accum) =>
         logDebug(s"Accumulator[${step}][${partitionId}][${name}]:" +
            s" ${accum.value}")
      }
   }

   def getStatsAccumulators: String = {
      accums.map { case (name, accum) =>
         s"${name}:${accum.value}"
      }.mkString(",")
   }

   override def aggregate(name: String, value: LongWritable) = {
      aggregate(name, value.get)
   }

   override def aggregate(name: String, value: Long) = {
      if (accums.contains(name)) {
         accums(name).add(value)
      }
   }

   override def addValidSubgraphs(n: Long) = {
      validSubgraphsAccum.add(n)
   }

   // other functions
   override def getPartitionId() = partitionId

   override def getStep() = step

   def computeAggregationLong
   (_defaultValue: Long, _value: S => Long, _reduce: (Long, Long) => Long)
   : Long

   def computeAggregationObjLong[K <: io.Serializable]
   (_key: S => K, _defaultValue: Long, _value: S => Long,
    _reduce: (Long, Long) => Long)
   : Iterator[(K, Long)]

   def computeAggregationObjObj[K <: io.Serializable, V <: io.Serializable]
   (_key: S => K, _value: S => V, _aggregate: (V, V) => Unit)
   : Iterator[(K, V)]
}

