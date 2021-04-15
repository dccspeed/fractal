package br.ufmg.cs.systems.fractal.computation

import java.io

import br.ufmg.cs.systems.fractal.Primitive
import br.ufmg.cs.systems.fractal.aggregation.{LongLongSubgraphAggregation, LongObjSubgraphAggregation, LongSubgraphAggregation, ObjLongSubgraphAggregation, ObjObjSubgraphAggregation}
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.{Logging, ReflectionSerializationUtils}
import com.koloboke.collect.map.LongLongMap
import org.apache.spark.TaskContext

import scala.reflect.ClassTag

trait SparkEngine[S <: Subgraph]
   extends ExecutionEngine[S] with Serializable with Logging {

   val stageId: Int = TaskContext.get().stageId()
   val partitionId: Int
   val step: Int

   def configuration: SparkConfiguration

   setLogLevel(configuration.getLogLevel)

   def computation: Computation[S]

   val computationCopy: Computation[S] = {
      val comp = ReflectionSerializationUtils.clone(computation)
      if (configuration.getSubgraphClass() == null) {
         configuration.setSubgraphClass(comp.getSubgraphClass())
      }
      comp
   }

   private val _primitives: Array[Primitive] = computation.primitives()

   override def primitives: Array[Primitive] = _primitives

   def numPartitions: Int = configuration.numPartitions

   def init(): Unit = {
      if (configuration.getSubgraphClass() == null) {
         configuration.setSubgraphClass(computation.getSubgraphClass())
      }
      computation.init(this, configuration)
      computationCopy.init(this, configuration)
   }

   override def getConfig(): Configuration = configuration

   override def getStageId: Int = stageId

   def finalizeEngine(): Unit

   override def getPartitionId() = partitionId

   override def getStep() = step

   def computeAggregationLong
   (longSubgraphAggregation: LongSubgraphAggregation[S])
   : Long

   def computeAggregationObjLong[K <: io.Serializable]
   (objLongSubgraphAggregation: ObjLongSubgraphAggregation[S, K])
   : Iterator[(K, Long)]

   def computeAggregationObjObj[K <: io.Serializable, V <: io.Serializable]
   (objObjSubgraphAggregation: ObjObjSubgraphAggregation[S, K, V])
   : Iterator[(K, V)]

   /**
    * This call starts this engine computation and aggregates the valid
    * subgraphs by key/value, where value is a long.
    *
    * @return iterator of (Long,Long) to be consumed downstream
    */
   def computeAggregationLongLong
   (longLongSubgraphAggregation: LongLongSubgraphAggregation[S])
   : Iterator[(Long, Long)]

   /**
    * This call starts this engine computation and aggregates the valid
    * subgraphs by key/value, where value is a long.
    *
    * @param longObjSubgraphAggregation
    * @return iterator of (Long,Long) to be consumed downstream
    */
   def computeAggregationLongObj
   [V <: io.Serializable]
   (longObjSubgraphAggregation: LongObjSubgraphAggregation[S, V])
   : Iterator[(Long, V)]
}

