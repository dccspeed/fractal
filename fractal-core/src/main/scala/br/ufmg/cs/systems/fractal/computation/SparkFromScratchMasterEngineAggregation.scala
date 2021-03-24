package br.ufmg.cs.systems.fractal.computation

import java.io.Serializable
import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor._
import br.ufmg.cs.systems.fractal.aggregation._
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.{Logging, ProcessComputeFunc, ThreadStats}
import br.ufmg.cs.systems.fractal.{Fractoid, Primitive}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CollectionAccumulator
import spire.ClassTag

/**
 * Underlying engine that runs the fractal master.
 * It interacts directly with the RDD interface in Spark by handling the
 * SparkContext.
 */
class SparkFromScratchMasterEngineAggregation[S <: Subgraph]
(
   val step: Int,
   configBc: Broadcast[SparkConfiguration],
   originalContainer: ComputationContainer[S]) extends SparkMasterEngine[S] {

   def config: SparkConfiguration = configBc.value

   private var masterActorRef: ActorRef = _

   def this(frac: Fractoid[S]) {
      this(frac.step, frac.configBc, frac.computationContainer)
      sc = frac.sparkContext
   }

   override def init(originalContainer: Computation[S]): Unit = {
      val start = System.currentTimeMillis

      super.init(originalContainer)

      // coordinator actor
      masterActorRef = ActorMessageSystem.createActor(this)

      logInfo(s"Started master-actor(step=${step}):" +
         s" ${masterActorRef}")

      val end = System.currentTimeMillis

      logInfo(s"${this} took ${(end - start)}ms to initialize.")
   }

   override def longRDD
   (longSubgraphAggregation: LongSubgraphAggregation[S])
   : RDD[Long] = {
      execEnginesRDD.map(_.computeAggregationLong(longSubgraphAggregation))
   }

   override def objLongRDD[K <: Serializable : ClassTag]
   (objLongSubgraphAggregation: ObjLongSubgraphAggregation[S, K])
   : RDD[(K, Long)] = {
      execEnginesRDD.flatMap(
         _.computeAggregationObjLong[K](objLongSubgraphAggregation))
   }

   override def objObjRDD[K <: Serializable, V <: Serializable]
   (objObjSubgraphAggregation: ObjObjSubgraphAggregation[S, K, V])
   : RDD[(K, V)] = {
      execEnginesRDD.flatMap(
         _.computeAggregationObjObj[K, V](objObjSubgraphAggregation))
   }

   override def longLongRDD
   (longLongSubgraphAggregation: LongLongSubgraphAggregation[S])
   : RDD[(Long, Long)] = {
      execEnginesRDD.flatMap(
         _.computeAggregationLongLong(longLongSubgraphAggregation))
   }

   override def longObjRDD[V <: Serializable : ClassTag]
   (longObjSubgraphAggregation: LongObjSubgraphAggregation[S, V])
   : RDD[(Long, V)] = {
      execEnginesRDD.flatMap(
         _.computeAggregationLongObj(longObjSubgraphAggregation))
   }

   override def execEnginesRDD: RDD[SparkEngine[S]] = {
      init(originalContainer)

      logInfo(s"${this} Computation starting from ${stepRDD}," +
         s", StorageLevel=${stepRDD.getStorageLevel}")

      // save original container, i.e., without parents' computations
      logInfo(s"From scratch computation (${this})." +
         s" Original computation: ${originalContainer}")

      // we will contruct the pipeline in this var
      var cc = originalContainer

      cc = {
         def withCustomFuncs(cc: ComputationContainer[S], depth: Int)
         : ComputationContainer[S] = cc.nextComputationOpt match {
            case Some(c) =>
               val ncc = withCustomFuncs(
                  c.asInstanceOf[ComputationContainer[S]],
                  depth + 1)
               cc.primitive match {
                  case Primitive.E =>
                     if (depth == 0) {
                        val extensionFuncFirst = new ExtensionPrimitiveFirst[S]
                        cc.shallowCopy(
                           processComputeOpt = Option(extensionFuncFirst),
                           nextComputationOpt = Option(ncc),
                           processOpt = None)
                     } else {
                        val extensionFuncMiddle = new
                              ExtensionPrimitiveMiddle[S]
                        cc.shallowCopy(
                           processComputeOpt = Option(extensionFuncMiddle),
                           nextComputationOpt = Option(ncc),
                           processOpt = None)
                     }
                  case Primitive.F =>
                     val filterFunc = new FilterPrimitive[S]
                     cc.shallowCopy(
                        processComputeOpt = Option(filterFunc),
                        nextComputationOpt = Option(ncc),
                        processOpt = None)
                  case other =>
                     throw new RuntimeException(s"Unexpected " +
                        s"primitive: ${other}")
               }

            case None =>
               cc.primitive match {
                  case Primitive.E =>
                     if (depth == 0) {
                        val extensionFuncFirstLast = new
                              ExtensionPrimitiveFirstLast[S]
                        cc.shallowCopy(
                           processComputeOpt = Option(extensionFuncFirstLast))
                     } else {
                        val extensionFuncLast = new ExtensionPrimitiveLast[S]
                        cc.shallowCopy(
                           processComputeOpt = Option(extensionFuncLast))
                     }
                  case Primitive.F =>
                     val filterFuncLast = new FilterPrimitiveLast[S]
                     cc.shallowCopy(
                        processComputeOpt = Option(filterFuncLast))
                  case other =>
                     throw new RuntimeException(s"Unexpected " +
                        s"primitive: ${other}")
               }
         }

         cc = withCustomFuncs(cc, 0).withComputationLabel("first_computation")
         cc.setDepth(0)
         cc
      }

      logInfo(s"From scratch computation (${this}). Final computation: ${cc}")

      logInfo(s"SparkConfiguration estimated size = " +
         s"${SparkConfiguration.serialize(config).length} bytes." +
         s" ${config}")

      /**
       * Local vars for clean serialization
       */
      val _step = step
      val _configBc = configBc
      val _computation = cc

      val taskData = _computation

      logInfo(s"TaskSize ${taskData} " +
         s"${SparkConfiguration.serialize(taskData).length}")

      // create accumulator for thread stats if this is allowed
      val threadStatsAccum: CollectionAccumulator[ThreadStats] =
         if (config.collectThreadStats()) {
            sc.collectionAccumulator[ThreadStats]("THREAD_STATS")
         } else {
            null
         }

      stepRDD.mapPartitionsWithIndex { (idx, _) =>
         _configBc.value.initializeWithTag(isMaster = false)
         val execEngine = new SparkFromScratchEngine[S](
            partitionId = idx,
            step = _step,
            computation = _computation,
            configuration = _configBc.value,
            threadStatsAccum // null means 'do not collect thread stats'
         )

         Iterator[SparkEngine[S]](execEngine)
      }
   }
}

class FilterPrimitiveLast[S <: Subgraph] extends ProcessComputeFunc[S] {
   override def toString = "FL"

   override def apply(enum: SubgraphEnumerator[S], c: Computation[S]): Long = {
      val subgraph = enum.getSubgraph
      if (c.filter_FILTERING_PRIMITIVE(subgraph)) {
         AggregationPrimitive(c, subgraph)
         c.addValidSubgraphs(1)
      }

      c.addCanonicalSubgraphs(1)

      0L
   }

}


class FilterPrimitive[S <: Subgraph] extends ProcessComputeFunc[S] {
   override def toString = "F"

   override def apply(enum: SubgraphEnumerator[S],
                      c: Computation[S]): Long = {
      val subgraph = enum.getSubgraph
      if (c.filter_FILTERING_PRIMITIVE(subgraph)) {
         c.addValidSubgraphs(1)
         c.nextComputation().compute()
      }

      c.addCanonicalSubgraphs(1)

      0L
   }

}

class ExtensionPrimitiveMiddle[S <: Subgraph] extends ProcessComputeFunc[S] {

   override def toString = "EM"

   def apply(iter: SubgraphEnumerator[S], c: Computation[S]): Long = {
      val nextComp = c.nextComputation()
      var numSubgraphs = 0L

      while (iter.extend_EXTENSION_PRIMITIVE()) {
         numSubgraphs += 1
         nextComp.compute()
      }

      c.addCanonicalSubgraphs(numSubgraphs)
      c.addValidSubgraphs(numSubgraphs)

      0L
   }
}

class ExtensionPrimitiveFirst[S <: Subgraph] extends ProcessComputeFunc[S] {

   override def toString = "EF"

   def apply(iter: SubgraphEnumerator[S], c: Computation[S]): Long = {
      val nextComp = c.nextComputation()
      var numSubgraphs = 0L

      while (iter.extend_EXTENSION_PRIMITIVE()) {
         numSubgraphs += 1
         nextComp.compute()
      }

      c.addCanonicalSubgraphs(numSubgraphs)
      c.addValidSubgraphs(numSubgraphs)

      0L
   }
}

class ExtensionPrimitiveFirstLast[S <: Subgraph] extends
   ProcessComputeFunc[S] {

   override def toString = "EFL"

   def apply(iter: SubgraphEnumerator[S], c: Computation[S]): Long = {
      val subgraph = iter.getSubgraph

      while (iter.extend_EXTENSION_PRIMITIVE()) AggregationPrimitive(c, subgraph)

      val numExtensions = iter.getExtensions.size()
      c.addValidSubgraphs(numExtensions)
      c.addCanonicalSubgraphs(numExtensions)

      0L
   }
}

class ExtensionPrimitiveLast[S <: Subgraph] extends ProcessComputeFunc[S] {

   override def toString = "EL"

   def apply(iter: SubgraphEnumerator[S], c: Computation[S]): Long = {
      val subgraph = iter.getSubgraph

      while (iter.extend_EXTENSION_PRIMITIVE()) AggregationPrimitive(c, subgraph)

      val numExtensions = iter.getExtensions.size()
      c.addValidSubgraphs(numExtensions)
      c.addCanonicalSubgraphs(numExtensions)

      0L
   }
}

object AggregationPrimitive {
   def apply[S <: Subgraph](c: Computation[S], s: S) = c.process(s)
}
