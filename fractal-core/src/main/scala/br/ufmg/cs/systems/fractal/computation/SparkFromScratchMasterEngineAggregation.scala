package br.ufmg.cs.systems.fractal.computation

import java.io.Serializable
import java.lang.management.ManagementFactory
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import akka.actor._
import br.ufmg.cs.systems.fractal.aggregation._
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.{FractalSparkListener, FractalThreadStats, Logging, ProcessComputeFunc}
import br.ufmg.cs.systems.fractal.{Fractoid, Primitive}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CollectionAccumulator

import scala.reflect.ClassTag

/**
 * Underlying engine that runs the fractal master.
 * It interacts directly with the RDD interface in Spark by handling the
 * SparkContext.
 */
class SparkFromScratchMasterEngineAggregation[S <: Subgraph]
(_fractoid: Fractoid[S]) extends SparkMasterEngine[S] {

   import SparkFromScratchMasterEngineAggregation._

   val fractoid: Fractoid[S] = _fractoid.withNextStepId

   val step: Int = fractoid.step

   val originalContainer: ComputationContainer[S] = fractoid.computationContainer

   val computation: Computation[S] =
      originalContainer.asInstanceOf[Computation[S]]

   val configBc: Broadcast[SparkConfiguration] = fractoid.configBc

   val config: SparkConfiguration = configBc.value

   override val sc: SparkContext = fractoid.sparkContext

   val parent: SparkFromScratchMasterEngineAggregation[_ <: Subgraph] = {
      if (fractoid.parent != null) {
         new SparkFromScratchMasterEngineAggregation(fractoid.parent)
      } else {
         null
      }
   }

   // get current fractoid, going backwards building engine data for each
   // partial workflow
   private lazy val engineDataArray: Array[EngineData] = {
      var engineDataList = List.empty[EngineData]

      var currEngine: SparkFromScratchMasterEngineAggregation[_ <: Subgraph] =
         this
      while (currEngine != null) {
         val currFrac = currEngine.fractoid
         val step = currFrac.step
         val configBc = currFrac.configBc
         val computationContainer =
            getFixedContainer(currFrac.computationContainer)
         val data: EngineData = (step, configBc, computationContainer)
         engineDataList = data :: engineDataList
         currEngine = currEngine.parent
      }

      engineDataList.toArray
   }

   private var masterActorRef: ActorRef = _

   val fixedContainer: ComputationContainer[S] =
      SparkFromScratchMasterEngineAggregation.getFixedContainer(originalContainer)

   override def init(): Unit = {
      val start = System.currentTimeMillis
      if (parent != null) parent.init()

      super.init()

      // coordinator actor
      masterActorRef = ActorMessageSystem.createActor(this)

      val end = System.currentTimeMillis

      logDebug(s"Started master-actor(step=${step}):" +
         s" ${masterActorRef}. Took ${(end - start)}ms to initialize")
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

   override def intIntRDD
   (intIntSubgraphAggregation: IntIntSubgraphAggregation[S])
   : RDD[(Int, Int)] = {
      execEnginesRDD.flatMap(
         _.computeAggregationIntInt(intIntSubgraphAggregation))
   }

   override def longObjRDD[V <: Serializable : ClassTag]
   (longObjSubgraphAggregation: LongObjSubgraphAggregation[S, V])
   : RDD[(Long, V)] = {
      execEnginesRDD.flatMap(
         _.computeAggregationLongObj(longObjSubgraphAggregation))
   }

   override def execEnginesRDD: RDD[SparkEngine[S]] = {
      try {
         init()
      } catch {
         case e: InterruptedException =>
            logWarn(s"Cannot create engine: ${e}")
            return sc.emptyRDD
      }

      // save original container, i.e., without parents' computations
      logDebug(s"From scratch computation (${this})." +
         s" Original computation: ${originalContainer}" +
         s" Fixed computation: ${fixedContainer}")

      // local val used for clean serialization
      val _engineDataArray = engineDataArray

      // create accumulator for thread stats if this is allowed
      val threadStatsAccum: CollectionAccumulator[FractalThreadStats] =
         if (config.collectThreadStats()) {
            val accumKey = config.getThreadStatsKey
            val accum = sc.collectionAccumulator[FractalThreadStats](accumKey)
            FractalSparkListener.threadStatsKeyToAccumId.put(accumKey, accum.id)
            accum
         } else {
            null
         }

      stepRDD.mapPartitionsWithIndex ((idx, _) => {
         val enginesArray = getEnginesArray(idx, threadStatsAccum,
            _engineDataArray)
         val execEngine = enginesArray.last
            .asInstanceOf[SparkFromScratchEngine[S]]

         val config = execEngine.configuration
         if (config.reachedTimeLimit()) {
            Iterator.empty
         } else {
            Iterator[SparkEngine[S]](execEngine)
         }
      })
   }
}

object SparkFromScratchMasterEngineAggregation {

   // engine data is: fractal step + configuration as broadcast var +
   // computation container representing this partial workflow
   private type EngineData =
      (Int,Broadcast[SparkConfiguration],ComputationContainer[_ <: Subgraph])

   /**
    * Get array of execution engines from engines data
    * @param idx partition id (thread id)
    * @param threadStatsAccum optional thread stats accumulator
    * @param engineDataArray engines data
    * @return array of engines, each representing a partial workflow
    */
   private def getEnginesArray(idx: Int,
                               threadStatsAccum: CollectionAccumulator[FractalThreadStats],
                               engineDataArray: Array[EngineData])
   : Array[SparkFromScratchEngine[_ <: Subgraph]] = {

      val numEngines = engineDataArray.length
      val enginesArray = new Array[SparkFromScratchEngine[_ <: Subgraph]](numEngines)

      // auxiliar function used to create engines with proper typing
      def createEngine[R <: Subgraph](step: Int,
                                      configBc: Broadcast[SparkConfiguration],
                                      computation: ComputationContainer[R])
      : SparkFromScratchEngine[R] = {
         configBc.value.initializeWithTag(isMaster = false)
         val execEngine = new SparkFromScratchEngine[R](
            partitionId = idx,
            step = step,
            computation = computation,
            configuration = configBc.value,
            threadStatsAccum // null means 'do not collect thread stats'
         )

         execEngine
      }

      // create engines from data
      var i = 0
      while (i < numEngines) {
         val (step, configBc, computation) = engineDataArray(i)
         enginesArray(i) = createEngine(step, configBc, computation)
         i += 1
      }

      // chain engines -- preivous and next
      i = 0
      while (i < numEngines) {
         val previous: SparkFromScratchEngine[_ <: Subgraph] =
            if (i == 0) null else enginesArray(i - 1)

         val next: SparkFromScratchEngine[_ <: Subgraph] =
            if (i == numEngines - 1) null else enginesArray(i + 1)

         val engine = enginesArray(i)
         engine.setPreviousEngine(previous)
         engine.setNextEngine(next)

         i += 1
      }

      enginesArray
   }

   /**
    * Fix container with proper process functions -- this method analyses the
    * workflow of computations and determines which process function should
    * be used based on each primitive
    * @param originalContainer simplified container from fractoid
    * @tparam S subgraph type
    * @return new container ready for shipping and execution
    */
   private def getFixedContainer[S <: Subgraph]
   (originalContainer: ComputationContainer[S]): ComputationContainer[S] = {
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

         cc = withCustomFuncs(cc, 0)
         cc.setDepth(0)
         cc
      }

      cc
   }
}

/**
 * Filtering primitive if last computation
 * @tparam S subgraph type
 */
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

/**
 * Filtering primitive if not last computation
 * @tparam S subgraph type
 */
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

/**
 * Extension primitive if not first AND not last computation
 * @tparam S subgraph type
 */
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

/**
 * Extension primitive if first computation
 * @tparam S subgraph type
 */
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

/**
 * Extension primitive if first AND last computation
 * @tparam S subgraph type
 */
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

/**
 * Extension primitive if not first but last computation
 * @tparam S subgraph type
 */
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

/**
 * Aggregation primitive -- always applied in the last computation of the
 * fractal step
 */
object AggregationPrimitive {
   def apply[S <: Subgraph](c: Computation[S], s: S): Unit = {
      c.process(s)
   }
}