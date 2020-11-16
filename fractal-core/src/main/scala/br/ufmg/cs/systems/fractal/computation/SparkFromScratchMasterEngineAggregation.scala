package br.ufmg.cs.systems.fractal.computation

import java.io.Serializable
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.function.IntConsumer

import akka.actor._
import br.ufmg.cs.systems.fractal.aggregation.{LongLongSubgraphAggregation, LongObjSubgraphAggregation, LongSubgraphAggregation, ObjLongSubgraphAggregation, ObjObjSubgraphAggregation}
import br.ufmg.cs.systems.fractal.{Fractoid, Primitive}
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.{EventTimer, Logging, ProcessComputeFunc, ReflectionUtils}
import com.koloboke.collect.map.LongLongMap
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{LongAccumulator, SizeEstimator}
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
   originalContainer: ComputationContainer[S]) extends SparkMasterEngine [S] {

   def config: SparkConfiguration = configBc.value

   var masterActorRef: ActorRef = _

   def this(frac: Fractoid[S]) {
      this (frac.step, frac.configBc, frac.computationContainer)
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
   (objLongSubgraphAggregation: ObjLongSubgraphAggregation[S,K])
   : RDD[(K,Long)] = {
      execEnginesRDD.flatMap(
         _.computeAggregationObjLong[K](objLongSubgraphAggregation))
   }

   override def objObjRDD[K <: Serializable, V <: Serializable]
   (objObjSubgraphAggregation: ObjObjSubgraphAggregation[S,K,V])
   : RDD[(K,V)] = {
      execEnginesRDD.flatMap(
         _.computeAggregationObjObj[K,V](objObjSubgraphAggregation))
   }

   override def longLongRDD
   (longLongSubgraphAggregation: LongLongSubgraphAggregation[S])
   : RDD[(Long,Long)] = {
      execEnginesRDD.flatMap(
         _.computeAggregationLongLong(longLongSubgraphAggregation))
   }

   override def longObjRDD[V <: Serializable : ClassTag]
   (longObjSubgraphAggregation: LongObjSubgraphAggregation[S,V])
   : RDD[(Long,V)] = {
      execEnginesRDD.flatMap(
         _.computeAggregationLongObj(longObjSubgraphAggregation))
   }

   override def execEnginesRDD: RDD[SparkEngine[S]] = {
      init(originalContainer)

      logInfo (s"${this} Computation starting from ${stepRDD}," +
         s", StorageLevel=${stepRDD.getStorageLevel}")

      // save original container, i.e., without parents' computations
      logInfo (s"From scratch computation (${this})." +
         s" Original computation: ${originalContainer}")

      // we will contruct the pipeline in this var
      var cc = originalContainer.withComputationLabel("last_step_begins")

      cc = {
         def withCustomFuncs(cc: ComputationContainer[S], depth: Int)
         : ComputationContainer[S] = cc.nextComputationOpt match {
            case Some(c) =>
               val ncc = withCustomFuncs(c.asInstanceOf[ComputationContainer[S]],
                  depth + 1)
               cc.primitive match {
                  case Primitive.E =>
                     if (depth == 0) {
                        val extensionFuncFirst = getProcessComputeFuncFirst()
                        cc.shallowCopy(
                           processComputeOpt = Option(extensionFuncFirst),
                           nextComputationOpt = Option(ncc),
                           processOpt = None)
                     } else {
                        val extensionFuncMiddle = getProcessComputeFuncMiddle()
                        cc.shallowCopy(
                           processComputeOpt = Option(extensionFuncMiddle),
                           nextComputationOpt = Option(ncc),
                           processOpt = None)
                     }
                  case Primitive.F =>
                     val filterFunc = getProcessComputeFuncFilter()
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
                        val extensionFuncFirstLast = getProcessComputeFuncFirstLast()
                        cc.shallowCopy(
                           processComputeOpt = Option(extensionFuncFirstLast))
                     } else {
                        val extensionFuncLast = getProcessComputeFuncLast()
                        cc.shallowCopy(
                           processComputeOpt = Option(extensionFuncLast))
                     }
                  case Primitive.F =>
                     val filterFuncLast = getProcessComputeFuncFilterLast()
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

      logInfo (s"From scratch computation (${this}). Final computation: ${cc}")

      logInfo (s"SparkConfiguration estimated size = " +
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

      stepRDD.mapPartitionsWithIndex { (idx, _) =>
         if (EventTimer.ENABLED) {
            EventTimer.workerInstance(idx).start(EventTimer.INITIALIZATION)
         }

         _configBc.value.initializeWithTag(isMaster = false)
         val execEngine = new SparkFromScratchEngine [S] (
            partitionId = idx,
            step = _step,
            computation = _computation,
            configuration = _configBc.value
         )

         Iterator[SparkEngine[S]](execEngine)
      }
   }

   def getProcessComputeFuncFilterLast(): ProcessComputeFunc[S] = {
      new ProcessComputeFunc[S] with Logging {
         override def apply(enum: SubgraphEnumerator[S],
                            c: Computation[S]): Long = {
            val subgraph = enum.getSubgraph
            var ret = 0L
            if (c.filter(subgraph)) {
               c.process(subgraph)

               if (Configuration.OPCOUNTER_ENABLED) {
                  c.addValidSubgraphs(1)
               }

               ret = 1L
            } else {
               ret = 0L
            }

            if (Configuration.OPCOUNTER_ENABLED) {
               c.addCanonicalSubgraphs(1)
            }

            ret
         }

         override def toString = "FL"
      }
   }

   def getProcessComputeFuncFilter(): ProcessComputeFunc[S] = {

      new ProcessComputeFunc[S] with Logging {

         override def apply(enum: SubgraphEnumerator[S],
                            c: Computation[S]): Long = {
            var ret = 0L
            val subgraph = enum.getSubgraph
            if (c.filter(subgraph)) {

               if (Configuration.OPCOUNTER_ENABLED) {
                  c.addValidSubgraphs(1)
               }

               c.nextComputation().compute()
            }

            if (Configuration.OPCOUNTER_ENABLED) {
               c.addCanonicalSubgraphs(1)
            }

            ret
         }

         override def toString = "F"
      }
   }

   def getProcessComputeFuncMiddle(): ProcessComputeFunc[S] = {
      new ProcessComputeFunc[S] with Logging {
         override def toString = "EM"

         def apply(iter: SubgraphEnumerator[S], c: Computation[S]): Long = {
            var ret = 0L
            val nextComp = c.nextComputation()

            while (iter.extend()) {
               if (Configuration.OPCOUNTER_ENABLED) {
                  c.addCanonicalSubgraphs(1)
                  c.addValidSubgraphs(1)
               }

               nextComp.compute()
            }

            ret
         }
      }
   }

   def getProcessComputeFuncFirst(): ProcessComputeFunc[S] = {
      new ProcessComputeFunc[S] with Logging {

         var workStealingSys: WorkStealingSystem[S] = _

         def apply(iter: SubgraphEnumerator[S], c: Computation[S]): Long = {
            val config = c.getConfig()
            val execEngine = c.getExecutionEngine().
               asInstanceOf[SparkFromScratchEngine[S]]

            var start = System.currentTimeMillis
            val ret = processCompute(iter, c)
            var elapsed = System.currentTimeMillis - start

            logInfo (s"WorkStealingMode internal=${config.internalWsEnabled()}" +
               s" external=${config.externalWsEnabled()}")

            logInfo (s"InitialComputation step=${c.getStep}" +
               s" partitionId=${c.getPartitionId} took ${elapsed} ms")

            // setup work-stealing system
            start = System.currentTimeMillis
            if (config.wsEnabled()) {
               val gtagExecutorActor = execEngine.slaveActorRef
               workStealingSys = new WorkStealingSystem[S](
                  processCompute, gtagExecutorActor, new ConcurrentLinkedQueue())

               workStealingSys.workStealingCompute(c)
            }
            elapsed = System.currentTimeMillis - start

            logInfo (s"WorkStealingComputation step=${c.getStep}" +
               s" partitionId=${c.getPartitionId} took ${elapsed} ms")

            ret
         }

         override def toString = "EF"

         private def processCompute(iter: SubgraphEnumerator[S],
                                    c: Computation[S]): Long = {
            var ret = 0L
            val nextComp = c.nextComputation()
            while (iter.extend()) {
               if (Configuration.OPCOUNTER_ENABLED) {
                  c.addCanonicalSubgraphs(1)
                  c.addValidSubgraphs(1)
               }

               nextComp.compute()
            }

            ret
         }
      }
   }

   def getProcessComputeFuncFirstLast(): ProcessComputeFunc[S] = {
      new ProcessComputeFunc[S] with Logging {
         var workStealingSys: WorkStealingSystem[S] = _

         def apply(iter: SubgraphEnumerator[S], c: Computation[S]): Long = {
            val config = c.getConfig()
            val execEngine = c.getExecutionEngine().
               asInstanceOf[SparkFromScratchEngine[S]]

            var start = System.currentTimeMillis
            val ret = processCompute(iter, c)
            var elapsed = System.currentTimeMillis - start

            logInfo (s"WorkStealingMode internal=${config.internalWsEnabled()}" +
               s" external=${config.externalWsEnabled()}")

            logInfo (s"InitialComputation step=${c.getStep}" +
               s" partitionId=${c.getPartitionId} took ${elapsed} ms")

            // setup work-stealing system
            start = System.currentTimeMillis
            if (config.wsEnabled()) {
               val gtagExecutorActor = execEngine.slaveActorRef
               workStealingSys = new WorkStealingSystem[S](
                  processCompute, gtagExecutorActor, new ConcurrentLinkedQueue())

               workStealingSys.workStealingCompute(c)
            }
            elapsed = System.currentTimeMillis - start

            logInfo (s"WorkStealingComputation step=${c.getStep}" +
               s" partitionId=${c.getPartitionId} took ${elapsed} ms")

            ret
         }

         override def toString = "EFL"

         private def processCompute(iter: SubgraphEnumerator[S],
                                    c: Computation[S]): Long = {

            val subgraph = iter.getSubgraph
            val extensions = iter.getExtensions
            val numExtensions = extensions.size()
            var i = 0
            while (i < numExtensions) {
               subgraph.addWord(extensions.getu(i))
               c.process(subgraph)
               subgraph.removeLastWord()
               i += 1
            }

            if (Configuration.OPCOUNTER_ENABLED) {
               c.addValidSubgraphs(numExtensions)
               c.addCanonicalSubgraphs(numExtensions)
            }
            0
         }
      }
   }

   def getProcessComputeFuncLast(): ProcessComputeFunc[S] = {
      new ProcessComputeFunc[S] with Logging {

         override def toString = "EL"

         def apply(iter: SubgraphEnumerator[S], c: Computation[S]): Long = {
            val subgraph = iter.getSubgraph
            val extensions = iter.getExtensions
            val numExtensions = extensions.size()
            var i = 0
            while (i < numExtensions) {
               subgraph.addWord(extensions.getu(i))
               c.process(subgraph)
               subgraph.removeLastWord()
               i += 1
            }

            if (Configuration.OPCOUNTER_ENABLED) {
               c.addValidSubgraphs(numExtensions)
               c.addCanonicalSubgraphs(numExtensions)
            }

            0
         }
      }
   }
}
