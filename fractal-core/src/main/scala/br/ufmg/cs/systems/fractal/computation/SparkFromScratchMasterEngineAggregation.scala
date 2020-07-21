package br.ufmg.cs.systems.fractal.computation

import java.io.Serializable
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.function.IntConsumer

import akka.actor._
import br.ufmg.cs.systems.fractal.{Fractoid, Primitive}
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.{EventTimer, Logging, ProcessComputeFunc, ReflectionUtils}
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

   import SparkFromScratchMasterEngineAggregation._

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
   (defaultValue: Long, value: S => Long, reduce: (Long,Long) => Long)
   : RDD[Long] = {
      execEnginesRDD.map(_.computeAggregationLong(defaultValue, value, reduce))
   }

   override def objLongRDD[K <: Serializable : ClassTag]
   (key: S => K, defaultValue: Long, value: S => Long,
    reduce: (Long,Long) => Long)
   : RDD[(K,Long)] = {
      execEnginesRDD.flatMap(
         _.computeAggregationObjLong[K](key, defaultValue, value, reduce))
   }

   override def objObjRDD[K <: Serializable, V <: Serializable]
   (key: S => K, value: S => V, aggregate: (V,V) => Unit)
   : RDD[(K,V)] = {
      execEnginesRDD.flatMap(
         _.computeAggregationObjObj[K,V](key, value, aggregate))
   }

   override def execEnginesRDD: RDD[SparkEngine[S]] = {
      init(originalContainer)

      logInfo (s"${this} Computation starting from ${stepRDD}," +
         s", StorageLevel=${stepRDD.getStorageLevel}")

      // save original container, i.e., without parents' computations
      //val originalContainer = config.computationContainer[S]
      logInfo (s"From scratch computation (${this})." +
         s" Original computation: ${originalContainer}")

      // find out how many computations are pipelined
      val numComputations = originalContainer.setDepth(0)

      // adding accumulators to each computation
      val egAccums = new Array[LongAccumulator](numComputations)
      val awAccums = new Array[LongAccumulator](numComputations)
      val exAccums = new Array[LongAccumulator](numComputations)
      var i = 0
      while (i < numComputations) {
         val egKey = s"${VALID_SUBGRAPHS}_${i}"
         val awKey = s"${CANONICAL_SUBGRAPHS}_${i}"
         val exKey = s"${NEIGHBORHOOD_LOOKUPS}_${i}"

         egAccums(i) = sc.longAccumulator(egKey)
         awAccums(i) = sc.longAccumulator(awKey)
         exAccums(i) = sc.longAccumulator(exKey)

         aggAccums.update (egKey, egAccums(i))
         aggAccums.update (awKey, awAccums(i))
         aggAccums.update (exKey, exAccums(i))

         logInfo(s"Added accumulators (${egKey},${awKey},${exKey})")

         i += 1
      }

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
                        val extensionFuncFirst = getProcessComputeFuncFirst(
                           egAccums(depth),
                           awAccums(depth)
                        )
                        cc.shallowCopy(
                           processComputeOpt = Option(extensionFuncFirst),
                           nextComputationOpt = Option(ncc),
                           processOpt = None)
                     } else {
                        val extensionFuncMiddle = getProcessComputeFuncMiddle(
                           egAccums(depth),
                           awAccums(depth)
                        )
                        cc.shallowCopy(
                           processComputeOpt = Option(extensionFuncMiddle),
                           nextComputationOpt = Option(ncc),
                           processOpt = None)
                     }
                  case Primitive.F =>
                     val filterFunc = getProcessComputeFuncFilter(
                        egAccums(depth),
                        awAccums(depth)
                     )
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
                        val extensionFuncFirstLast = getProcessComputeFuncFirstLast(
                           egAccums(depth),
                           awAccums(depth)
                        )
                        cc.shallowCopy(
                           processComputeOpt = Option(extensionFuncFirstLast))
                     } else {
                        val extensionFuncLast = getProcessComputeFuncLast(
                           egAccums(depth),
                           awAccums(depth)
                        )
                        cc.shallowCopy(
                           processComputeOpt = Option(extensionFuncLast))
                     }
                  case Primitive.F =>
                     val filterFuncLast = getProcessComputeFuncFilterLast(
                        egAccums(depth),
                        awAccums(depth)
                     )
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
      //logInfo (s"SparkConfiguration estimated size = " +
      //   s"${SizeEstimator.estimate(config} bytes." +
      //s" ${config}")
      //logInfo (s"HadoopConfiguration estimated size = " +
      //   s"${SizeEstimator.estimate(config.hadoopConf)} bytes")

      validSubgraphsAccum = sc.longAccumulator
      aggAccums.update("valid_subgraphs", validSubgraphsAccum)

      /**
       * Local vars for clean serialization
       */
      val _step = step
      val _accums = aggAccums
      val _validSubgraphsAccum = validSubgraphsAccum
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
            accums = _accums,
            validSubgraphsAccum = _validSubgraphsAccum,
            //computation = computationCopy,
            computation = _computation,
            configuration = _configBc.value
         )

         Iterator[SparkEngine[S]](execEngine)
      }
   }

   /**
    * Master's computation takes place here, superstep by superstep
    */
   lazy val next: Boolean = true

   def getProcessComputeFuncFilterLast
   (_egAccum: LongAccumulator, _awAccum: LongAccumulator)
   : ProcessComputeFunc[S] = {
      new ProcessComputeFunc[S] with Logging {
         val egAccum = _egAccum
         val awAccum = _awAccum
         override def apply(enum: SubgraphEnumerator[S],
                            c: Computation[S]): Long = {
            val subgraph = enum.getSubgraph
            var ret = 0L
            awAccum.add(1)
            if (c.filter(subgraph)) {
               c.process(subgraph)
               egAccum.add(1)
               ret = 1L
            } else {
               ret = 0L
            }

            ret
         }

         override def toString = "FL"
      }
   }

   def getProcessComputeFuncFilter(_egAccum: LongAccumulator,
                                   _awAccum: LongAccumulator): ProcessComputeFunc[S] = {

         new ProcessComputeFunc[S] with Logging {

            val egAccum = _egAccum
            val awAccum = _awAccum

            override def apply(enum: SubgraphEnumerator[S],
                               c: Computation[S]): Long = {
            var ret = 0L
            var subgraphsGenerated = 0L
            var addWords = 0L
            val nextEnum = enum.extend()
            val subgraph = nextEnum.getSubgraph
            addWords += 1
            if (c.filter(subgraph)) {
               subgraphsGenerated += 1
               ret += c.nextComputation().compute(subgraph)
            }
            egAccum.add(subgraphsGenerated)
            awAccum.add(addWords)
            ret
         }

         override def toString = "F"
      }
   }

   def getProcessComputeFuncMiddle(_egAccum: LongAccumulator,
                                   _awAccum: LongAccumulator): ProcessComputeFunc[S] = {
      new ProcessComputeFunc[S] with Logging {
         val egAccum = _egAccum
         val awAccum = _awAccum

         override def toString = "EM"

         def apply(iter: SubgraphEnumerator[S], c: Computation[S]): Long = {
            var currentSubgraph: S = iter.getSubgraph
            var addWords = 0L
            var subgraphsGenerated = 0L
            var ret = 0L
            val nextComp = c.nextComputation()

            while (iter.hasNext) {
               val nextEnum = iter.extend()
               currentSubgraph = nextEnum.getSubgraph()
               addWords += 1
               subgraphsGenerated += 1
               currentSubgraph.nextExtensionLevel()
               ret += nextComp.compute(currentSubgraph)
               currentSubgraph.previousExtensionLevel()
            }

            awAccum.add(addWords)
            egAccum.add(subgraphsGenerated)

            ret
         }
      }
   }

   def getProcessComputeFuncFirst(_egAccum: LongAccumulator,
                             _awAccum: LongAccumulator): ProcessComputeFunc[S] = {
      new ProcessComputeFunc[S] with Logging {

         val egAccum = _egAccum
         val awAccum = _awAccum

         var workStealingSys: WorkStealingSystem[S] = _

         def apply(iter: SubgraphEnumerator[S], c: Computation[S]): Long = {
            val config = c.getConfig()
            val execEngine = c.getExecutionEngine().
               asInstanceOf[SparkFromScratchEngine[S]]

            //var currComp = c.nextComputation()
            //while (currComp != null) {
            //   currComp.setExecutionEngine(execEngine)
            //   currComp.init(config)
            //   currComp.initAggregations(config)
            //   currComp = currComp.nextComputation
            //}

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
            var currentSubgraph: S = iter.getSubgraph
            var addWords = 0L
            var subgraphsGenerated = 0L
            var ret = 0L
            val nextComp = c.nextComputation()

            while (iter.hasNext) {
               val nextEnum = iter.extend()
               currentSubgraph = nextEnum.getSubgraph()
               addWords += 1
               subgraphsGenerated += 1
               currentSubgraph.nextExtensionLevel()
               ret += nextComp.compute(currentSubgraph)
               currentSubgraph.previousExtensionLevel()
            }

            awAccum.add(addWords)
            egAccum.add(subgraphsGenerated)

            ret
         }
      }
   }

   def getProcessComputeFuncFirstLast(_egAccum: LongAccumulator,
                             _awAccum: LongAccumulator): ProcessComputeFunc[S] = {
      new ProcessComputeFunc[S] with Logging {
         val egAccum = _egAccum

         val awAccum = _awAccum

         var workStealingSys: WorkStealingSystem[S] = _

         val lastStepConsumer: LastStepConsumer[S] = new LastStepConsumer[S]

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
            var addWords = 0L
            var subgraphsGenerated = 0L
            val wordIds = iter.getWordIds()
            lastStepConsumer.set(iter.getSubgraph(), c)
            wordIds.forEach(lastStepConsumer)
            addWords += lastStepConsumer.addWords
            subgraphsGenerated += lastStepConsumer.subgraphsGenerated
            awAccum.add(addWords)
            egAccum.add(subgraphsGenerated)
            subgraphsGenerated
         }
      }
   }

   def getProcessComputeFuncLast(_egAccum: LongAccumulator,
                                 _awAccum: LongAccumulator): ProcessComputeFunc[S] = {
      new ProcessComputeFunc[S] with Logging {
         val egAccum = _egAccum

         val awAccum = _awAccum

         val lastStepConsumer: LastStepConsumer[S] = new LastStepConsumer[S]

         override def toString = "EL"

         def apply(iter: SubgraphEnumerator[S], c: Computation[S]): Long = {
            var addWords = 0L
            var subgraphsGenerated = 0L
            val wordIds = iter.getWordIds()
            lastStepConsumer.set(iter.getSubgraph(), c)
            wordIds.forEach(lastStepConsumer)
            addWords += lastStepConsumer.addWords
            subgraphsGenerated += lastStepConsumer.subgraphsGenerated
            awAccum.add(addWords)
            egAccum.add(subgraphsGenerated)
            subgraphsGenerated
         }
      }
   }
}

private class LastStepConsumer[S <: Subgraph] extends IntConsumer with
   Serializable {
   var subgraph: S = _
   var computation: Computation[S] = _
   var addWords: Long = _
   var subgraphsGenerated: Long = _

   def set(subgraph: S, computation: Computation[S]): LastStepConsumer[S] = {
      this.subgraph = subgraph
      this.computation = computation
      this.addWords = 0L
      this.subgraphsGenerated = 0L
      this
   }

   override def accept(w: Int): Unit = {
      addWords += 1
      subgraph.addWord(w)
      subgraphsGenerated += 1
      computation.process(subgraph)
      subgraph.removeLastWord()
   }
}


object SparkFromScratchMasterEngineAggregation {
   val NEIGHBORHOOD_LOOKUPS = "neighborhood_lookups"

   val NEIGHBORHOOD_LOOKUPS_ARR = {
      val arr = new Array[String](16)
      var i = 0
      while (i < arr.length) {
         arr(i) = s"${NEIGHBORHOOD_LOOKUPS}_${i}"
         i += 1
      }
      arr
   }

   def NEIGHBORHOOD_LOOKUPS(depth: Int): String = {
      NEIGHBORHOOD_LOOKUPS_ARR(depth)
   }

   val CANONICAL_SUBGRAPHS = "canonical_subgraphs"
   val VALID_SUBGRAPHS = "valid_subgraphs"
   val AGG_CANONICAL_FILTER = "canonical_filter"
}
