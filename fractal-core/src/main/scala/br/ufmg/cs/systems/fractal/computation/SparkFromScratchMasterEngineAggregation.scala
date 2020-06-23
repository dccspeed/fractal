package br.ufmg.cs.systems.fractal.computation

import java.io.Serializable
import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor._
import br.ufmg.cs.systems.fractal.Primitive
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.{EventTimer, Logging, ProcessComputeFunc}
import org.apache.spark.SparkContext
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
   _step: Int,
   _config: SparkConfiguration[S],
   _parentOpt: Option[SparkMasterEngine[S]]) extends SparkMasterEngine [S] {

   import SparkFromScratchMasterEngineAggregation._

   def step = _step

   def config: SparkConfiguration[S] = _config

   def parentOpt: Option[SparkMasterEngine[S]] = _parentOpt

   var masterActorRef: ActorRef = _

   def this(_sc: SparkContext, step: Int, config: SparkConfiguration[S],
            parent: SparkMasterEngine[S]) {
      this (step, config, Option(parent))
      sc = _sc
   }

   override def init(): Unit = {
      val start = System.currentTimeMillis

      super.init()

      // gtag computations must have incremental aggregations because we compute
      // from scratch all the steps, then if one of those depends on any previous
      // aggregation (e.g., fsm computation) we are safe
      config.set("incremental_aggregation", true)

      // gtag actor
      masterActorRef = ActorMessageSystem.createActor(this)

      logInfo(s"Started gtag-master-actor(step=${step}):" +
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
      init()

      logInfo (s"${this} Computation starting from ${stepRDD}," +
         s", StorageLevel=${stepRDD.getStorageLevel}")

      // save original container, i.e., without parents' computations
      val originalContainer = config.computationContainer[S]
      logInfo (s"From scratch computation (${this})." +
         s" Original computation: ${originalContainer}")

      // find out how many computations are pipelined
      val numComputations = {
         var cc = originalContainer
         var curr: SparkMasterEngine[S] = this
         while (curr.parentOpt.isDefined) {
            curr = curr.parentOpt.get
            cc = curr.config.computationContainer[S].withComputationAppended(cc)
         }
         cc.setDepth(0)
      }

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

      // add parents' computations
      var curr: SparkMasterEngine[S] = this
      while (curr.parentOpt.isDefined) {
         curr = curr.parentOpt.get
         cc = curr.config.computationContainer[S].withComputationAppended(cc)
      }

      val extensionFunc = getProcessComputeFunc(egAccums, awAccums)
      cc = {
         def withCustomFuncs(cc: ComputationContainer[S], depth: Int)
         : ComputationContainer[S] = cc.nextComputationOpt match {
            case Some(c) =>
               val ncc = withCustomFuncs(c.asInstanceOf[ComputationContainer[S]],
                  depth + 1)
               cc.primitiveOpt match {
                  case Some(Primitive.E) =>
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
                  case Some(Primitive.F) =>
                     val filterFunc = getProcessComputeFuncFilter(
                        egAccums(depth),
                        awAccums(depth)
                     )
                     cc.shallowCopy(
                        processComputeOpt = Option(filterFunc),
                        nextComputationOpt = Option(ncc),
                        processOpt = None)
                  case _ =>
                     cc.shallowCopy(
                        processComputeOpt = Option(extensionFunc),
                        nextComputationOpt = Option(ncc),
                        processOpt = None)
               }

            case None =>
               cc.primitiveOpt match {
                  case Some(Primitive.E) =>
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
                  case Some(Primitive.F) =>
                     val filterFuncLast = getProcessComputeFuncFilterLast(
                        egAccums(depth),
                        awAccums(depth)
                     )
                     cc.shallowCopy(
                        processComputeOpt = Option(filterFuncLast))
                  case _ =>
                     cc.shallowCopy(
                        processComputeOpt = Option(extensionFunc))
               }
         }

         cc = withCustomFuncs(cc, 0).withComputationLabel("first_computation")
         cc.setDepth(0)
         cc
      }

      // set the modified pipelined computation
      this.config.set(SparkConfiguration.COMPUTATION_CONTAINER, cc)

      logInfo (s"From scratch computation (${this}). Final computation: ${cc}")

      logInfo (s"SparkConfiguration estimated size = " +
         s"${SizeEstimator.estimate(config)} bytes")
      logInfo (s"HadoopConfiguration estimated size = " +
         s"${SizeEstimator.estimate(config.hadoopConf)} bytes")

      validSubgraphsAccum = sc.longAccumulator
      aggAccums.update("valid_subgraphs", validSubgraphsAccum)

      /**
       * Local vars for clean serialization
       */
      val _step = step
      val _accums = aggAccums
      val _validSubgraphsAccum = validSubgraphsAccum
      val _previousAggregationsBc = previousAggregationsBc
      val _configBc = configBc

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
            previousAggregationsBc = _previousAggregationsBc,
            configuration = _configBc.value
         )
         Iterator[SparkEngine[S]](execEngine)
      }
   }

   /**
    * Master's computation takes place here, superstep by superstep
    */
   lazy val next: Boolean = true

   def getProcessComputeFuncFilterLast(
                                         _egAccum: LongAccumulator,
                                         _awAccum: LongAccumulator): ProcessComputeFunc[S] = {
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

   def getProcessComputeFunc(_egAccums: Array[LongAccumulator],
                             _awAccums: Array[LongAccumulator]): ProcessComputeFunc[S] = {
      new ProcessComputeFunc[S] with Logging {
         val numComputations = _egAccums.length

         val egAccums = _egAccums

         val awAccums = _awAccums

         var workStealingSys: WorkStealingSystem[S] = _

         var lastStepConsumer: LastStepConsumer[S] = _

         override def toString = "NaivePC"

         def apply(iter: SubgraphEnumerator[S], c: Computation[S]): Long = {
            if (c.getDepth() == 0) {
               val config = c.getConfig()
               val execEngine = c.getExecutionEngine().
                  asInstanceOf[SparkFromScratchEngine[S]]

               egAccums(c.getDepth) = execEngine.
                  accums(s"${VALID_SUBGRAPHS}_${c.getDepth}")
               awAccums(c.getDepth) = execEngine.
                  accums(s"${CANONICAL_SUBGRAPHS}_${c.getDepth}")

               var currComp = c.nextComputation()
               while (currComp != null) {
                  val depth = currComp.getDepth()
                  currComp.setExecutionEngine(execEngine)
                  currComp.init(config)
                  currComp.initAggregations(config)
                  egAccums(depth) = execEngine.accums(
                     s"${VALID_SUBGRAPHS}_${depth}")
                  awAccums(depth) = execEngine.accums(
                     s"${CANONICAL_SUBGRAPHS}_${depth}")
                  currComp = currComp.nextComputation
               }

               lastStepConsumer = new LastStepConsumer[S]()

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
                  val slaveActorRef = execEngine.slaveActorRef
                  workStealingSys = new WorkStealingSystem[S](
                     processCompute, slaveActorRef, new ConcurrentLinkedQueue())

                  workStealingSys.workStealingCompute(c)
               }
               elapsed = System.currentTimeMillis - start

               logInfo (s"WorkStealingComputation step=${c.getStep}" +
                  s" partitionId=${c.getPartitionId} took ${elapsed} ms")

               ret
            } else {
               processCompute(iter, c)
            }
         }

         private def hasNextComputation(iter: SubgraphEnumerator[S],
                                        c: Computation[S], nextComp: Computation[S]): Long = {
            var currentSubgraph: S = iter.getSubgraph
            var addWords = 0L
            var subgraphsGenerated = 0L
            var ret = 0L

            while (iter.hasNext) {
               val nextEnum = iter.extend()
               currentSubgraph = nextEnum.getSubgraph()
               addWords += 1
               if (c.filter(currentSubgraph)) {
                  subgraphsGenerated += 1
                  currentSubgraph.nextExtensionLevel()
                  ret += nextComp.compute(currentSubgraph)
                  currentSubgraph.previousExtensionLevel()
               }
            }

            awAccums(c.getDepth).add(addWords)
            egAccums(c.getDepth).add(subgraphsGenerated)

            ret
         }

         private def lastComputation(iter: SubgraphEnumerator[S],
                                     c: Computation[S]): Long = {

            var addWords = 0L
            var subgraphsGenerated = 0L

            val wordIds = iter.getWordIds()
            if (wordIds != null) {
               lastStepConsumer.set(iter.getSubgraph(), c)
               wordIds.forEach(lastStepConsumer)
               addWords += lastStepConsumer.addWords
               subgraphsGenerated += lastStepConsumer.subgraphsGenerated
            } else {
               val subgraph = iter.next()
               addWords += 1
               if (c.filter(subgraph)) {
                  subgraphsGenerated += 1
                  c.process(subgraph)
               }
            }

            awAccums(c.getDepth).add(addWords)
            egAccums(c.getDepth).add(subgraphsGenerated)

            subgraphsGenerated
         }

         private def processCompute(iter: SubgraphEnumerator[S],
                                    c: Computation[S]): Long = {
            val nextComp = c.nextComputation()

            if (nextComp != null) {
               hasNextComputation(iter, c, nextComp)
            } else {
               lastComputation(iter, c)
            }
         }
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

            var currComp = c.nextComputation()
            while (currComp != null) {
               val depth = currComp.getDepth()
               currComp.setExecutionEngine(execEngine)
               currComp.init(config)
               currComp.initAggregations(config)
               currComp = currComp.nextComputation
            }

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

            var currComp = c.nextComputation()
            while (currComp != null) {
               val depth = currComp.getDepth()
               currComp.setExecutionEngine(execEngine)
               currComp.init(config)
               currComp.initAggregations(config)
               currComp = currComp.nextComputation
            }

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
