package io.arabesque.computation

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.{Iterator => JavaIterator}

import akka.actor._
import io.arabesque.aggregation._
import io.arabesque.aggregation.reductions._
import io.arabesque.cache.LZ4ObjectCache
import io.arabesque.conf.SparkConfiguration
import io.arabesque.embedding._
import io.arabesque.utils.Logging
import io.arabesque.utils.collection.IntSet
import io.arabesque.{ProcessComputeFunc, WordFilterFunc}
import org.apache.hadoop.io.IntWritable
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.util.{LongAccumulator, SizeEstimator}

import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * Underlying engine that runs the Arabesque master.
 * It interacts directly with the RDD interface in Spark by handling the
 * SparkContext.
 */
class SparkViewTagMasterEngine[E <: Embedding](
    _config: SparkConfiguration[E],
    _parentOpt: Option[SparkMasterEngine[E]]) extends SparkMasterEngine [E] {

  import SparkViewTagMasterEngine._
  import SparkFromScratchMasterEngine._
  import SparkMasterEngine._

  var repetition: Boolean = false

  def config: SparkConfiguration[E] = _config
  
  def parentOpt: Option[SparkMasterEngine[E]] = _parentOpt
  
  var gtagMasterActorRef: ActorRef = _
  
  def this(_sc: SparkContext, config: SparkConfiguration[E]) {
    this (config, None)
    sc = _sc
    init()
  }
  
  def this(_sc: SparkContext, config: SparkConfiguration[E],
      parent: SparkMasterEngine[E]) {
    this (config, Option(parent))
    sc = _sc
    init()
  }

  override def init(): Unit = {
    val start = System.currentTimeMillis

    super.init()
    
    // gtag computations must have incremental aggregations because we compute
    // from scratch all the steps, then if one of those depends on any previous
    // aggregation (e.g., fsm computation) we are secured
    config.set("incremental_aggregation", true)

    // gtag actor
    gtagMasterActorRef = GtagMessagingSystem.createActor(this)

    logInfo(s"Started gtag-master-actor(step=${superstep}):" +
      s" ${gtagMasterActorRef}")

    val end = System.currentTimeMillis

    logInfo(s"${this} took ${(end - start)}ms to initialize.")
  }
  
  /**
   * Master's computation takes place here, superstep by superstep
   */
  lazy val next: Boolean = {

    val superstepStart = System.currentTimeMillis
    
    logInfo (s"${this} Computation starting from ${superstepRDD}," +
      s", StorageLevel=${superstepRDD.getStorageLevel}")

    logInfo(s"Computation step=${superstep} configId=${this.config.getId}")

    // save original container, i.e., without parents' computations
    val originalContainer = config.computationContainer[E]
    logInfo (s"From scratch computation (${this})." +
      s" Original computation: ${originalContainer}")

    // find out how many computations are pipelined
    val (numComputations, numComputationsLastStep) = {
      var cc = originalContainer
      var curr: SparkMasterEngine[E] = this
      val numComputationsLastStep = curr.config.
        computationContainer[E].setDepth(0)
      while (curr.parentOpt.isDefined) {
        curr = curr.parentOpt.get
        cc = curr.config.computationContainer[E].withComputationAppended(cc)
      }
      (cc.setDepth(0), numComputationsLastStep)
    }

    // adding accumulators to each computation
    val veAccums = new Array[LongAccumulator](numComputations)
    val ceAccums = new Array[LongAccumulator](numComputations)
    val eAccums = new Array[LongAccumulator](numComputations)  
    
    var i = 0
    while (i < numComputations) {
      val veKey = s"${VALID_EMBEDDINGS}_${i}"
      val ceKey = s"${CANONICAL_EMBEDDINGS}_${i}"
      val eKey = s"${NEIGHBORHOOD_LOOKUPS}_${i}"

      veAccums(i) = sc.longAccumulator(veKey)
      ceAccums(i) = sc.longAccumulator(ceKey)
      eAccums(i) = sc.longAccumulator(eKey)

      aggAccums.update (veKey, veAccums(i))
      aggAccums.update (ceKey, ceAccums(i))
      aggAccums.update (eKey, eAccums(i))

      logInfo(s"Added accumulators (${veKey},${ceKey},${eKey})")

      i += 1
    }
    
    // we will contruct the pipeline in this var
    var cc = originalContainer.withComputationLabel("last_step_begins")

    // accumulate parents' computations
    var curr: SparkMasterEngine[E] = this
    while (curr.parentOpt.isDefined) {
      curr = curr.parentOpt.get
      cc = curr.config.computationContainer[E].withComputationAppended(cc)
    }

    val wordFilterFuncs = new Array[WordFilterFunc[E]](numComputations)

    // configure custom ProcessComputeFunc and aggregations
    val processComputeFunc = getProcessComputeFunc(veAccums, ceAccums)
    cc = {
      def withCustomFuncs(cc: ComputationContainer[E], depth: Int)
        : ComputationContainer[E] = cc.nextComputationOpt match {
        case Some(c) =>
          val ncc = withCustomFuncs(c.asInstanceOf[ComputationContainer[E]],
            depth + 1)
          val _cc = cc.shallowCopy(
            processComputeOpt = Option(processComputeFunc),
            initAggregationsOpt = Option(aggregationRegister(cc, depth)),
            nextComputationOpt = Option(ncc),
            wordFilterOpt = Option(wordFilterFuncs(depth)))
          _cc.initAggregations(this.config)
          _cc

        case None =>
          val _cc = cc.shallowCopy(
            processComputeOpt = Option(processComputeFunc),
            initAggregationsOpt = Option(aggregationRegister(cc, depth)),
            wordFilterOpt = Option(wordFilterFuncs(depth)))
          _cc.initAggregations(this.config)
          _cc
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

    val _aggAccums = aggAccums

    val execEngines = getExecutionEngines (
      superstepRDD = superstepRDD,
      superstep = superstep,
      configBc = configBc,
      aggAccums = _aggAccums,
      previousAggregationsBc = previousAggregationsBc)

    execEngines.persist(MEMORY_ONLY_SER)
    execEngines.foreachPartition (_ => {})

    val enumerationElapsed = System.currentTimeMillis - superstepStart

    logInfo(s"Enumeration step=${superstep} took ${enumerationElapsed} ms")

    /** [1] We extract and aggregate the *aggregations* globally.
     */
    val aggregationsFuture = getAggregations (execEngines, numPartitions)
    // aggregations
    Await.ready (aggregationsFuture, atMost = Duration.Inf)
    aggregationsFuture.value.get match {

      case Success(previousAggregations) =>
        aggregations = mergeOrReplaceAggregations (aggregations,
          previousAggregations)
        
        aggregations.foreach { case (name, agg) =>
          val mapping = agg.getMapping
          val numMappings = agg.getNumberMappings
          logInfo (s"Aggregation[${name}][numMappings=${numMappings}]\n" +
            s"${mapping.map(t => s"Aggregation[${name}][${superstep}]" +
            s" ${t._1}: ${t._2}").mkString("\n")}")
        }

        previousAggregationsBc = sc.broadcast (aggregations)

      case Failure(e) =>
        logError (s"Error in collecting aggregations: ${e.getMessage}")
        throw e
    }
    
    execEngines.unpersist()

    // whether the user chose to customize master computation, executed every
    // superstep
    masterComputation.compute()
    
    // print stats
    aggAccums.foreach { case (name, accum) =>
      logInfo (s"Accumulator[${superstep}][${name}]: ${accum.value}")
      accum.reset()
    }

    // master will send poison pills to all executor actors of this step
    gtagMasterActorRef ! Reset
    
    val superstepFinish = System.currentTimeMillis
    logInfo (
      s"Superstep $superstep finished in ${superstepFinish - superstepStart} ms"
    )

    // make sure we maintain the engine's original state
    this.config.set(SparkConfiguration.COMPUTATION_CONTAINER, originalContainer)

    !sc.isStopped && !isComputationHalted
  }

  /**
   * Creates an RDD of execution engines 
   */
  def getExecutionEngines[E <: Embedding](
      superstepRDD: RDD[LZ4ObjectCache],
      superstep: Int,
      configBc: Broadcast[SparkConfiguration[E]],
      aggAccums: Map[String,LongAccumulator],
      previousAggregationsBc: Broadcast[_]): RDD[SparkEngine[E]] = {

    val execEngines = superstepRDD.mapPartitionsWithIndex { (idx, cacheIter) =>
    
      configBc.value.initialize()
          
      val execEngine = new SparkFromScratchEngine [E] (
        partitionId = idx,
        superstep = superstep,
        accums = aggAccums,
        previousAggregationsBc = previousAggregationsBc,
        configurationId = configBc.value.getId
      )

      execEngine.init()
      execEngine.compute (cacheIter)
      execEngine.finalize()
          
      configBc.value.uninitialize()

      Iterator[SparkEngine[E]](execEngine)
    }

    execEngines
  }

  def getProcessComputeFunc(_egAccums: Array[LongAccumulator],
      _awAccums: Array[LongAccumulator]): ProcessComputeFunc[E] = {
    new ProcessComputeFunc[E] with Logging { 
      val numComputations = _egAccums.length

      val egAccums = _egAccums

      val awAccums = _awAccums

      var enabledWordEdges: AggregationStorage[IntWritable,IntSet] = _

      var reusableInt: IntWritable = _
      
      var reusableSet: IntSet = _
      
      var workStealingSys: WorkStealingSystem[E] = _
        
      var totalNumWords: Int = _

      var computations: Array[Computation[E]] = _
      
      var iterators: Array[JavaIterator[E]] = _
      
      def apply(iter: JavaIterator[E], c: Computation[E]): Long = {
        if (c.getDepth() == 0) {
          val config = c.getConfig()
          val execEngine = c.getExecutionEngine().asInstanceOf[SparkEngine[E]]

          egAccums(c.getDepth) = execEngine.
            accums(s"${VALID_EMBEDDINGS}_${c.getDepth}")
          awAccums(c.getDepth) = execEngine.
            accums(s"${CANONICAL_EMBEDDINGS}_${c.getDepth}")

          var currComp = c.nextComputation()
          while (currComp != null) {
            val depth = currComp.getDepth()
            currComp.setExecutionEngine(execEngine)
            currComp.init(config)
            currComp.initAggregations(config)
            egAccums(depth) = execEngine.accums(
              s"${VALID_EMBEDDINGS}_${depth}")
            awAccums(depth) = execEngine.accums(
              s"${CANONICAL_EMBEDDINGS}_${depth}")
            currComp = currComp.nextComputation
          }

          totalNumWords = config.getNumWords()

          reusableInt = new IntWritable()
          reusableSet = new IntSet(1)

          enabledWordEdges = 
            c.getAggregationStorage [IntWritable,IntSet] (PREV_ENUM)

          // setup work-stealing system
          val gtagExecutorActor = execEngine.
            asInstanceOf[SparkFromScratchEngine[E]].gtagActorRef

          val callback = {
            val reusableInt = new IntWritable()
            val reusableSet = new IntSet(1)
            (consumer: EmbeddingIterator[E], ret: Long) => {
              if (ret > 0) {
                val prefix = consumer.getPrefix()
                val prefixSize = prefix.size()
                var i = 0
                while (i < prefixSize - 1) {
                  reusableInt.set(prefix.getUnchecked(i))
                  reusableSet.clear()
                  reusableSet.addInt(prefix.getUnchecked(i + 1))
                  enabledWordEdges.aggregateWithReusables(reusableInt, reusableSet)
                  i += 1
                }
              }
            }
          }

          workStealingSys = new WorkStealingSystem[E](
            processCompute, gtagExecutorActor, new ConcurrentLinkedQueue(),
            callback = callback)
          
          val ret = processCompute(iter, c)
          workStealingSys.workStealingCompute(c)
          ret

        } else {
          processCompute(iter, c)
        }
      }

      private def hasNextComputation(iter: JavaIterator[E],
          c: Computation[E], nextComp: Computation[E]): Long = {
        var currentEmbedding: E = null.asInstanceOf[E]
        var validChildren = 0L
        var _validChildren = 0L
        var addWords = 0L
        var embeddingsGenerated = 0L

        while (iter.hasNext) {
          currentEmbedding = iter.next
          addWords += 1

          if (c.filter(currentEmbedding)) {

            embeddingsGenerated += 1
            currentEmbedding.nextExtensionLevel
            _validChildren = nextComp.compute(currentEmbedding)
            currentEmbedding.previousExtensionLevel

            if (_validChildren > 0) {
              validChildren += _validChildren
              val words = currentEmbedding.getWords()
              val numWords = words.size()
              if (numWords > 1) {
                reusableInt.set(words.getUnchecked(numWords - 2))
                reusableSet.clear()
                reusableSet.addInt(words.getUnchecked(numWords - 1))
                enabledWordEdges.aggregateWithReusables(
                  reusableInt, reusableSet)
              }
            }
          }
        }
        
        awAccums(c.getDepth).add(addWords)
        egAccums(c.getDepth).add(embeddingsGenerated)

        validChildren
      }

      private def lastComputation(iter: JavaIterator[E],
          c: Computation[E]): Long = {
        var currentEmbedding: E = null.asInstanceOf[E]
        var addWords = 0L
        var embeddingsGenerated = 0L

        while (iter.hasNext) {
          currentEmbedding = iter.next
          addWords += 1
          if (c.filter(currentEmbedding)) {
            embeddingsGenerated += 1
            c.process(currentEmbedding)
            val words = currentEmbedding.getWords()
            val numWords = words.size()
            if (numWords > 1) {
              reusableInt.set(words.getUnchecked(numWords - 2))
              reusableSet.clear()
              reusableSet.addInt(words.getUnchecked(numWords - 1))
              enabledWordEdges.aggregateWithReusables(
                reusableInt, reusableSet)
            }
          }
        }

        awAccums(c.getDepth).add(addWords)
        egAccums(c.getDepth).add(embeddingsGenerated)

        embeddingsGenerated
      }

      private def processCompute(iter: JavaIterator[E],
          c: Computation[E]): Long = {
        val nextComp = c.nextComputation()

        val ret = if (nextComp != null) {
          hasNextComputation(iter, c, nextComp)
        } else {
          lastComputation(iter, c)
        }

        ret
      }
    }
  }

  def aggregationRegister(cc: ComputationContainer[E], depth: Int)
    : (Computation[E]) => Unit = {

    val aggStorageClass =
      classOf[AggregationStorage[IntWritable,IntSet]]
    val keyClass = classOf[IntWritable]
    val valueClass = classOf[IntSet]
    val persistent = false
    val reductionFunc = new ReductionFunctionContainer(
      (s1: IntSet, s2: IntSet) => {
        if (s1 != null) {
          if (s2 != null) {
            s1.union(s2)
          } else {
            s1
          }
        } else {
          if (s2 != null) {
            s2
          } else {
            null
          }
        }
      }
    )
    
    val endAggregationFunc =
      null.asInstanceOf[EndAggregationFunction[IntWritable,IntSet]]

    // get the old init aggregations function in order to compose it
    val oldInitAggregation = cc.initAggregationsOpt match {
      case Some(initAggregations) => initAggregations
      case None => (c: Computation[E]) => {}
    }

    // construct an incremental init aggregations function
    (c: Computation[E]) => {
      oldInitAggregation (c) // init aggregations so far
      c.getConfig().registerAggregation [IntWritable,IntSet] (
        PREV_ENUM,
        aggStorageClass, keyClass, valueClass, persistent,
        reductionFunc, endAggregationFunc, false)
    }
  }
}

object SparkViewTagMasterEngine {
  val PREV_ENUM = "previous_enumeration"
}
