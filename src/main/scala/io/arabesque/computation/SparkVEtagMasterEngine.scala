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
import io.arabesque.utils.collection.AtomicBitSetArray
import io.arabesque.{ProcessComputeFunc, WordFilterFunc}
import org.apache.hadoop.io.NullWritable
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
class SparkVEtagMasterEngine[E <: Embedding](
    _config: SparkConfiguration[E],
    _parentOpt: Option[SparkMasterEngine[E]]) extends SparkMasterEngine [E] {

  import SparkVEtagMasterEngine._
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
    computeNext()
    //gtagMasterActorRef ! PoisonPill
    //val tmp = new SparkBtagMasterEngine (sc, config.copy(), parentOpt.getOrElse(null))
    //tmp.repetition = true
    //tmp.previousAggregationsBc = previousAggregationsBc
    //tmp.computeNext()
  }
  
  def computeNext(): Boolean = {

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

    // configure custom WordFilterFunc, except for computations of the last step
    val wordFilterFuncs = new Array[WordFilterFunc[E]](numComputations)
    
    //if ((numComputations - numComputationsLastStep) > 0) {
    //  val wordFilterFunc = getWordFilterFunc(cfAccums, gfAccums, gfcfAccums, 0)
    //  var j = 0
    //  while (j < wordFilterFuncs.length) {
    //    wordFilterFuncs(j) = wordFilterFunc
    //    j += 1
    //  }
    //}
    
    //if ((numComputations - numComputationsLastStep) > 0) {
    //  wordFilterFuncs(0) = getWordFilterFunc(cfAccums, gfAccums, gfcfAccums, 0)
    //}
    
    //val lastIdx = if (!repetition) {
    //  (numComputations - numComputationsLastStep) - 1
    //} else {
    //  numComputations - 1
    //}
    //if (lastIdx >= 0) {
    //  wordFilterFuncs(lastIdx) =
    //    getWordFilterFunc(cfAccums, gfAccums, gfcfAccums, 1)
    //}

    //val lastButOneIdx = if (!repetition) {
    //  (numComputations - numComputationsLastStep) - 2
    //} else {
    //  numComputations - 2
    //}
    //if (lastButOneIdx >= 0) {
    //  wordFilterFuncs(lastButOneIdx) =
    //    getWordFilterFunc(cfAccums, gfAccums, gfcfAccums, 0)
    //}

    //val lastIdx = (numComputations - numComputationsLastStep) - 1
    //val lastButOneIdx = (numComputations - numComputationsLastStep) - 2
    //if (repetition && lastIdx >= 0) {
    //  wordFilterFuncs(lastIdx) =
    //    getWordFilterFunc(cfAccums, gfAccums, gfcfAccums, 1)
    //}
    //if (!repetition && lastButOneIdx >= 0) {
    //  wordFilterFuncs(lastButOneIdx) =
    //    getWordFilterFunc(cfAccums, gfAccums, gfcfAccums, 1)
    //}

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
    
      val prevAggs = previousAggregationsBc.value.asInstanceOf[Map[String,_]]
      (prevAggs.get(VPREV_ENUM), prevAggs.get(EPREV_ENUM)) match {
        case (Some(vagg), Some(eagg)) =>

          val vtag = vagg.
            asInstanceOf[AggregationStorage[NullWritable,AtomicBitSetArray]].
            getValue(NullWritable.get())

          val etag = eagg.
            asInstanceOf[AggregationStorage[NullWritable,AtomicBitSetArray]].
            getValue(NullWritable.get())

          configBc.value.initializeWithTag(vtag, etag)

        case (None, None) =>
          configBc.value.initialize()

        case tags =>
          throw new RuntimeException(s"Not allowed, missing tag: ${tags}")
      }
          
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

      var enabledVertices: AtomicBitSetArray = _
      
      var enabledEdges: AtomicBitSetArray = _

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

          enabledVertices = new AtomicBitSetArray(config.getNumVertices())
          c.getAggregationStorage [NullWritable,AtomicBitSetArray] (VPREV_ENUM).
            aggregate(NullWritable.get(), enabledVertices)
          
          enabledEdges = new AtomicBitSetArray(config.getNumEdges())
          c.getAggregationStorage [NullWritable,AtomicBitSetArray] (EPREV_ENUM).
            aggregate(NullWritable.get(), enabledEdges)

          if (c.getStep == 0) {
            enabledEdges.enableAll()
          }

          // setup work-stealing system
          val gtagExecutorActor = execEngine.
            asInstanceOf[SparkFromScratchEngine[E]].gtagActorRef

          val callback = (consumer: EmbeddingIterator[E], ret: Long) => {
            if (ret > 0) {
              val embedding = consumer.getEmbedding()
              val prefixSize = consumer.getPrefix().size()
              embedding.applyTagTo(enabledVertices, enabledEdges, prefixSize - 1)
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
              currentEmbedding.applyTagFrom(enabledVertices, enabledEdges,
                currentEmbedding.getNumWords() - 1)
              validChildren += _validChildren
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
            currentEmbedding.applyTagFrom(enabledVertices, enabledEdges,
              currentEmbedding.getNumWords() - 1)
          }
        }

        awAccums(c.getDepth).add(addWords)
        egAccums(c.getDepth).add(embeddingsGenerated)

        embeddingsGenerated
      }

      private def untaggedLastComputation(iter: JavaIterator[E],
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

  //def getWordFilterFunc(
  //    _cfAccums: Array[LongAccumulator],
  //    _gfAccums: Array[LongAccumulator],
  //    _gfcfAccums: Array[LongAccumulator],
  //    idx: Int): WordFilterFunc[E] = {
  //  new WordFilterFunc[E] with Logging {
  //    val cfAccums = _cfAccums

  //    val gfAccums = _gfAccums

  //    val gfcfAccums = _gfcfAccums

  //    var enabledWords: AtomicBitSetArray = _

  //    var totalNumWords: Int = _

  //    def apply(e: E, w: Int, c: Computation[E]): Boolean = {
  //      if (enabledWords == null) {
  //        totalNumWords = c.getConfig().getNumWords()
  //        enabledWords = c.
  //          readAggregation[NullWritable,AtomicBitSetArray](PREV_ENUM).
  //          getValue(NullWritable.get())
  //      }
  //      enabledWords.contains(idx * totalNumWords + w)
  //    }
  //  }
  //}

  def aggregationRegister(cc: ComputationContainer[E], depth: Int)
    : (Computation[E]) => Unit = {

    val aggStorageClass =
      classOf[AggregationStorage[NullWritable,AtomicBitSetArray]]
    val keyClass = classOf[NullWritable]
    val valueClass = classOf[AtomicBitSetArray]
    val persistent = false
    val reductionFunc = new ReductionFunctionContainer(
      (s1: AtomicBitSetArray, s2: AtomicBitSetArray) => {
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
      null.asInstanceOf[EndAggregationFunction[NullWritable,AtomicBitSetArray]]

    // get the old init aggregations function in order to compose it
    val oldInitAggregation = cc.initAggregationsOpt match {
      case Some(initAggregations) => initAggregations
      case None => (c: Computation[E]) => {}
    }

    // construct an incremental init aggregations function
    (c: Computation[E]) => {
      oldInitAggregation (c) // init aggregations so far
      c.getConfig().registerAggregation [NullWritable,AtomicBitSetArray] (
        VPREV_ENUM,
        aggStorageClass, keyClass, valueClass, persistent,
        reductionFunc, endAggregationFunc, false)
      c.getConfig().registerAggregation [NullWritable,AtomicBitSetArray] (
        EPREV_ENUM,
        aggStorageClass, keyClass, valueClass, persistent,
        reductionFunc, endAggregationFunc, false)
    }
  }
}

object SparkVEtagMasterEngine {
  val VPREV_ENUM = "vprevious_enumeration"
  val EPREV_ENUM = "eprevious_enumeration"
}
