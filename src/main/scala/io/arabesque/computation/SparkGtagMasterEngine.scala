package io.arabesque.computation

import akka.actor._
import akka.util.Timeout

import com.typesafe.config.ConfigFactory

import io.arabesque.{ProcessComputeFunc, WordFilterFunc}
import io.arabesque.aggregation._
import io.arabesque.aggregation.reductions._
import io.arabesque.cache.{ByteArrayObjectCache, LZ4ObjectCache}
import io.arabesque.conf.SparkConfiguration
import io.arabesque.embedding._
import io.arabesque.graph.BasicMainGraph
import io.arabesque.utils.{Logging, SerializableConfiguration}
import io.arabesque.utils.collection.IntArrayList

import java.io._
import java.util.{Iterator => JavaIterator, Properties}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicLongArray

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, LongWritable, Writable}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.{Accumulator, HashPartitioner, SparkContext}
import org.apache.spark.util.SizeEstimator

import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Random, Success}

/**
 * Underlying engine that runs the Arabesque master.
 * It interacts directly with the RDD interface in Spark by handling the
 * SparkContext.
 */
class SparkGtagMasterEngine[E <: Embedding](
    _config: SparkConfiguration[E],
    _parentOpt: Option[SparkMasterEngine[E]]) extends SparkMasterEngine [E] {

  def config: SparkConfiguration[E] = _config
  
  config.initialize()

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
      s", StorageLevel=${superstepRDD.getStorageLevel}" +
      s", previousAggregations=${previousAggregationsBc.value.asInstanceOf[Map[String,AggregationStorage[_,_]]].get("support")}")

    // custom container functions
    val processComputeFunc = getProcessComputeFunc()
    val wordFilterFunc = getWordFilterFunc()

    // accumulates computations from parents
    var curr: SparkMasterEngine[E] = this
    val originalContainer = curr.config.computationContainer[E]
    logInfo (s"From scratch computation (${this})." +
      s" Original computation: ${originalContainer}")

    // include parent computations
    var cc = originalContainer.withComputationLabel("last_step_begins")
    while (curr.parentOpt.isDefined) {
      curr = curr.parentOpt.get
      cc = curr.config.computationContainer[E].
        shallowCopy (wordFilterOpt = Option(wordFilterFunc)).
        withComputationAppended(cc)
    }

    // set custom functions
    def withCustomFuncs(cc: ComputationContainer[E])
      : ComputationContainer[E] = {
      cc.nextComputationOpt match {
        case Some(c) =>
          val ncc = withCustomFuncs(c.asInstanceOf[ComputationContainer[E]])
          cc.shallowCopy(processComputeOpt = Option(processComputeFunc),
            initAggregationsOpt = Option(aggregationRegister(cc)),
            nextComputationOpt = Option(ncc))

        case None =>
          cc.shallowCopy(processComputeOpt = Option(processComputeFunc),
            initAggregationsOpt = Option(aggregationRegister(cc)))
      }
    }

    cc = withCustomFuncs(cc).withComputationLabel("first_computation")

    this.config.set(SparkConfiguration.COMPUTATION_CONTAINER, cc)

    // NOTE: We need this extra initAggregations because this communication
    // strategy adds a 'previous_enumeration' aggregation
    cc.initAggregations(this.config)

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

    execEngines.persist(MEMORY_ONLY)
    execEngines.foreachPartition (_ => {})

    /** [1] We extract and aggregate the *aggregations* globally.
     */
    val aggregationsFuture = getAggregations (execEngines, numPartitions)
    // aggregations
    Await.ready (aggregationsFuture, atMost = Duration.Inf)
    aggregationsFuture.value.get match {
      case Success(previousAggregations) =>
        aggregations = mergeOrReplaceAggregations (aggregations,
          previousAggregations)
        
        logInfo (s"""Aggregations and sizes
          ${aggregations.
          map(tup => (tup._1,tup._2.getNumberMappings)).mkString("\n")}
        """)

        aggregations.foreach { case (name, agg) =>
          val mapping = agg.getMapping
          logInfo (s"Aggregation[${name}]\n" +
            s"${mapping.map(t => s"${t._1}: ${t._2}").mkString("\n")}")
        }

        previousAggregationsBc = sc.broadcast (aggregations)

      case Failure(e) =>
        logError (s"Error in collecting aggregations: ${e.getMessage}")
        throw e
    }

    logInfo (s"StorageLevel = ${storageLevel}")

    // whether the user chose to customize master computation, executed every
    // superstep
    masterComputation.compute()
    
    // print stats
    aggAccums = aggAccums.map { case (name,accum) =>
      logInfo (s"Accumulator[$name]: ${accum.value}")
      (name -> sc.accumulator [Long] (0L, name))
    }

    gtagMasterActorRef ! Reset
    
    val superstepFinish = System.currentTimeMillis
    logInfo (
      s"Superstep $superstep finished in ${superstepFinish - superstepStart} ms"
    )

    // make sure we maintain the engine's original state
    this.config.set(SparkConfiguration.COMPUTATION_CONTAINER, originalContainer)

    !sc.isStopped
  }

  /**
   * Creates an RDD of execution engines 
   * TODO
   */
  private def getExecutionEngines[E <: Embedding](
      superstepRDD: RDD[LZ4ObjectCache],
      superstep: Int,
      configBc: Broadcast[SparkConfiguration[E]],
      aggAccums: Map[String,Accumulator[_]],
      previousAggregationsBc: Broadcast[_]): RDD[SparkEngine[E]] = {

    val execEngines = superstepRDD.mapPartitionsWithIndex { (idx, cacheIter) =>

      configBc.value.initialize()

      val execEngine = new SparkGtagEngine [E] (
        partitionId = idx,
        superstep = superstep,
        accums = aggAccums,
        previousAggregationsBc = previousAggregationsBc,
        configuration = configBc.value
      )

      execEngine.init()
      val prevEnum = execEngine.getAggregatedValue [
        AggregationStorage[IntWritable,LongWritable]] ("previous_enumeration")
      if (prevEnum != null) {
        prevEnum.setAtomicArrayDirty()
      }
      execEngine.compute (cacheIter)
      execEngine.finalize()
      Iterator[SparkEngine[E]](execEngine)
    }

    execEngines
  }

  protected def getProcessComputeFunc(): ProcessComputeFunc[E] = {
    new ProcessComputeFunc[E] { 
      var wordCountsOpt: Option[AtomicLongArray] = None
      var gtagExecutorActorOpt: Option[ActorRef] = None
      lazy val remoteWorkQueue: ConcurrentLinkedQueue[StealWorkResponse] =
        new ConcurrentLinkedQueue()

      def getWordCounts(c: Computation[E])
        : AtomicLongArray = wordCountsOpt match {
        case Some(wordCounts) =>
          wordCounts

        case None =>
          val prevAgg = c.
            readAggregation[IntWritable,LongWritable]("previous_enumeration")
          val wordCounts = if (prevAgg != null) {
            val embeddingClass = c.getConfig().getEmbeddingClass
            val numWords = {
              if (embeddingClass == classOf[EdgeInducedEmbedding]) {
                c.getConfig().getMainGraph[BasicMainGraph].getNumberEdges()
              } else if (embeddingClass == classOf[VertexInducedEmbedding]) {
                c.getConfig().getMainGraph[BasicMainGraph].getNumberVertices()
              } else {
                throw new RuntimeException(
                  s"Unknown embedding type ${embeddingClass}")
                0
              }
            }
            prevAgg.asAtomicArray(numWords)
          } else {
            null
          }
          wordCountsOpt = Option(wordCounts)
          wordCounts
      }

      def getGtagExecutorActor(c: Computation[E]): ActorRef = {
        gtagExecutorActorOpt match {
          case Some(gtagExecutorActor) =>
            gtagExecutorActor

          case None =>
            val gtagExecutorActor = c.getExecutionEngine().
              asInstanceOf[SparkGtagEngine[E]].gtagActorRef
            gtagExecutorActorOpt = Option(gtagExecutorActor)
            gtagExecutorActor
        }
      }

      def apply(iter: JavaIterator[E], c: Computation[E]): Int = {
        processCompute(iter, c)
        if (c.computationLabel() == "first_computation") {
          workStealingCompute(c)
        }
        0
      }
      
      private def processCompute(iter: JavaIterator[E],
          c: Computation[E]): Int = {
        val nextComp = c.nextComputation()
        val wordCounts = getWordCounts(c)
        var currentEmbedding: E = null.asInstanceOf[E]
        val writableOne = new LongWritable(1)
        val reusableIdWritable = new IntWritable()
        var backtrack = 0
        
        if (nextComp != null) {
          nextComp.setExecutionEngine(c.getExecutionEngine())
          nextComp.init(c.getConfig())
          nextComp.initAggregations(c.getConfig())

          if (wordCounts != null &&
              nextComp.computationLabel() == "last_step_begins") {
            while (iter.hasNext) {
              backtrack = 0
              currentEmbedding = iter.next

              if (c.filter(currentEmbedding)) {
                // c.process(currentEmbedding)

                //val words = currentEmbedding.getWords
                //var i = 0
                //while (i < currentEmbedding.getNumWords) {
                //  val currCount = wordCounts.decrementAndGet(
                //    words.get(i))

                //  if (currCount == 0) {
                //    backtrack = backtrack max (
                //      currentEmbedding.getNumWords - (i + 1))
                //  } else if (currCount < 0) {
                //    throw new RuntimeException(
                //      s"Counter should never be less than zero:" +
                //      s" belowzero step=${c.getStep}" +
                //      s" partitionId=${c.getPartitionId}" +
                //      s" embedding=${currentEmbedding} word=${words.get(i)}" +
                //      s" currComp=${currCount} computation=${c}")
                //  }

                //  i += 1
                //}

                currentEmbedding.nextEntensionLevel
                backtrack = backtrack max nextComp.compute(currentEmbedding)
                currentEmbedding.previousExtensionLevel

                if (backtrack > 0) {
                  iter.hasNext
                  return backtrack - 1
                }
              }
            }
          } else {
            while (iter.hasNext) {
              backtrack = 0
              currentEmbedding = iter.next

              if (c.filter(currentEmbedding)) {
                // c.process(currentEmbedding)

                currentEmbedding.nextEntensionLevel
                backtrack = backtrack max nextComp.compute(currentEmbedding)
                currentEmbedding.previousExtensionLevel

                if (backtrack > 0) {
                  iter.hasNext
                  return backtrack - 1
                }
              }
            }
          }
        } else {
          while (iter.hasNext) {
            currentEmbedding = iter.next
            if (c.filter(currentEmbedding)) {
              if (c.shouldExpand(currentEmbedding)) {

                // log word counts for next enumeration
                val words = currentEmbedding.getWords()
                val numWords = currentEmbedding.getNumWords()
                var i = 0
                while (i < numWords) {
                  reusableIdWritable.set(words.get(i))
                  c.map("previous_enumeration", reusableIdWritable, writableOne)
                  i += 1
                }

                c.getExecutionEngine().processExpansion(currentEmbedding)
              }
              c.process(currentEmbedding)
            }
          }
        }
        0
      }

      private def workStealingCompute(c: Computation[E]): Unit = {
        implicit val executionContext = GtagMessagingSystem.akkaExecutorContext
        implicit val timeout = Timeout(15 seconds)
        var workStealed = true
        @volatile var remoteRequestSent = false
        var remoteCancellable = null.asInstanceOf[Cancellable]
        val gtagExecutorActor = getGtagExecutorActor(c)

        /**
         * This callback is scheduled right after a remote stealing request, as
         * a retrying mechanism.
         */
        def requestExpirer: Unit = {
          remoteRequestSent = false
        }

        /**
         * Try to steal work from remote workers. Additionally, it schedules a
         * request expirerer.
         */
        def handleRemoteSteal: Unit = GtagMessagingSystem.akkaSysOpt match {
          case Some(akkaSys) =>
            if (remoteCancellable != null) {
              remoteCancellable.cancel()
            }

            val (requestId, requestFuture) =
              GtagMessagingSystem.tryStealWorkFromRemote(c)
            requestFuture.onComplete {
              case Success(response: StealWorkResponse) =>
                assert (remoteWorkQueue.add(response))

              case Success(response) =>
                throw new RuntimeException(s"Unknown response: ${response}")

              case Failure(e) =>
                throw e
            }
            remoteRequestSent = true

            remoteCancellable = akkaSys.scheduler.scheduleOnce(1 seconds)(
              requestExpirer)(executionContext)

          case None =>
            // do nothing
        }

        do {
          val workStealedLocally = workStealingComputeLocal(c)

          if (!workStealedLocally && !remoteRequestSent) {
            handleRemoteSteal
          }

          val response = remoteWorkQueue.poll()
          if (response != null) {
            response.workOpt match {
              case Some(embeddingBatchBytes) =>
                val consumer = GtagMessagingSystem.
                  deserializeEmbeddingBatch(embeddingBatchBytes, c)
                val computation = consumer.getComputation()

                val start = System.currentTimeMillis
                gtagExecutorActor ! Log(s"WorkStealedRemote" +
                  s" step=${c.getStep}" +
                  s" stealedByPartitionId=${computation.getPartitionId}" +
                  s" consumer=${consumer}" +
                  s" computation=${computation}")

                processCompute(consumer, computation)

                val end = System.currentTimeMillis
                gtagExecutorActor ! Log(s"WorkStealedAndProcessedRemote" +
                  s" step=${c.getStep}"+
                  s" stealedByPartitionId=${computation.getPartitionId}" +
                  s" consumer=${consumer}" +
                  s" computation=${computation}" +
                  s" elapsed=${(end - start)}ms")

                if (remoteCancellable != null) {
                  remoteCancellable.cancel()
                }
                remoteRequestSent = false

              case None =>
                if (response.numPeers == c.getNumberPartitions) {
                  workStealed = false
                }
                if (remoteCancellable != null) {
                  remoteCancellable.cancel()
                }
                remoteRequestSent = false
            }
          }
        } while (workStealed || !remoteWorkQueue.isEmpty)
      }

      private def workStealingComputeLocal(c: Computation[E]): Boolean = {
        val gtagExecutorActor = getGtagExecutorActor(c)
        val random = new Random(c.getPartitionId)
        val computations = random.shuffle(
          SparkGtagEngine.activeComputations.
          filter { case ((s,p),_) =>
            s == c.getStep() && p != c.getPartitionId
          }.map(_._2).toSeq.asInstanceOf[Seq[Computation[E]]]
          )

        var stealed = false
        var i = 0
        while (i < computations.length) {
          val currComp = computations(i)
          val consumer = currComp.forkConsumer()
          if (consumer != null) {
            stealed = true
            val label = consumer.computationLabel()

            // reach proper computation depth w.r.t. the stealed work
            var curr = c
            while (curr != null && curr.computationLabel() != label) {
              curr = curr.nextComputation()
            }

            assert (curr != null, s"label=${label} ${c}")

            val start = System.currentTimeMillis
            gtagExecutorActor ! Log(s"%%% workStealed step=${c.getStep}" +
              s" stealedByPartitionId=${curr.getPartitionId}" +
              s" stealedFromPartitionId=${currComp.getPartitionId}" +
              s" consumer=${consumer}" +
              s" computation=${curr}")

            val boundedIterator = new Iterator[E] {
              val batchSize = {
                val gtagBatchLow = c.getConfig().getGtagBatchSizeLow()
                val gtagBatchHigh = c.getConfig().getGtagBatchSizeHigh()
                random.nextInt(
                  gtagBatchHigh - gtagBatchLow + 1) + gtagBatchLow
              }
              var i = 0

              override def hasNext(): Boolean = {
                i < batchSize && consumer.hasNext
              }

              override def next(): E = {
                i += 1
                consumer.next
              }
            }

            processCompute(boundedIterator, curr)

            val end = System.currentTimeMillis
            gtagExecutorActor ! Log(s"WorkStealedAndProcessed" +
              s" step=${c.getStep}" +
              s" stealedByPartitionId=${curr.getPartitionId}" +
              s" stealedFromPartitionId=${currComp.getPartitionId}" +
              s" consumer=${consumer}" +
              s" computation=${curr}" +
              s" elapsed=${(end - start)}ms")

            currComp.joinConsumer(consumer)
          }
          i += 1
        }
        stealed
      }
    }
  }

  private def getWordFilterFunc(): WordFilterFunc[E] = {
    new WordFilterFunc[E] {
      @transient lazy val reusableWordId = new IntWritable()
      var wordCountsOpt: Option[AtomicLongArray] = None

      def getWordCounts(c: Computation[E])
        : AtomicLongArray = wordCountsOpt match {
        case Some(wordCounts) =>
          wordCounts

        case None =>
          val wordCounts = c.
            readAggregation[IntWritable,LongWritable]("previous_enumeration").
            asAtomicArray(
              c.getConfig().getMainGraph[BasicMainGraph].getNumberEdges())
          wordCountsOpt = Option(wordCounts)
          wordCounts
      }

      def apply(e: E, w: Int, c: Computation[E]): Boolean = {
        val res = e.isCanonicalEmbeddingWithWord(w) && {
          val wordCount = getWordCounts(c).get(w)
          val res = wordCount > 0
          res
        }
        res
      }
    }
  }

  private def aggregationRegister(cc: ComputationContainer[E])
    : (Computation[E]) => Unit = {

    val name = "previous_enumeration"
    val aggStorageClass =
      classOf[AggregationStorage[IntWritable,LongWritable]]
    val keyClass = classOf[IntWritable]
    val valueClass = classOf[LongWritable]
    val persistent = false
    val reductionFunc = new LongSumReduction()
    val endAggregationFunc =
      null.asInstanceOf[EndAggregationFunction[IntWritable,LongWritable]]

    // get the old init aggregations function in order to compose it
    val oldInitAggregation = cc.initAggregationsOpt match {
      case Some(initAggregations) => initAggregations
      case None => (c: Computation[E]) => {}
    }

    // construct an incremental init aggregations function
    (c: Computation[E]) => {
      oldInitAggregation (c) // init aggregations so far
      c.getConfig().registerAggregation [IntWritable,LongWritable] (name,
        aggStorageClass, keyClass, valueClass, persistent,
        reductionFunc, endAggregationFunc)
    }
  }
}
