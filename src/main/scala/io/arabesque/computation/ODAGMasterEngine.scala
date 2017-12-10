package io.arabesque.computation

import java.io.{ByteArrayInputStream, DataInputStream}

import io.arabesque.aggregation.{AggregationStorage, AggregationStorageMetadata}
import io.arabesque.conf.SparkConfiguration
import io.arabesque.embedding._
import io.arabesque.odag._
import io.arabesque.pattern.Pattern
import io.arabesque.utils.SerializableConfiguration

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Writable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.util.{LongAccumulator, SizeEstimator}

import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Success}

/**
 * Underlying engine that runs the Arabesque master.
 * It interacts directly with the RDD interface in Spark by handling the
 * SparkContext.
 */
trait ODAGMasterEngine [
    K,
    E <: Embedding,
    O <: BasicODAG,
    S <: BasicODAGStash[O,S],
    C <: ODAGEngine[E,O,S,C]
  ] extends SparkMasterEngine [E] {

  // workaround to pass classtags to traits
  implicit def oTag: ClassTag[O]
  implicit def cTag: ClassTag[C]

  def config: SparkConfiguration[E]
  
  config.initialize()
  
  var aggregatedOdagsBc: Broadcast[scala.collection.Map[K,O]] = _

  override def init(): Unit = {
    parentOpt match {
      case Some(parent) =>
        aggregatedOdagsBc = parent.
          asInstanceOf[ODAGMasterEngine[K,_,O,_,_]].aggregatedOdagsBc
        
      case None =>
        aggregatedOdagsBc = sc.broadcast (Map.empty)
    }

    super.init()
  }

  def getAggregatedOdags(execEngines: RDD[ODAGEngine[E,O,S,C]],
      previousAggregationsBc: Broadcast[_],
      configBc: Broadcast[SparkConfiguration[E]])
    : RDD[(K,O)]

  /**
   * Master's computation takes place here, superstep by superstep
   */
  lazy val next: Boolean = {
    val superstepStart = System.currentTimeMillis

    logInfo (s"SparkConfiguration estimated size =" +
      s" ${SizeEstimator.estimate(config)} bytes")
    logInfo (s"HadoopConfiguration estimated size =" +
      s" ${SizeEstimator.estimate(config.hadoopConf)} bytes")

    val execEngines = getExecutionEngines (
      superstepRDD = superstepRDD,
      superstep = superstep,
      configBc = configBc,
      aggregatedOdagsBc = aggregatedOdagsBc,
      aggAccums = aggAccums,
      previousAggregationsBc = previousAggregationsBc)

    // keep engines (filled with expansions and aggregations) for the rest of
    // the superstep
    execEngines.persist (MEMORY_ONLY)

    // Materialize execEngines
    execEngines.foreachPartition (_ => {})

    /** [1] We extract and aggregate the *aggregations* globally.
     *  That gives us the opportunity to do aggregationFilter in the generated
     *  ODAGs before collecting/broadcasting */

    // create futures (two jobs submitted roughly simultaneously)
    val aggregationsFuture = getAggregations (execEngines, numPartitions)
    // aggregations
    Await.ready (aggregationsFuture, atMost = Duration.Inf)
    aggregationsFuture.value.get match {
      case Success(previousAggregations) =>

        aggregations = mergeOrReplaceAggregations (aggregations,
          previousAggregations)

        logInfo (s"""Aggregations and sizes
          ${previousAggregations.
          map(tup => (tup._1,tup._2)).mkString("\n")}
        """)

        previousAggregationsBc = sc.broadcast (previousAggregations)

      case Failure(e) =>
        logError (s"Error in collecting aggregations: ${e.getMessage}")
        throw e
    }

    /** [2] At this point we have updated the *previousAggregations*. Now we
     *  can: (i) aggregationFilter the ODAGs residing in the execution
     *  engines, if this applies; and (ii) flush the remaining ODAGs for
     *  global aggregation.
     */

    val aggregatedOdags = getAggregatedOdags(
      execEngines.asInstanceOf[RDD[ODAGEngine[E,O,S,C]]],
      previousAggregationsBc, configBc)

    val odagsFuture = Future { aggregatedOdags.collect.toMap  }
    // odags
    Await.ready (odagsFuture, atMost = Duration.Inf)
    odagsFuture.value.get match {
      case Success(aggregatedOdagsLocal) =>
        logInfo (s"Number of aggregated ODAGs = ${aggregatedOdagsLocal.size}")
        aggregatedOdagsBc.unpersist()
        aggregatedOdagsBc = sc.broadcast (aggregatedOdagsLocal)
        logInfo (s"New odags broadcasted (id=${aggregatedOdagsBc.id})")

        /* maybe debug odag stats */
        if (log.isDebugEnabled) {
          for ((pattern,odag) <- aggregatedOdagsLocal.iterator) {
            val storage = odag.getStorage
            storage.finalizeConstruction
            logDebug (
              s"Superstep{${superstep}}" +
              s";Patterns{1}" +
              s";StorageEstimate{${SizeEstimator.estimate (odag.getStorage)}}" +
              s";PatternEstimate{${SizeEstimator.estimate (pattern.asInstanceOf[AnyRef])}}" +
              s";${storage.toStringResume}" +
              s";${storage.getStats}" +
              s";${storage.getStats.getSizeEstimations}"
            )
          }
        }

      case Failure(e) =>
        logError (s"Error in collecting odags ${e.getMessage}")
        throw e
    }

    // the exec engines have no use anymore, make room for the next round
    execEngines.unpersist()

    // whether the user chose to customize master computation, executed every
    // superstep
    masterComputation.compute()

    // print stats
    aggAccums = aggAccums.map { case (name,accum) =>
      logInfo (s"Accumulator[${superstep}][${name}]: ${accum.value}")
      (name -> sc.longAccumulator (name))
    }
    
    val superstepFinish = System.currentTimeMillis
    logInfo (s"Superstep $superstep finished in ${superstepFinish - superstepStart} ms")

    !sc.isStopped && !aggregatedOdagsBc.value.isEmpty
  }

  /**
   * Creates an RDD of execution engines
   * TODO
   */
  def getExecutionEngines(
      superstepRDD: RDD[_],
      superstep: Int,
      configBc: Broadcast[SparkConfiguration[E]],
      aggregatedOdagsBc: Broadcast[scala.collection.Map[K,O]],
      aggAccums: Map[String,LongAccumulator],
      previousAggregationsBc: Broadcast[_]): RDD[SparkEngine[E]] = {

    // read embeddings from global agg. ODAGs, expand, filter and process
    val execEngines = superstepRDD.mapPartitionsWithIndex { (idx, _) =>

      configBc.value.initialize()

      val execEngine = ODAGEngine [E,O,S,C] (
        partitionId = idx,
        superstep = superstep,
        accums = aggAccums,
        previousAggregationsBc = previousAggregationsBc,
        configuration = configBc.value
      )
      execEngine.init()
      val stash = ODAGStash [E,O,S] (configBc.value, aggregatedOdagsBc.value)
      execEngine.compute (Iterator (stash))
      execEngine.finalize()
      Iterator[SparkEngine[E]](execEngine)
    }

    execEngines
  }
}
