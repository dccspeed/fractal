package io.arabesque.computation

import io.arabesque.aggregation.{AggregationStorage, AggregationStorageMetadata}
import io.arabesque.cache.LZ4ObjectCache
import io.arabesque.conf.SparkConfiguration
import io.arabesque.embedding._
import io.arabesque.utils.SerializableConfiguration

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Writable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.util.{LongAccumulator, SizeEstimator}

import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

/**
 * Underlying engine that runs the Arabesque master.
 * It interacts directly with the RDD interface in Spark by handling the
 * SparkContext.
 */
class SparkEmbeddingMasterEngine[E <: Embedding](
      _config: SparkConfiguration[E],
      _parentOpt: Option[SparkMasterEngine[E]])
  extends SparkMasterEngine [E] {

  def config: SparkConfiguration[E] = _config
  
  config.initialize()

  def parentOpt: Option[SparkMasterEngine[E]] = _parentOpt
  
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

  /**
   * Master's computation takes place here, superstep by superstep
   */
  lazy val next: Boolean = {

    val superstepStart = System.currentTimeMillis

    logInfo (s"SparkConfiguration estimated size = " +
      s"${SizeEstimator.estimate(config)} bytes")
    logInfo (s"HadoopConfiguration estimated size = " +
      s"${SizeEstimator.estimate(config.hadoopConf)} bytes")

    logInfo (s"Computation starting from ${superstepRDD}," +
      s", StorageLevel=${superstepRDD.getStorageLevel}")

    val _aggAccums = aggAccums

    val execEngines = getExecutionEngines (
      superstepRDD = superstepRDD,
      superstep = superstep,
      configBc = configBc,
      aggAccums = _aggAccums,
      previousAggregationsBc = previousAggregationsBc)

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

        previousAggregationsBc = sc.broadcast (aggregations)

      case Failure(e) =>
        logError (s"Error in collecting aggregations: ${e.getMessage}")
        throw e
    }

    logInfo (s"StorageLevel = ${storageLevel}")

    /** [2] We shuffle the embeddings and prepare to the next superstep
     */
    superstepRDD = execEngines.
      flatMap (_.flush).asInstanceOf[RDD[(Int,LZ4ObjectCache)]].
      partitionBy (new HashPartitioner (numPartitions)).
      values.persist(storageLevel)

    superstepRDD.foreachPartition (_ => {})
    
    // whether the user chose to customize master computation, executed every
    // superstep
    masterComputation.compute()
    
    // print stats
    aggAccums = aggAccums.map { case (name,accum) =>
      logInfo (s"Accumulator[${superstep}][${name}]: ${accum.value}")
      (name -> sc.longAccumulator (name))
    }
    
    val superstepFinish = System.currentTimeMillis
    logInfo (
      s"Superstep $superstep finished in ${superstepFinish - superstepStart} ms"
    )
    
    !sc.isStopped && !superstepRDD.isEmpty
  }

  /**
   * Creates an RDD of execution engines 
   * TODO
   */
  private def getExecutionEngines[E <: Embedding](
      superstepRDD: RDD[LZ4ObjectCache],
      superstep: Int,
      configBc: Broadcast[SparkConfiguration[E]],
      aggAccums: Map[String,LongAccumulator],
      previousAggregationsBc: Broadcast[_]): RDD[SparkEngine[E]] = {

    // read embeddings from embedding caches, expand, filter and process
    val execEngines = superstepRDD.mapPartitionsWithIndex { (idx, cacheIter) =>

      configBc.value.initialize()

      val execEngine = new SparkEmbeddingEngine [E] (
        partitionId = idx,
        superstep = superstep,
        accums = aggAccums,
        previousAggregationsBc = previousAggregationsBc,
        configurationId = configBc.value.getId
      )

      execEngine.init()
      execEngine.compute (cacheIter)
      execEngine.finalize()
      Iterator[SparkEngine[E]](execEngine)
    }

    execEngines
  }
}
