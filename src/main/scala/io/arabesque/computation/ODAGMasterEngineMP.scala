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
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
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
class ODAGMasterEngineMP [E <: Embedding] (
      _config: SparkConfiguration[E],
      _parentOpt: Option[SparkMasterEngine[E]])
    (implicit val oTag: ClassTag[MultiPatternODAG], implicit val cTag: ClassTag[ODAGEngineMP[E]])
    extends ODAGMasterEngine [Int,E,MultiPatternODAG,MultiPatternODAGStash,ODAGEngineMP[E]] {
  
  def config: SparkConfiguration[E] = _config
  
  def parentOpt: Option[SparkMasterEngine[E]] = _parentOpt

  def this(_sc: SparkContext, config: SparkConfiguration[E]) {
    this (config, None)
    sc = _sc
    init()
  }
  
  def this(_sc: SparkContext, config: SparkConfiguration[E], parent: SparkMasterEngine[E]) {
    this (config, Option(parent))
    sc = _sc
    init()
  }

  def this(confs: Map[String,Any]) {
    this (new SparkConfiguration [E] (confs), None)

    sc = new SparkContext(config.sparkConf)
    val logLevel = config.getString ("log_level", "INFO").toUpperCase
    sc.setLogLevel (logLevel)

    init()
  }
  
  def this(confs: Map[String,Any], parent: SparkMasterEngine[E]) {
    this (new SparkConfiguration [E] (confs), Option(parent))

    sc = new SparkContext(config.sparkConf)
    val logLevel = config.getString ("log_level", "INFO").toUpperCase
    sc.setLogLevel (logLevel)

    init()
  }

  def getAggregatedOdags(
      execEngines: RDD[ODAGEngine[E,MultiPatternODAG,MultiPatternODAGStash,ODAGEngineMP[E]]],
      previousAggregationsBc: Broadcast[_], configBc: Broadcast[SparkConfiguration[E]]) = {

    // we choose the flush method for ODAGs: load-balancing vs. overhead
    val aggregatedOdags = config.getOdagFlushMethod match {
      case SparkConfiguration.FLUSH_BY_PATTERN =>
        val odags = execEngines.
          map (_.withNewAggregations (previousAggregationsBc)). // update previousAggregations
          flatMap (_.flush).
          asInstanceOf[RDD[(Int,MultiPatternODAG)]]
        aggregatedOdagsByPattern (odags)
      
      case SparkConfiguration.FLUSH_BY_ENTRIES =>
        sc.makeRDD (Seq.empty[(Int,MultiPatternODAG)])

      case SparkConfiguration.FLUSH_BY_PARTS =>
        sc.makeRDD (Seq.empty[(Int,MultiPatternODAG)])

    }

    aggregatedOdags
  }

  private def aggregatedOdagsByPattern(odags: RDD[(Int,MultiPatternODAG)]) = {

    // (flushByPattern)
    val aggregatedOdags = odags.reduceByKey { (odag1, odag2) =>
      odag1.aggregate (odag2)
      odag1
    }.
    map { case (key, odag) =>
      odag.setSerializeAsReadOnly (true)
      (key, odag)
    }

    aggregatedOdags
  }

}
