package io.arabesque.odag

import java.util.concurrent.ExecutorService
import java.io._

import io.arabesque.computation.Computation
import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding.Embedding
import io.arabesque.pattern.Pattern

import scala.collection.JavaConverters._

class MultiPatternODAGStash
    extends BasicODAGStash[MultiPatternODAG,MultiPatternODAGStash]
    with Serializable {

  @transient var _configuration: Configuration[_ <: Embedding] = _

  var configId: Int = -1

  def configuration: Configuration[_ <: Embedding] = {
    if (_configuration == null) {
      _configuration = Configuration.get(configId)
    }
    _configuration
  }

  var odags: Array[MultiPatternODAG] = _

  var numOdags: Int = _

  @transient lazy val reusablePattern: Pattern = configuration.createPattern

  def this(maxOdags: Int) = {
    this()
    odags = new Array(maxOdags)
    numOdags = 0
  }

  def this(odagMap: scala.collection.Map[_,MultiPatternODAG]) = {
    this()
    odags = odagMap.values.toArray
    numOdags = odags.size
  }

  override def init(config: Configuration[_ <: Embedding])
    : MultiPatternODAGStash = {
    _configuration = config
    this
  }

  def aggregationFilter(computation: Computation[_]): Unit = {
    for (i <- 0 until odags.size if odags(i) != null)
      if (!odags(i).aggregationFilter(computation)) {
        odags(i) = null
        numOdags -= 1
      }
  }

  override def addEmbedding(embedding: Embedding): Unit = {
    reusablePattern.setEmbedding (embedding)
    val idx = {
      val cand = reusablePattern.hashCode % getNumZips
      cand + (if (cand < 0) getNumZips else 0)
    }

    if (odags(idx) == null) {
      // initialize multi-pattern odag
      odags(idx) = new MultiPatternODAG (embedding.getNumWords).
        init(configuration)
      numOdags += 1
    }

    // add embedding to the selected odag
    odags(idx).addEmbedding (embedding, reusablePattern)
  }

  override def aggregate(odag: MultiPatternODAG): Unit = ???

  override def aggregateUsingReusable(odag: MultiPatternODAG): Unit = ???

  override def aggregateStash(other: MultiPatternODAGStash): Unit = {
  }
   
  override def finalizeConstruction(
      pool: ExecutorService, parts: Int): Unit = {
    assert (odags.size <= configuration.getMaxOdags)
    for (odag <- odags.iterator if odag != null)
      odag.finalizeConstruction (pool, parts)
  }

  override def isEmpty(): Boolean = odags.isEmpty

  override def getNumZips(): Int =
    if (odags != null) odags.size else 0

  override def getEzips() =
    odags.filter(_ != null).toIterable.asJavaCollection

  override def clear(): Unit = {
    for (i <- 0 until odags.size) odags(i) = null
  }

  /**
   * TODO: not supporting hadoop writables for now
   */
  override def readFields(dataInput: DataInput): Unit = ???
  override def write(dataOutput: DataOutput): Unit = ???
  
}
