package io.arabesque.computation

import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding._

class EncoderMasterEngine [E <: Embedding, PE <: Embedding] (_encParent: SparkMasterEngine[PE])
    extends SparkEmbeddingMasterEngine[E](new SparkConfiguration [E] (_encParent.config.confs), None) {

  def encParent: SparkMasterEngine[PE] = _encParent
  
  override lazy val superstep: Int = encParent.superstep

  override def init(): Unit = {
    // master must know aggregators metadata
    val computation = config.createComputation [E]
    superstepRDD = encParent.superstepRDD
    aggAccums = encParent.aggAccums
    previousAggregationsBc = encParent.previousAggregationsBc
    aggregations = encParent.aggregations
  }

  override lazy val next: Boolean = {
    init()
    true
  }

}
