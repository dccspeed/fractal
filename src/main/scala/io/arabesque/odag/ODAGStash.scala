package io.arabesque.odag

import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding.Embedding
import io.arabesque.pattern.Pattern

import scala.collection.JavaConverters._

object ODAGStash {
  import Configuration._
  import SparkConfiguration._

  def apply [E <: Embedding, O <: BasicODAG, S <: BasicODAGStash[O,S]]
    (config: Configuration[E],
      aggregatedOdags: scala.collection.Map[_,O]): S =
      config.getString(CONF_COMM_STRATEGY, CONF_COMM_STRATEGY_DEFAULT) match {

    case COMM_ODAG_SP =>
      new SinglePatternODAGStash(aggregatedOdags.asJava.asInstanceOf[java.util.Map[Pattern,SinglePatternODAG]]).
        init(config).asInstanceOf[S]

    case COMM_ODAG_MP =>
      new MultiPatternODAGStash(aggregatedOdags.asInstanceOf[Map[Int,MultiPatternODAG]]).
        init(config).asInstanceOf[S]
  }
}
