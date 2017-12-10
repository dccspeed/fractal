package io.arabesque

import io.arabesque.computation._
import io.arabesque.embedding._

/**
 * This is a set of aliases for extending arabesque functions using the extended
 * syntax. The most typical use for this kind of pattern is when the function
 * requires a reusable local variable in order to avoid unnecessary object
 * creation.
 *
 */

trait SpecializedFunction3[
    @specialized(Int, Long, Double) -T1,
    @specialized(Int, Long, Double) -T2,
    @specialized(Int, Long, Double) -T3,
    @specialized(Int, Long, Double) +R] {
  def apply(t1: T1, t2: T2, t3: T3): R
}

trait VertexProcessFunc
    extends Function2[
      VertexInducedEmbedding, Computation[VertexInducedEmbedding], Unit
    ] with Serializable

trait EdgeProcessFunc
    extends Function2[
      EdgeInducedEmbedding, Computation[EdgeInducedEmbedding], Unit
    ]
    with Serializable

trait WordFilterFunc [E <: Embedding] extends Serializable {
  def apply(t1: E, t2: Int, t3: Computation[E]): Boolean
}

trait ProcessComputeFunc [E <: Embedding]
    extends Function2[java.util.Iterator[E], Computation[E], Long]
    with Serializable

trait MasterComputeFunc
    extends Function1[MasterComputation, Unit]
    with Serializable 
