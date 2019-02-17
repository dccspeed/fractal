package br.ufmg.cs.systems.fractal.util

import br.ufmg.cs.systems.fractal.computation._
import br.ufmg.cs.systems.fractal.subgraph._

/**
 * This is a set of aliases for extending fractal functions using the extended
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
      VertexInducedSubgraph, Computation[VertexInducedSubgraph], Unit
    ] with Serializable

trait EdgeProcessFunc
    extends Function2[
      EdgeInducedSubgraph, Computation[EdgeInducedSubgraph], Unit
    ]
    with Serializable

trait WordFilterFunc [E <: Subgraph] extends Serializable {
  def apply(t1: E, t2: Int, t3: Computation[E]): Boolean
}

trait ProcessComputeFunc [E <: Subgraph]
    extends Function2[java.util.Iterator[E], Computation[E], Long]
    with Serializable

trait MasterComputeFunc
    extends Function1[MasterComputation, Unit]
    with Serializable 
