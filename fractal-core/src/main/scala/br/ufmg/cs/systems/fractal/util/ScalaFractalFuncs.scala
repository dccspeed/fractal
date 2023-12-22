package br.ufmg.cs.systems.fractal.util

import java.util.function.Predicate

import br.ufmg.cs.systems.fractal.callback.SubgraphCallback
import br.ufmg.cs.systems.fractal.computation._
import br.ufmg.cs.systems.fractal.graph._
import br.ufmg.cs.systems.fractal.subgraph._

trait ProcessComputeFunc [S <: Subgraph]
    extends Function2[SubgraphEnumerator[S], Computation[S], Long]
    with Serializable

object ScalaFractalFuncs {
   type CustomSubgraphCallback[S <: Subgraph] =
      (S, Computation[S], SubgraphCallback[S]) => Unit
}

