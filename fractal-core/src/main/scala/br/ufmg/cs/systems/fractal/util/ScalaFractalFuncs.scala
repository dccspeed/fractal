package br.ufmg.cs.systems.fractal.util

import br.ufmg.cs.systems.fractal.computation._
import br.ufmg.cs.systems.fractal.graph._
import br.ufmg.cs.systems.fractal.subgraph._

import java.util.function.Predicate

trait ProcessComputeFunc [S <: Subgraph]
    extends Function2[SubgraphEnumerator[S], Computation[S], Long]
    with Serializable

trait VertexFilterFunc [V] extends Predicate[Vertex[V]] with Serializable

trait EdgeFilterFunc [E] extends Predicate[Edge[E]] with Serializable
