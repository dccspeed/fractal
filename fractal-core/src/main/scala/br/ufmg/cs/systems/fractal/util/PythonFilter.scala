package br.ufmg.cs.systems.fractal.util

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.subgraph.{SerializableSubgraph, Subgraph}

class PythonFilter[S <: Subgraph](val filterstr: String) extends Function2[S,
   Computation[S],Boolean] with Serializable {
   @transient lazy val pythonFilterRunner: PythonFilterRunner = new
         PythonFilterRunner(filterstr)
   override def apply(s: S,
                      c: Computation[S]): Boolean = {
      pythonFilterRunner.test(SerializableSubgraph
        .fromInternalSubgraph(s, c.getPattern).asString())
   }
}

