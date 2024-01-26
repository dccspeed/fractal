package br.ufmg.cs.systems.fractal.util

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.subgraph.{SerializableSubgraph, Subgraph}
import jep.Interpreter
import jep.SharedInterpreter

class PythonFilter[S <: Subgraph](val filterstr: String) extends Function2[S,
   Computation[S],Boolean] with Serializable {
   @transient lazy val pythonFilterRunner: PythonFilterRunner = new
         PythonFilterRunner(filterstr)
   override def apply(s: S,
                      c: Computation[S]): Boolean = {
      try {
         val interp = new SharedInterpreter
         try {
            interp.exec("from java.lang import System")
            interp.exec("s = 'Hello World'")
            interp.exec("System.out.println(s)")
            interp.exec("print(s)")
            interp.exec("print(s[1:-1])")
         } finally if (interp != null) interp.close()

         pythonFilterRunner.test(SerializableSubgraph
            .fromInternalSubgraph(s).asString())
      }
   }
}

