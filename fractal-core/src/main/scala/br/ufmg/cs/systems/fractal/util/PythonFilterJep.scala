package br.ufmg.cs.systems.fractal.util

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.subgraph.{SerializableSubgraph, Subgraph}
import jep.{MainInterpreter, SharedInterpreter}
import org.apache.hadoop.shaded.org.jline.utils.InputStreamReader

import java.io.BufferedReader

class PythonFilterJep[S <: Subgraph](val filterstr: String) extends Function2[S,
   Computation[S],Boolean] with Serializable {

   val jepSharedLibPath = {
      val scriptpath = s"${System.getenv("FRACTAL_HOME")}/fractal-core/src/main/python/findjeppath.py"
      val p = Runtime.getRuntime().exec(s"python ${scriptpath}")
      val in = new BufferedReader(new InputStreamReader(p.getInputStream))
      val path = in.readLine()
      Logging.logWarn(path)
      path
   }

   @transient lazy val pythonInterpreter = {
      MainInterpreter.setJepLibraryPath(jepSharedLibPath)
      val interp = new SharedInterpreter()
      val code =
         s"""
           |import dill
           |import codecs
           |import sys
           |#from fractal import Subgraph
           |
           |# deserialize filter (assume hex string)
           |filterstr = \"${filterstr}\"
           |filter = dill.loads(codecs.decode(filterstr.encode(), "hex"))
           |f = open('/tmp/error.txt', 'w')
           |sys.stderr = f
           |print("filter function:", filter, file=f)
           |
           |class Subgraph:
           |    def __init__(self, sstr):
           |        toks = iter(sstr.split(","))
           |        self.num_vertices = int(next(toks))
           |        self.num_edges = int(next(toks))
           |        self.vids = []
           |        self.eids = []
           |        self.pedges = []
           |        self.pvlabels = []
           |        self.pelabels = []
           |        for i in range(self.num_vertices):
           |            self.vids.append(int(next(toks)))
           |        for i in range(self.num_edges):
           |            self.eids.append(int(next(toks)))
           |        for i in range(self.num_edges):
           |            src = int(next(toks))
           |            dst = int(next(toks))
           |            self.pedges.append((src,dst))
           |        for i in range(self.num_vertices):
           |            self.pvlabels.append(int(next(toks)))
           |        for i in range(self.num_edges):
           |            self.pelabels.append(int(next(toks)))
           |
           |    def __str__(self):
           |        return "Subgraph(num_vertices=%d, num_edges=%d, vids=%s, eids=%s, " \\
           |               "pedges=%s, pvlabels=%s, pelabels=%s)" % (
           |            self.num_vertices, self.num_edges, self.vids, self.eids,
           |            self.pedges, self.pvlabels, self.pelabels)
           |
           """.stripMargin
      interp.exec(code)
      interp
   }

   override def apply(s: S,
                      c: Computation[S]): Boolean = {
      val subgraphstr = SerializableSubgraph.fromInternalSubgraph(s).asString()
      pythonInterpreter.exec(s"subgraph = Subgraph(\"${subgraphstr}\")")
      val valid = pythonInterpreter.getValue("filter(subgraph)")
      //Logging.logError(valid.toString)
      valid == true
   }
}

