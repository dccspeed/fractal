package br.ufmg.cs.systems.fractal.gmlib.queryspecialization

import br.ufmg.cs.systems.fractal.computation.{Computation, PComputationContainer}
import br.ufmg.cs.systems.fractal.conf.SparkConfiguration
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.graph.VELabeledMainGraph
import br.ufmg.cs.systems.fractal.pattern.{JBlissPattern, Pattern, PatternExplorationPlan}
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, PatternInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.collection.{IntArrayList, ObjArrayList}
import br.ufmg.cs.systems.fractal.{FractalGraph, Fractoid}
import org.apache.spark.broadcast.Broadcast

class QuerySpecializationPO(val patternsBc: Broadcast[Array[Pattern]])
   extends BuiltInApplication[Fractoid[EdgeInducedSubgraph]] {

   def patterns: Array[Pattern] = patternsBc.value

   private lazy val (configs, patternGraphs, psubgraphs) = {
      val numPatterns = patterns.length
      val _configs = new Array[SparkConfiguration](numPatterns)
      val _patternGraphs = new Array[VELabeledMainGraph](numPatterns)
      val _psubgraphs = new Array[PatternInducedSubgraph](numPatterns)
      var i = 0
      while (i < numPatterns) {
         val config = new SparkConfiguration()
         config.setSubgraphClass(classOf[PatternInducedSubgraph])
         config.setPatternClass(classOf[JBlissPattern])
         patterns(i).init(config)
         _configs(i) = config
         i += 1
      }

      i = 0
      while (i < numPatterns) {
         val patternGraph = new VELabeledMainGraph
         patternGraph.init(patterns(i))
         _configs(i).setMainGraph(patternGraph)
         _psubgraphs(i) = _configs(i).createSubgraph(classOf[PatternInducedSubgraph])
         _patternGraphs(i) = patternGraph
         i += 1
      }

      (_configs, _patternGraphs, _psubgraphs)
   }

   private lazy val computation
   : PComputationContainer[PatternInducedSubgraph] = {
      val comp = new PComputationContainer[PatternInducedSubgraph]()
      comp
   }

   private lazy val extensionsPerLevel = {
      val numEdges = patterns.head.getNumberOfEdges
      val _extensionsPerLevel = new ObjArrayList[IntArrayList](numEdges + 1)
      for (_ <- 0 until numEdges + 1) {
         _extensionsPerLevel.add(new IntArrayList(numEdges))
      }
      _extensionsPerLevel
   }

   private def isSubpatternOf(patternWithoutPlan: Pattern): Boolean = {
      patternWithoutPlan.setVertexLabeled(true)
      patternWithoutPlan.setInduced(false)
      val patternWithPlan = PatternExplorationPlan.apply(patternWithoutPlan).get(0)
      computation.setPattern(patternWithPlan)
      var j = 0
      while (j < patterns.length) {
         val pattern = patterns(j)
         val config = configs(j)
         patternWithPlan.init(config)
         val patternGraph = patternGraphs(j)
         val subgraph = psubgraphs(j)
         subgraph.reset()
         val totalNumVertices = pattern.getNumberOfVertices
         val extensions = extensionsPerLevel.get(0)
         extensions.clear()
         subgraph.computeFirstLevelExtensions(patternWithPlan, totalNumVertices, 1, 0, patternGraph,
            extensions)
         var i = 0
         val numExtensions = extensions.size()
         while (i < numExtensions) {
            subgraph.addWord(extensions.getu(i))
            if (findRec(subgraph, patternWithPlan, pattern)) return true
            subgraph.removeLastWord()
            i += 1
         }
         j += 1
      }

      false
   }

   private def findRec(subgraph: PatternInducedSubgraph,
                       patternWithPlan: Pattern, pattern: Pattern)
   : Boolean = {
      val numVertices = subgraph.getNumVertices
      if (numVertices == patternWithPlan.getNumberOfVertices) return true

      val extensions = extensionsPerLevel.get(numVertices)
      extensions.clear()
      subgraph.computeExtensions(computation, extensions)
      val numExtensions = extensions.size()
      var i = 0
      while (i < numExtensions) {
         subgraph.addWord(extensions.getu(i))
         if (findRec(subgraph, patternWithPlan, pattern)) return true
         subgraph.removeLastWord()
         i += 1
      }

      false
   }

   override def apply(fg: FractalGraph): Fractoid[EdgeInducedSubgraph] = {
      val numEdges = patterns.head.getNumberOfEdges
      fg.efractoid
         .expand(1)
         .filter((s,c) => isSubpatternOf(s.quickPattern()))
         .explore(numEdges - 1)
   }
}
