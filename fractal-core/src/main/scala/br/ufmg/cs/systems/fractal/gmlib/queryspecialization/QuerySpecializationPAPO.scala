package br.ufmg.cs.systems.fractal.gmlib.queryspecialization

import br.ufmg.cs.systems.fractal.computation.{AllEdgesSubgraphEnumerator, Computation, PComputationContainer}
import br.ufmg.cs.systems.fractal.conf.SparkConfiguration
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.graph.{SmallVELabeledMainGraph, UnlabeledMainGraph, VELabeledMainGraph}
import br.ufmg.cs.systems.fractal.pattern.{JBlissPattern, Pattern, PatternExplorationPlan, PatternExplorationPlanOrderingHeuristic}
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, PatternInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.VertexPredicate
import br.ufmg.cs.systems.fractal.util.collection.{IntArrayList, ObjArrayList}
import br.ufmg.cs.systems.fractal.{FractalGraph, Fractoid}
import com.koloboke.collect.map.hash.HashIntIntMaps
import org.apache.spark.broadcast.Broadcast

class QuerySpecializationPAPO(_pattern: Pattern)
   extends BuiltInApplication[Fractoid[EdgeInducedSubgraph]] {

   private val pattern: Pattern = {
      val p = _pattern.copy()
      p.setExplorationPlan(_pattern.explorationPlan())
      p
   }

   private lazy val (config, patternGraph, psubgraph) = {
      val _config = new SparkConfiguration()
      _config.setSubgraphClass(classOf[PatternInducedSubgraph])
      _config.setPatternClass(classOf[JBlissPattern])
      pattern.init(_config)
      val _patternGraph = new SmallVELabeledMainGraph
      _config.setMainGraph(_patternGraph)
      val _psubgraph = _config.createSubgraph(classOf[PatternInducedSubgraph])
      (_config, _patternGraph, _psubgraph)
   }

   private lazy val computation
   : PComputationContainer[PatternInducedSubgraph] = {
      val comp = new PComputationContainer[PatternInducedSubgraph]()
      comp.setPattern(pattern)
      comp
   }

   private lazy val extensionsPerLevel = {
      val numVertices = pattern.getNumberOfVertices
      val _extensionsPerLevel = new ObjArrayList[IntArrayList](numVertices)
      for (_ <- 0 until numVertices) {
         _extensionsPerLevel.add(new IntArrayList(numVertices))
      }
      _extensionsPerLevel
   }

   private var vpred: VertexPredicate = _

   private var previousMatch: IntArrayList = _

   private def isSubgraphValid(s: EdgeInducedSubgraph, c: Computation[EdgeInducedSubgraph])
   : Boolean = {
      val vertices = s.getVertices
      val numVertices = vertices.size()

      // init graph with subgraph
      patternGraph.init(s)
      psubgraph.reset()
      pattern.init(config)
      val extensions = extensionsPerLevel.getu(0)
      extensions.clear()

      if (vpred == null) {
         vpred = pattern.explorationPlan().vertexPredicate(0)
      }

      if (previousMatch == null) {
         previousMatch = c.getExecutionEngine.getPreviousEngine.getComputation
            .getSubgraphEnumerator.getSubgraph.getVertices
      }

      var i = 0
      while (i < numVertices) {
         val u = vertices.getu(i)
         if (vpred.test(u)) {
            extensions.add(u)
         }
         i += 1
      }

      i = 0
      extensions.sort()
      val numExtensions = extensions.size()
      while (i < numExtensions) {
         psubgraph.addWord(extensions.getu(i))
         if (findRec(psubgraph)) {
            return psubgraph.getVertices.equals(previousMatch)
         }
         psubgraph.removeLastWord()
         i += 1
      }

      false
   }

   private def findRec(subgraph: PatternInducedSubgraph): Boolean = {
      val numVertices = subgraph.getNumVertices
      if (numVertices == pattern.getNumberOfVertices) {
         return true
      }

      val extensions = extensionsPerLevel.get(numVertices)
      extensions.clear()
      subgraph.computeExtensions(computation, extensions)
      extensions.sort()
      val numExtensions = extensions.size()
      var i = 0
      while (i < numExtensions) {
         subgraph.addWord(extensions.getu(i))
         if (findRec(subgraph)) return true
         subgraph.removeLastWord()
         i += 1
      }

      false
   }

   override def apply(fg: FractalGraph): Fractoid[EdgeInducedSubgraph] = {
      val fractoid = fg.pfractoid(_pattern)
         .expand(_pattern.getNumberOfVertices)
         .efractoid
         .expand(1, classOf[AllEdgesSubgraphEnumerator])
         .filter(isSubgraphValid _)

      fractoid
   }
}
