package br.ufmg.cs.systems.fractal.gmlib

import br.ufmg.cs.systems.fractal.computation.{Computation, RandomWalkEnumerator, SamplingEnumerator}
import br.ufmg.cs.systems.fractal.gmlib.clique.{KClistEnumerator, MaximalCliquesEnumerator}
import br.ufmg.cs.systems.fractal.gmlib.fsm.{FSMHybrid, FSMPF, FSMPFMCVC, FSMSF, MinImageSupport}
import br.ufmg.cs.systems.fractal.gmlib.kws.KeywordSearchPO
import br.ufmg.cs.systems.fractal.gmlib.motifs.{MotifsHybrid, MotifsPF, MotifsPFMCVC, MotifsSF}
import br.ufmg.cs.systems.fractal.gmlib.periodic.{InducedPeriodicSubgraphsPF, InducedPeriodicSubgraphsPFMCVC, InducedPeriodicSubgraphsSF}
import br.ufmg.cs.systems.fractal.gmlib.quasicliques.QuasiCliquesSF
import br.ufmg.cs.systems.fractal.graph.MainGraph
import br.ufmg.cs.systems.fractal.pattern._
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, PatternInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.collection.ObjSet
import br.ufmg.cs.systems.fractal.util.pool.{IntArrayListPool, IntArrayListViewPool}
import br.ufmg.cs.systems.fractal.util.{Logging, Utils}
import br.ufmg.cs.systems.fractal.{FSMSF, FractalGraph, Fractoid}
import org.apache.spark.rdd.RDD

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
 * Built-in algorithms with the following alternative implementations:
 *
 * SF: Subgraph-first paradigm
 * PF: Pattern-first paradigm
 * PFMCVC: Pattern-first paradigm optimized by matching the minimum connected
 * vertex cover of the pattern first and then, finishing the matching in one
 * step because the cover gives access to all remaining neighborhoods
 * necessary for the whole pattern matching
 * @param self fractal graph
 */
class BuiltInApplications(self: FractalGraph) extends Logging {

   /**
    * Generated all subgraphs using the PF approach
    * @param numEdges number of edges in the subgraphs
    * @param callback to be applied to each partial result of this computation
    */
   def subgraphsMaxEdgesPF
   (numEdges: Int, callback: Fractoid[PatternInducedSubgraph] => Unit)
   : Unit = {

      var futures = List[Future[Unit]]()

      val canonicalPatternsRDD = PatternUtilsRDD.edgePatternsRDD(
         self.fractalContext.sparkContext, numEdges)

      // local iterator for better memory footprint
      val iter = PatternUtilsRDD.localIterator(canonicalPatternsRDD)
      while (iter.hasNext) {
         val patternWithoutPlan = iter.next()
         // ignore label
         patternWithoutPlan.setVertexLabeled(false)

         // naive exploration plan
         val pattern = PatternExplorationPlan
            .apply(patternWithoutPlan).get(0)
         pattern.updateSymmetryBreakerVertexUnlabeled()

         // callback
         val partialResult = patternMatchingPF(pattern)
            .explore(pattern.getNumberOfVertices - 1)
         val future = Future(callback(partialResult))
         futures = future :: futures
      }

      Await.result(Future.sequence(futures), Duration.Inf)
   }

   /**
    * Generated all subgraphs using the PF approach
    * @param numVertices number of vertices in the subgraphs
    * @param callback to be applied to each partial result of this computation
    */
   def subgraphsPF
   (numVertices: Int, callback: Fractoid[PatternInducedSubgraph] => Unit)
   : Unit = {

      val canonicalPatternsRDD = PatternUtilsRDD.vertexPatternsRDD(
         self.fractalContext.sparkContext, numVertices)

      // local iterator for better memory footprint
      val iter = PatternUtilsRDD.localIterator(canonicalPatternsRDD)
      while (iter.hasNext) {
         val patternWithoutPlan = iter.next()

         // ignore label
         patternWithoutPlan.setVertexLabeled(false)

         // naive exploration plan
         val pattern = PatternExplorationPlan.apply(patternWithoutPlan).get(0)
         pattern.updateSymmetryBreakerVertexUnlabeled()

         // callback
         val partialResult = patternMatchingPF(pattern).explore(numVertices - 1)
         callback(partialResult)
      }
   }

   /**
    * Generated all induced subgraphs using the PFMCVC approach
    * @param numVertices number of vertices in the induced subgraphs
    * @param callback to be applied to each partial result of this computation
    */
   def inducedSubgraphsPFMCVC
   (numVertices: Int,
    callback: Fractoid[PatternInducedSubgraph] => Unit): Unit = {

      // generate all canonical patterns with *numVertices* vertices
      val canonicalPatternsRDD = PatternUtilsRDD.vertexPatternsRDD(
         self.fractalContext.sparkContext, numVertices)

      // local iterator for better memory footprint
      val iter = PatternUtilsRDD.localIterator(canonicalPatternsRDD)
      while (iter.hasNext) {
         val patternWithoutPlan = iter.next()
         patternWithoutPlan.setInduced(true)
         patternWithoutPlan.setVertexLabeled(false)

         // update with MCVC optimized plan
         val newPatternsCur = PatternExplorationPlanMCVC
            .apply(patternWithoutPlan).cursor()

         while (newPatternsCur.moveNext()) {
            val pattern = newPatternsCur.elem()
            val mcvcSize = pattern.explorationPlan.mcvcSize()
            val partialResult = patternMatchingPF(pattern).explore(mcvcSize - 1)
            callback(partialResult)
         }
      }
   }

   /**
    * Generated all induced subgraphs using the PF approach
    * @param numVertices number of vertices in the induced subgraphs
    * @param callback to be applied to each partial result of this computation
    */
   def inducedSubgraphsPF
   (numVertices: Int, callback: Fractoid[PatternInducedSubgraph] => Unit)
   : Unit = {

      val canonicalPatternsRDD = PatternUtilsRDD.vertexPatternsRDD(
         self.fractalContext.sparkContext, numVertices)

      // local iterator for better memory footprint
      val iter = PatternUtilsRDD.localIterator(canonicalPatternsRDD)
      while (iter.hasNext) {
         val patternWithoutPlan = iter.next()

         // induced subgraphs and ignore label
         patternWithoutPlan.setInduced(true)
         patternWithoutPlan.setVertexLabeled(false)

         // naive exploration plan
         val pattern = PatternExplorationPlan.apply(patternWithoutPlan).get(0)
         pattern.updateSymmetryBreakerVertexUnlabeled()

         // callback
         val partialResult = patternMatchingPF(pattern).explore(numVertices - 1)
         callback(partialResult)
      }
   }

   /**
    * Motifs counting by listing and using the SF paradigm
    * @param numVertices motifs size
    * @return RDD representing a mapping (CanonicalPattern -> Count)
    */
   def motifsSF(numVertices: Int): RDD[(Pattern,Long)] = {
      val app = new MotifsSF(numVertices)
      app.apply(self)
   }

   /**
    * Motifs counting by listing and using the PFMCVC paradigm
    * @param numVertices motifs size
    * @return RDD representing a mapping (CanonicalPattern -> Count)
    */
   def motifsPFMCVC(numVertices: Int): RDD[(Pattern,Long)] = {
      val app = new MotifsPFMCVC(numVertices)
      app.apply(self)
   }

   /**
    * Motifs counting by listing and using the PF paradigm
    * @param numVertices motifs size
    * @return RDD representing a mapping (CanonicalPattern -> Count)
    */
   def motifsPF(numVertices: Int): RDD[(Pattern,Long)] = {
      val app = new MotifsPF(numVertices)
      app.apply(self)
   }

   /**
    * Motifs counting by listing and using hybrid paradigm
    * @param numVertices motifs size
    * @return RDD representing a mapping (CanonicalPattern -> Count)
    */
   def motifsHybrid(numVertices: Int): RDD[(Pattern,Long)] = {
      val app = new MotifsHybrid(numVertices)
      app.apply(self)
   }

   /**
    * Motifs counting by listing a sample of subgraphs uniformly at random
    * and using the SF paradigm
    * @param numVertices motifs size
    * @param fraction sample fraction
    * @return RDD representing a mapping (CanonicalPattern -> FractionCount)
    */
   def motifsSampleSF(numVertices: Int, fraction: Double)
   : RDD[(Pattern,Long)] = {
      val senumClass = classOf[SamplingEnumerator[VertexInducedSubgraph]]
      val fractionKey = "sampling_fraction"
      self.set(fractionKey, fraction)
         .vfractoid
         .expand(numVertices, senumClass)
         .aggregationCanonicalPatternLong(
            s => s.quickPattern(), 0, _ => 1L, _ + _)
   }

   /**
    * All-cliques listing using the PO (pattern oblivious) paradigm
    * @param numVertices number of vertices in the cliques
    * @return fractoid with cliques computation
    */
   def cliquesPO(numVertices: Int): Fractoid[VertexInducedSubgraph] = {
      self.vfractoid.expand(1)
         .filter((s,c) => s.numEdgesAdded == s.getNumVertices - 1)
         .explore(numVertices - 1)
   }

   /**
    * All-cliques listing using the PA (pattern-aware) paradigm
    * @param numVertices number of vertices in the cliques
    * @return fractoid with cliques computation
    */
   def cliquesPA(numVertices: Int): Fractoid[PatternInducedSubgraph] = {
      // single vertex cliques
      var pattern = PatternUtils.singleVertexPattern()
      pattern.setVertexLabeled(false)

      // explore and extend cliques
      while (pattern.getNumberOfVertices < numVertices) {
         pattern.setVertexLabeled(false)
         // extend to next clique
         val newEdges = (0 until pattern.getNumberOfVertices).toArray
         pattern = PatternUtils.addVertex(pattern, newEdges:_*)
      }

      val frac = patternMatchingPF(pattern)
         .explore(pattern.getNumberOfVertices - 1)

      frac

   }

   /**
    * All-cliques listing implementing the efficient DAG structure from
    * [[https://dl.acm.org/citation.cfm?id=3186125]]
    * @return Fractoid with the cliques computation
    */
   def cliquesCustomKClist(numVertices: Int): Fractoid[VertexInducedSubgraph] = {
      val senumClass = classOf[KClistEnumerator[VertexInducedSubgraph]]
      self.set("clique_size", numVertices)
         .vfractoidNoEdgeUpdate
         .expand(numVertices, senumClass)
   }

   /**
    * All maximal cliques up to a upper bound on the number of vertices using
    * the PO (pattern oblivious) approach
    * @param maxNumVertices upper bound on the number of vertices
    */
   def maximalCliquesPO(maxNumVertices: Int)
   : Fractoid[VertexInducedSubgraph] = {

      // function that verifies whether a subgraph (clique) is maximal
      val isMaximal = (s: VertexInducedSubgraph,
                       c: Computation[VertexInducedSubgraph]) => {
         val vertices = s.getVertices
         val graph = c.getConfig.getMainGraph
         val neighborhood = IntArrayListViewPool.instance().createObject()
         var intersectionEmpty = false

         if (vertices.size() == 1) {
            graph.neighborhoodVertices(vertices.getu(0), neighborhood)
            intersectionEmpty = neighborhood.isEmpty
         } else {

            var intersection = IntArrayListPool.instance().createObject()
            var previous = IntArrayListPool.instance().createObject()

            // first neighborhood
            val n1 = graph.neighborhoodVertices(vertices.getu(0))
            val n2 = graph.neighborhoodVertices(vertices.getu(1))
            Utils.sintersect(n1, n2, 0, n1.size(), 0, n2.size(), intersection)
            n1.reclaim()
            n2.reclaim()

            intersectionEmpty = intersection.isEmpty

            var i = 2
            while (!intersectionEmpty && i < vertices.size()) {
               val aux = intersection
               intersection = previous
               previous = aux
               graph.neighborhoodVertices(vertices.getu(i), neighborhood)
               intersection.clear()
               Utils.sintersect(previous, neighborhood, 0, previous.size(), 0,
                  neighborhood.size(), intersection)
               intersectionEmpty = intersection.isEmpty
               i += 1
            }

            intersection.reclaim()
            previous.reclaim()
         }

         neighborhood.reclaim()
         intersectionEmpty
      }

      val frac = self.vfractoid
         .expand(1)
         .filter((s,c) => s.numEdgesAdded() == s.getNumVertices - 1)
         .explore(maxNumVertices - 1)
         .filter(isMaximal)

      frac
   }

   /**
    * All maximal cliques up to a upper bound on the number of vertices using
    * the PA (pattern aware) approach
    * @param maxNumVertices upper bound on the number of vertices
    */
   def maximalCliquesPA(maxNumVertices: Int)
   : Fractoid[PatternInducedSubgraph] = {

      // function that verifies whether a subgraph (clique) is maximal
      val isMaximal = (s: PatternInducedSubgraph,
         c: Computation[PatternInducedSubgraph]) => {
         val vertices = s.getVertices
         val graph = c.getConfig.getMainGraph
         val neighborhood = IntArrayListViewPool.instance().createObject()
         var intersectionEmpty = false

         if (vertices.size() == 1) {
            graph.neighborhoodVertices(vertices.getu(0), neighborhood)
            intersectionEmpty = neighborhood.isEmpty
         } else {

            var intersection = IntArrayListPool.instance().createObject()
            var previous = IntArrayListPool.instance().createObject()

            // first neighborhood
            val n1 = graph.neighborhoodVertices(vertices.getu(0))
            val n2 = graph.neighborhoodVertices(vertices.getu(1))
            Utils.sintersect(n1, n2, 0, n1.size(), 0, n2.size(), intersection)
            n1.reclaim()
            n2.reclaim()

            intersectionEmpty = intersection.isEmpty

            var i = 2
            while (!intersectionEmpty && i < vertices.size()) {
               val aux = intersection
               intersection = previous
               previous = aux
               graph.neighborhoodVertices(vertices.getu(i), neighborhood)
               intersection.clear()
               Utils.sintersect(previous, neighborhood, 0, previous.size(), 0,
                  neighborhood.size(), intersection)
               intersectionEmpty = intersection.isEmpty
               i += 1
            }

            intersection.reclaim()
            previous.reclaim()
         }

         neighborhood.reclaim()
         intersectionEmpty
      }

      // single vertex cliques
      var pattern = PatternUtils.singleVertexPattern()
      pattern.setVertexLabeled(false)

      // explore and extend cliques
      while (pattern.getNumberOfVertices < maxNumVertices) {
         pattern.setVertexLabeled(false)
         // extend to next clique
         val newEdges = (0 until pattern.getNumberOfVertices).toArray
         pattern = PatternUtils.addVertex(pattern, newEdges:_*)
      }

      val frac = patternMatchingPF(pattern)
         .explore(pattern.getNumberOfVertices - 1)
         .filter(isMaximal)

      frac
   }

   /**
    * All maximal cliques listing implementing the parallel version of
    * Tomita's algorithm
    * [[https://dl.acm.org/doi/10.1145/3380936]]
    * @param maxNumVertices upper bound on the number of vertices
    * @return new fractoid with the initial state for maximal cliques
    */
   def maximalCliquesCustomQuick(maxNumVertices: Int)
   : Fractoid[VertexInducedSubgraph] = {

      // function that verifies whether a subgraph (clique) is maximal
      val isMaximal = (s: VertexInducedSubgraph,
                       c: Computation[VertexInducedSubgraph]) => {
         val vertices = s.getVertices
         val graph = c.getConfig.getMainGraph
         val neighborhood = IntArrayListViewPool.instance().createObject()
         var intersectionEmpty = false

         if (vertices.size() == 1) {
            graph.neighborhoodVertices(vertices.getu(0), neighborhood)
            intersectionEmpty = neighborhood.isEmpty
         } else {

            var intersection = IntArrayListPool.instance().createObject()
            var previous = IntArrayListPool.instance().createObject()

            // first neighborhood
            val n1 = graph.neighborhoodVertices(vertices.getu(0))
            val n2 = graph.neighborhoodVertices(vertices.getu(1))
            Utils.sintersect(n1, n2, 0, n1.size(), 0, n2.size(), intersection)
            n1.reclaim()
            n2.reclaim()

            intersectionEmpty = intersection.isEmpty

            var i = 2
            while (!intersectionEmpty && i < vertices.size()) {
               val aux = intersection
               intersection = previous
               previous = aux
               graph.neighborhoodVertices(vertices.getu(i), neighborhood)
               intersection.clear()
               Utils.sintersect(previous, neighborhood, 0, previous.size(), 0,
                  neighborhood.size(), intersection)
               intersectionEmpty = intersection.isEmpty
               i += 1
            }

            intersection.reclaim()
            previous.reclaim()
         }

         neighborhood.reclaim()
         intersectionEmpty
      }

      val senumClass = classOf[MaximalCliquesEnumerator[VertexInducedSubgraph]]
      self.vfractoid
         .expand(maxNumVertices, senumClass)
         .filter(isMaximal)
   }

   /**
    * Frequent subgraph mining using the "pattern-first" approach and the MNI
    * (Minimum Node Image) as support metric
    * @param minSupport Minimum support value
    * @param maxNumEdges Hard limit on the size of the interesting patterns
    * @return an RDD with frequent patterns and their current support
    */
   def fsmPF(minSupport: Int, maxNumEdges: Int)
   : RDD[(Pattern,MinImageSupport)] = {
      val app = new FSMPF(minSupport, maxNumEdges)
      app.apply(self)
   }

   /**
    * Frequent subgraph mining using the "pattern-first" approach and the MNI
    * (Minimum Node Image) as support metric and using the optimized MCVC
    * approach for pattern matching
    * @param supportThreshold Minimum support value
    * @param maxNumEdges Hard limit on the size of the interesting patterns
    * @return an RDD with frequent patterns and their current support
    */
   def fsmPFMCVC(supportThreshold: Int, maxNumEdges: Int)
   : RDD[(Pattern, MinImageSupport)] = {
      val app = new FSMPFMCVC(supportThreshold, maxNumEdges)
      app.apply(self)
   }

   /**
    * Frequent subgraph Mining (FSM)
    * @param support threshold to determine what is frequent according to
    *                the (minimum image)
    * @param maxNumEdges
    * @return an RDD with frequent patterns and their support
    */
   def fsmSF(support: Int, maxNumEdges: Int)
   : RDD[(Pattern,MinImageSupport)] = {
      val app = new FSMSF(support, maxNumEdges)
      app.apply(self)
   }

   def fsmHybrid(support: Int, maxNumEdges: Int)
   : RDD[(Pattern,MinImageSupport)] = {
      val app = new FSMHybrid(support, maxNumEdges)
      app.apply(self)
   }

   /**
    * Subgraph querying (pattern matching) using edge-by-edge exploration
    * @param pattern representing the subgraphs structure
    * @return new fractoid
    */
   def patternMatchingSF(pattern: Pattern): Fractoid[EdgeInducedSubgraph] = {
      val numEdges = pattern.getNumberOfEdges
      val quickPatterns = PatternUtils.quickPatterns(pattern)
      val quickPatternsPerLevel = new Array[Set[Pattern]](numEdges)

      var i = 0
      while (i < numEdges) {
         quickPatternsPerLevel(i) = Set.empty[Pattern]
         i += 1
      }

      val cur = quickPatterns.cursor()
      while (cur.moveNext()) {
         var pattern = cur.elem()
         i = numEdges - 1
         while (i >= 0) {
            quickPatternsPerLevel(i) += pattern
            pattern = pattern.copy()
            pattern.removeLastNEdges(1)
            i -= 1
         }
      }

      i = 0
      while (i < numEdges) {
         logApp(s"QuickPatterns ${quickPatternsPerLevel(i)}")
         i += 1
      }

      val sc = self.fractalContext.sparkContext
      val quickPatternsPerLevelBc = sc.broadcast(quickPatternsPerLevel)

      // filtering function: last edge must exist in the map
      val edgeFilterFunc
      : (EdgeInducedSubgraph, Computation[EdgeInducedSubgraph]) => Boolean =
         (s,c) => {
            val quickPattern = s.quickPattern()
            val numEdges = quickPattern.getNumberOfEdges
            quickPatternsPerLevelBc.value(numEdges - 1).contains(quickPattern)
         }

      val frac = self.efractoid
         .expand(1)
         .filter(edgeFilterFunc)

      frac
   }

   /**
    * Match pattern using Minimum Connected Vertex Cover (MCVC) strategy
    * TODO: these fractoids returned use a simple callback as last process
    * steps and thus, it is not safe to grow this workflow
    * @param pattern representing a query
    * @return an array of fractoids with partial query results
    */
   def patternMatchingPFMCVC(pattern: Pattern)
   : Array[Fractoid[PatternInducedSubgraph]] = {
      logInfo(s"PatternBeforePlan ${pattern}")

      val newPatterns = PatternExplorationPlanMCVCVgroups.apply(pattern)
      val partialResults = new Array[Fractoid[PatternInducedSubgraph]](newPatterns.size())

      var i = 0
      while (i < newPatterns.size()) {
         val nextPattern = newPatterns.getu(i)
         val explorationPlanMCVC = nextPattern.explorationPlan()
         val mcvcSize = explorationPlanMCVC.mcvcSize()

         val gquerying = this.patternMatchingPF(nextPattern)
            .explore(mcvcSize - 1)

         logInfo(s"MCVCPartialResult pattern=${nextPattern}" +
            s" sbLower=${nextPattern.vsymmetryBreakerLowerBound()}" +
            s" sbUpper=${nextPattern.vsymmetryBreakerUpperBound()}" +
            s" plan=${explorationPlanMCVC}" +
            s" fractoid=${gquerying}")

         partialResults(i) = gquerying
         i += 1
      }

      partialResults
   }

   /**
    * Subgraph querying (pattern matching)
    * @param pattern representing the subgraphs structure
    * @return new fractoid
    */
   def patternMatchingPF(pattern: Pattern): Fractoid[PatternInducedSubgraph] = {
      logInfo (s"Querying pattern ${pattern} in ${this}.")
      self.pfractoid(pattern).expand(1)
   }

   /**
    * Subgraph querying (pattern matching) - sampling version
    * @param pattern representing the subgraphs structure
    * @return new fractoid
    */
   def patternMatchingPF(pattern: Pattern, fraction: Double): Fractoid[PatternInducedSubgraph] = {
      logInfo (s"Querying fraction=${fraction} of pattern ${pattern} in ${this}.")
      val fractionKey = "sampling_fraction"
      val senumClass = classOf[SamplingEnumerator[PatternInducedSubgraph]]
      self.set(fractionKey, fraction).pfractoid(pattern).expand(1, senumClass)
   }

   /**
    * Vertex-induced implementation of quasi-cliques
    * @param numVertices number of vertices
    * @param minDensity density of edges between 0 and 1.
    * @return Fractoid with the initial state for quasi-cliques
    */
   def quasiCliquesSF(numVertices: Int,
                      minDensity: Double): Fractoid[VertexInducedSubgraph] = {
      val app = new QuasiCliquesSF(numVertices, minDensity)
      app.apply(self)
   }

   /**
    * Lists periodic induced subgraphs using the subgraph-first approach: visit
    * subgraph and filter.
    * @param periodicity Minimum number of times a subgraph must occur in the
    *                    graph to be considered periodic
    * @return fractoid containing the computation
    */
   def periodicInducedSubgraphsSF(periodicity: Int)
   : Fractoid[VertexInducedSubgraph] = {
      val app = new InducedPeriodicSubgraphsSF(periodicity)
      app.apply(self)
   }

   /**
    * Lists periodic induced subgraphs using the pattern-first
    * approach: generates valid patterns of certain size and matches each one
    * of them
    * @param periodicity Minimum number of times a subgraph must occur in the
    *                    graph to be considered periodic
    * @param callback callback to be applied to each partial result
    */
   def periodicInducedSubgraphsPF
   (periodicity: Int, numVertices: Int,
    callback: (Pattern, Fractoid[PatternInducedSubgraph]) => Unit): Unit = {
      val app = new InducedPeriodicSubgraphsPF(periodicity,
         numVertices, callback)
      app.apply(self)
   }

   /**
    * Lists periodic induced subgraphs using the pattern-first
    * approach: generates valid patterns of certain size and matches each one
    * of them. This version uses an optimized matching version MCVC.
    * @param periodicity Minimum number of times a subgraph must occur in the
    *                    graph to be considered periodic
    * @param callback callback to be applied to each partial result
    */
   def periodicInducedSubgraphsPFMCVC
   (periodicity: Int, numVertices: Int,
    callback: (InducedPeriodicSubgraphsPFMCVC, Pattern,
       Fractoid[PatternInducedSubgraph]) => Unit): Unit = {
      val app = new InducedPeriodicSubgraphsPFMCVC(periodicity,
         numVertices, callback)
      app.apply(self)
   }

   /**
    * Finds induced subgraphs containing only given labels (pattern-aware)
    * @param labelsSet target labels
    * @param numVertices number of vertices in the induced subgraphs
    * @param callback callback for each fractoid generated
    * @return fractoid representing the computation
    */
   def inducedSubgraphSearchLabelsPA
   (labelsSet: Set[Int], numVertices: Int): Iterator[Fractoid[PatternInducedSubgraph]] = {
      val labelFilter
      : (PatternInducedSubgraph, Computation[PatternInducedSubgraph]) => Boolean =
         (s,c) => {
            val graph = s.getMainGraph
            val vertices = s.getVertices
            val numVertices = vertices.size()
            var valid = true
            var i = 0
            while (valid && i < numVertices) {
               val u = vertices.getu(i)
               if (!labelsSet.contains(graph.firstVertexLabel(u))) {
                  valid = false
               }
               i += 1
            }
            valid
         }

      val patternsIter = PatternUtilsRDD
         .vertexPatternsRDD(self.fractalContext.sparkContext, numVertices).toLocalIterator

      patternsIter.map(pattern => {
         self.pfractoid(pattern)
            .expand(1).filter(labelFilter)
            .explore(numVertices - 1)
      })
   }

   /**
    * Finds induced subgraphs containing only given labels
    * @param labelsSet target labels
    * @param numVertices number of vertices in the subgraphs
    * @return fractoid representing the computation
    */
   def inducedSubgraphSearchLabelsPO(labelsSet: Set[Int], numVertices: Int)
   : Fractoid[VertexInducedSubgraph] = {
      val labelFilter
      : (VertexInducedSubgraph, Computation[VertexInducedSubgraph]) => Boolean =
         (s,c) => {
            val graph = s.getMainGraph
            val vertices = s.getVertices
            val numVertices = vertices.size()
            var valid = true
            var i = 0
            while (valid && i < numVertices) {
               val u = vertices.getu(i)
               if (!labelsSet.contains(graph.firstVertexLabel(u))) {
                  valid = false
               }
               i += 1
            }
            valid
         }

     self.vfractoid.expand(1).filter(labelFilter).explore(numVertices - 1)
   }

   /**
    * Given a set of words (labels) find subgraphs such that each edge in it
    * covers some uncovered word by the others edges of the subgraph
    * @param keywords
    * @return new fractoid
    */
   def keywordSearchPO(keywords: Set[Int], numEdges: Int)
   : Fractoid[EdgeInducedSubgraph] = {
      val app = new KeywordSearchPO(keywords, numEdges)
      app.apply(self)
   }

   /**
    * Attempt to extract a set of canonical patterns draw from *numSamples*
    * subgraph samples with *numVertices* vertices.
    * @param numSamples
    * @param numVertices
    * @return a collection of canonical patterns
    */
   def getPatternsFromSamples
   (numSamples: Int, numVertices: Int, seed: Long = 1L): RDD[Pattern] = {
      val numThreads = if (self.numPartitions > numSamples) 1 else self.numPartitions
      val samplesPerThread = Math.max(numSamples / numThreads, 1)
      val patternsRDD = self
         .set("samples_per_thread", samplesPerThread)
         .set("random_walk_seed", seed)
         .set("num_partitions", numThreads)
         .vfractoid
         .expand(numVertices,
            classOf[RandomWalkEnumerator[VertexInducedSubgraph]])
         .aggregationCanonicalPatternLong(
            s => s.quickPattern(), 0L, s => 0L, (v, _) => v)
         .keys

      patternsRDD
   }
}
