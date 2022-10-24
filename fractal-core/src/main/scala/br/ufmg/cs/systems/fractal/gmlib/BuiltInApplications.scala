package br.ufmg.cs.systems.fractal.gmlib

import br.ufmg.cs.systems.fractal.computation.{AllEdgesSubgraphEnumerator, Computation, RandomWalkEnumerator, SamplingEnumerator}
import br.ufmg.cs.systems.fractal.gmlib.clique.{KClistEnumerator, MaximalCliquesEnumerator}
import br.ufmg.cs.systems.fractal.gmlib.fsm._
import br.ufmg.cs.systems.fractal.gmlib.kws.{KeywordSearchPO, MinimalKeywordSearchPA, MinimalKeywordSearchPO}
import br.ufmg.cs.systems.fractal.gmlib.mcvc.MCVCEnumerator
import br.ufmg.cs.systems.fractal.gmlib.motifs.{MotifsHybrid, MotifsPA, MotifsPAMCVC, MotifsPO}
import br.ufmg.cs.systems.fractal.gmlib.periodic.{InducedPeriodicSubgraphsPA, InducedPeriodicSubgraphsPAMCVC, InducedPeriodicSubgraphsPO}
import br.ufmg.cs.systems.fractal.gmlib.quasicliques.{QuasiCliquesPA, QuasiCliquesPAPO, QuasiCliquesPO}
import br.ufmg.cs.systems.fractal.gmlib.queryspecialization.{QuerySpecializationPAPO, QuerySpecializationPO}
import br.ufmg.cs.systems.fractal.pattern._
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, PatternInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import br.ufmg.cs.systems.fractal.util.pool.{IntArrayListPool, IntArrayListViewPool}
import br.ufmg.cs.systems.fractal.util.{Logging, Utils}
import br.ufmg.cs.systems.fractal.{FractalGraph, Fractoid}
import com.koloboke.collect.set.hash.{HashIntSets, HashObjSets}
import org.apache.spark.rdd.RDD

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
 * Built-in algorithms with the following alternative implementations:
 *
 * PO: Subgraph-first paradigm
 * PA: Pattern-first paradigm
 * PAMCVC: Pattern-first paradigm optimized by matching the minimum connected
 * vertex cover of the pattern first and then, finishing the matching in one
 * step because the cover gives access to all remaining neighborhoods
 * necessary for the whole pattern matching
 * @param self fractal graph
 */
class BuiltInApplications(self: FractalGraph) extends Logging {

   /**
    * Generated all subgraphs using the PA approach
    * @param numEdges number of edges in the subgraphs
    * @param callback to be applied to each partial result of this computation
    */
   def subgraphsMaxEdgesPA
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
         val partialResult = patternQueryingPA(pattern)
            .explore(pattern.getNumberOfVertices - 1)
         val future = Future(callback(partialResult))
         futures = future :: futures
      }

      Await.result(Future.sequence(futures), Duration.Inf)
   }

   /**
    * Generated all subgraphs using the PA approach
    * @param numVertices number of vertices in the subgraphs
    * @param callback to be applied to each partial result of this computation
    */
   def subgraphsPA
   (numVertices: Int, callback: Fractoid[PatternInducedSubgraph] => Unit)
   : Unit = {

      val canonicalPatternsRDD = PatternUtilsRDD.getOrGenerateVertexPatternsRDD(
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
         val partialResult = patternQueryingPA(pattern).explore(numVertices - 1)
         callback(partialResult)
      }
   }

   /**
    * Generated all induced subgraphs using the PAMCVC approach
    * @param numVertices number of vertices in the induced subgraphs
    * @param callback to be applied to each partial result of this computation
    */
   def inducedSubgraphsPAMCVC
   (numVertices: Int,
    callback: Fractoid[PatternInducedSubgraph] => Unit): Unit = {

      // generate all canonical patterns with *numVertices* vertices
      val canonicalPatternsRDD = PatternUtilsRDD.getOrGenerateVertexPatternsRDD(
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
            val partialResult = patternQueryingPA(pattern).explore(mcvcSize - 1)
            callback(partialResult)
         }
      }
   }

   /**
    * Generated all induced subgraphs using the PA approach
    * @param numVertices number of vertices in the induced subgraphs
    * @param callback to be applied to each partial result of this computation
    */
   def inducedSubgraphsPA
   (numVertices: Int, callback: Fractoid[PatternInducedSubgraph] => Unit)
   : Unit = {

      val canonicalPatternsRDD = PatternUtilsRDD.getOrGenerateVertexPatternsRDD(
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
         val partialResult = patternQueryingPA(pattern).explore(numVertices - 1)
         callback(partialResult)
      }
   }

   /**
    * Motifs counting by listing and using the PO paradigm
    * @param numVertices motifs size
    * @return RDD representing a mapping (CanonicalPattern -> Count)
    */
   def motifsPO(numVertices: Int): RDD[(Pattern,Long)] = {
      val app = new MotifsPO(numVertices)
      app.apply(self)
   }

   /**
    * Motifs counting by listing and using the PAMCVC paradigm
    * @param numVertices motifs size
    * @return RDD representing a mapping (CanonicalPattern -> Count)
    */
   def motifsPAMCVC(numVertices: Int): RDD[(Pattern,Long)] = {
      val app = new MotifsPAMCVC(numVertices)
      app.apply(self)
   }

   /**
    * Motifs counting by listing and using the PA paradigm
    * @param numVertices motifs size
    * @return RDD representing a mapping (CanonicalPattern -> Count)
    */
   def motifsPA(numVertices: Int): RDD[(Pattern,Long)] = {
      val app = new MotifsPA(numVertices)
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
    * and using the PO paradigm
    * @param numVertices motifs size
    * @param fraction sample fraction
    * @return RDD representing a mapping (CanonicalPattern -> FractionCount)
    */
   def motifsSamplePO(numVertices: Int, fraction: Double)
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

      val frac = patternQueryingPA(pattern)
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

      val frac = patternQueryingPA(pattern)
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
   def fsmPA(minSupport: Int, maxNumEdges: Int)
   : RDD[(Pattern,MinImageSupport)] = {
      val app = new FSMPA(minSupport, maxNumEdges)
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
   def fsmPAMCVC(supportThreshold: Int, maxNumEdges: Int)
   : RDD[(Pattern, MinImageSupport)] = {
      val app = new FSMPAMCVC(supportThreshold, maxNumEdges)
      app.apply(self)
   }

   /**
    * Frequent subgraph Mining (FSM)
    * @param support threshold to determine what is frequent according to
    *                the (minimum image)
    * @param maxNumEdges
    * @return an RDD with frequent patterns and their support
    */
   def fsmPO(support: Int, maxNumEdges: Int)
   : RDD[(Pattern,MinImageSupport)] = {
      val app = new FSMPO(support, maxNumEdges)
      app.apply(self)
   }

   /**
    * Frequent subgraph mining (FSM) combining a pattern-aware and
    * pattern-oblivious subgraph exploration strategy
    * @param support
    * @param maxNumEdges
    * @return an RDD with frequent patterns and their support
    */
   def fsmPAPO(support: Int, maxNumEdges: Int)
   : RDD[(Pattern,MinImageSupport)] = {
      val app = new FSMPAPO(support, maxNumEdges)
      app.apply(self)
   }

   /**
    * Subgraph querying (pattern matching) using edge-by-edge exploration
    * @param pattern representing the subgraphs structure
    * @return new fractoid
    */
   def patternQueryingPO(pattern: Pattern): Fractoid[EdgeInducedSubgraph] = {
      val numEdges = pattern.getNumberOfEdges
      val quickPatterns = PatternUtils.quickPatterns(pattern)
      val quickPatternsPerLevel = PatternUtils.quickPatternsPerLevel(quickPatterns)

      var i = 0
      while (i < numEdges) {
         logApp(s"QuickPatterns size=${quickPatternsPerLevel(i).size}" +
            s" ${quickPatternsPerLevel(i)}")
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
   def patternQueryingPAMCVC(pattern: Pattern)
   : Array[Fractoid[PatternInducedSubgraph]] = {
      logInfo(s"PatternBeforePlan ${pattern}")

      val newPatterns = PatternExplorationPlanMCVC.apply(pattern)
      val partialResults = new Array[Fractoid[PatternInducedSubgraph]](newPatterns.size())

      var i = 0
      while (i < newPatterns.size()) {
         val nextPattern = newPatterns.getu(i)
         val explorationPlanMCVC = nextPattern.explorationPlan()

         val gquerying = self.pfractoid(nextPattern)
            .expand(nextPattern.getNumberOfVertices, classOf[MCVCEnumerator])

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

   def patternQueryingPAMCVC_old(pattern: Pattern)
   : Array[Fractoid[PatternInducedSubgraph]] = {
      logInfo(s"PatternBeforePlan ${pattern}")

      val newPatterns = PatternExplorationPlanMCVC.apply(pattern)
      val partialResults = new Array[Fractoid[PatternInducedSubgraph]](newPatterns.size())

      var i = 0
      while (i < newPatterns.size()) {
         val nextPattern = newPatterns.getu(i)
         val explorationPlanMCVC = nextPattern.explorationPlan()
         val mcvcSize = explorationPlanMCVC.mcvcSize()

         val gquerying = this.patternQueryingPA(nextPattern)
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
   def patternQueryingPA(pattern: Pattern): Fractoid[PatternInducedSubgraph] = {
      logInfo (s"Querying pattern ${pattern} in ${this}.")
      self.pfractoid(pattern).expand(1)
   }

   /**
    * Subgraph querying (pattern matching) - sampling version
    * @param pattern representing the subgraphs structure
    * @return new fractoid
    */
   def patternQueryingPA(pattern: Pattern, fraction: Double): Fractoid[PatternInducedSubgraph] = {
      logInfo (s"Querying fraction=${fraction} of pattern ${pattern} in ${this}.")
      val fractionKey = "sampling_fraction"
      val senumClass = classOf[SamplingEnumerator[PatternInducedSubgraph]]
      self.set(fractionKey, fraction).pfractoid(pattern).expand(1, senumClass)
   }

   /**
    * Pattern-oblivious implementation of quasi-cliques
    * @param numVertices number of vertices
    * @param minDensity density of edges between 0 and 1.
    * @return Fractoid with the initial state for quasi-cliques
    */
   def quasiCliquesPO(numVertices: Int,
                      minDensity: Double): Fractoid[VertexInducedSubgraph] = {
      val app = new QuasiCliquesPO(numVertices, minDensity)
      app.apply(self)
   }

   /**
    * Pattern-aware implementation of quasi-cliques
    * @param numVertices number of vertices
    * @param minDensity density of edges between 0 and 1.
    * @return Fractoid with the initial state for quasi-cliques
    */
   def quasiCliquesPA(numVertices: Int, minDensity: Double)
   : Seq[Fractoid[PatternInducedSubgraph]] = {
      val app = new QuasiCliquesPA(numVertices, minDensity)
      app.apply(self)
   }

   /**
    * Pattern-aware and pattern-oblivious hybrid implementation of quasi-cliques
    * @param numVertices number of vertices
    * @param minDensity density of edges between 0 and 1.
    * @return Fractoid with the initial state for quasi-cliques
    */
   def quasiCliquesPAPO(numVertices: Int, minDensity: Double)
   : Seq[Fractoid[VertexInducedSubgraph]] = {
      val app = new QuasiCliquesPAPO(numVertices, minDensity)
      app.apply(self)
   }

   /**
    * Lists periodic induced subgraphs using the subgraph-first approach: visit
    * subgraph and filter.
    * @param periodicity Minimum number of times a subgraph must occur in the
    *                    graph to be considered periodic
    * @return fractoid containing the computation
    */
   def periodicInducedSubgraphsPO(periodicity: Int)
   : Fractoid[VertexInducedSubgraph] = {
      val app = new InducedPeriodicSubgraphsPO(periodicity)
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
   def periodicInducedSubgraphsPA
   (periodicity: Int, numVertices: Int,
    callback: (Pattern, Fractoid[PatternInducedSubgraph]) => Unit): Unit = {
      val app = new InducedPeriodicSubgraphsPA(periodicity,
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
   def periodicInducedSubgraphsPAMCVC
   (periodicity: Int, numVertices: Int,
    callback: (InducedPeriodicSubgraphsPAMCVC, Pattern,
       Fractoid[PatternInducedSubgraph]) => Unit): Unit = {
      val app = new InducedPeriodicSubgraphsPAMCVC(periodicity,
         numVertices, callback)
      app.apply(self)
   }

   /**
    * Finds induced subgraphs containing only given labels (pattern-aware)
    * @param labelsSet target labels
    * @param numVertices number of vertices in the induced subgraphs
    * @return fractoid representing the computation
    */
   def labelSearchPA(labelsSet: Set[Int], numVertices: Int)
   : Iterator[Fractoid[PatternInducedSubgraph]] = {
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

      val sc = self.fractalContext.sparkContext
      val patterns = PatternUtilsRDD
         .getOrGenerateVertexPatternsRDD(sc, numVertices)

      patterns.toLocalIterator.map(pattern => {
         pattern.setInduced(true)
         pattern.setVertexLabeled(false)
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
   def labelSearchPO(labelsSet: Set[Int], numVertices: Int)
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
    * Given a set of words (labels) find subgraphs such that each keyword is
    * covered by some vertex and this subgraph should be minimal.
    * @param keywords
    * @param numVertices
    * @return new fractoid
    */
   def minimalKeywordSearchPO(keywords: Set[Int], numVertices: Int)
   : Fractoid[VertexInducedSubgraph] = {
      val app = new MinimalKeywordSearchPO(keywords, numVertices)
      app.apply(self)
   }

   /**
    * Given a set of words (labels) find subgraphs such that each keyword is
    * covered by some vertex and this subgraph should be minimal.
    * @param keywords
    * @param numVertices
    * @return new fractoid
    */
   def minimalKeywordSearchPA(keywords: Set[Int], numVertices: Int)
   : Iterator[Fractoid[PatternInducedSubgraph]] = {
      val app = new MinimalKeywordSearchPA(keywords, numVertices)
      app.apply(self)
   }

   /**
    * Pattern-oblivious implementation of query specialization result set
    * computation: given a pattern, obtain subgraph instances of pattern
    * specializations of that pattern. A pattern specialization is defined as
    * the given pattern with an additional edge.
    * @param pattern query
    * @return new fractoid
    */
   def querySpecializationPO(pattern: Pattern)
   : Fractoid[EdgeInducedSubgraph] = {
      val sc = self.fractalContext.sparkContext
      pattern.setInduced(false)
      pattern.setVertexLabeled(true)
      pattern.setEdgeLabeled(false)

      // graph vertex labels
      val vertexLabels = self.vfractoid.expand(1)
         .aggregationLongLong(
            s => {
               val graph = s.getMainGraph()
               graph.firstVertexLabel(s.getVertices.getLast)
            },
            0L,
            s => 1L,
            (a, _) => a
         )
         .keys.distinct().map(_.toInt)

      val internalExtendedPatterns = PatternUtilsRDD
         .extendByEdgeInternalRDD(sc.makeRDD(List(pattern)), 1)

      val externalExtendedPatterns = vertexLabels
         .flatMap(vlabel => {
            val patterns = PatternUtils.extendByEdgeExternal(pattern, vlabel, 1)
            patterns.toArray.map(_.asInstanceOf[Pattern])
         })

      val patterns = sc
         .union(internalExtendedPatterns, externalExtendedPatterns)

      val patternsBc = sc.broadcast(patterns.collect())
      val app = new QuerySpecializationPO(patternsBc)
      app.apply(self)
   }

   /**
    * Pattern-aware implementation of query specialization result set
    * computation: given a pattern, obtain subgraph instances of pattern
    * specializations of that pattern. A pattern specialization is defined as
    * the given pattern with an additional edge.
    * @param pattern query
    * @return new fractoid
    */
   def querySpecializationPA(pattern: Pattern)
   : Seq[Fractoid[PatternInducedSubgraph]] = {
      pattern.setInduced(false)
      pattern.setVertexLabeled(true)
      pattern.setEdgeLabeled(false)

      val sc = self.fractalContext.sparkContext

      // graph vertex labels
      val vertexLabels = self.vfractoid.expand(1)
         .aggregationLongLong(
            s => {
               val graph = s.getMainGraph()
               graph.firstVertexLabel(s.getVertices.getLast)
            },
            0L,
            s => 1L,
            (a, _) => a
         )
         .keys.distinct().map(_.toInt)

      val internalExtendedPatterns = PatternUtilsRDD
         .extendByEdgeInternalRDD(sc.makeRDD(List(pattern)), 1)

      val externalExtendedPatterns = vertexLabels
         .flatMap(vlabel => {
            val patterns = PatternUtils.extendByEdgeExternal(pattern, vlabel, 1)
            patterns.toArray.map(_.asInstanceOf[Pattern])
         })

      val extendedPatterns = sc
         .union(internalExtendedPatterns, externalExtendedPatterns)

      logApp(s"numPatterns=${extendedPatterns.count()}")

      val fractoids = extendedPatterns
         .map(pattern => {
            pattern.setInduced(false)
            pattern.setVertexLabeled(true)
            pattern.setEdgeLabeled(false)
            pattern
         })
         .collect()
         .map(pattern => {
            self.pfractoid(pattern).expand(pattern.getNumberOfVertices)
         })

      fractoids
   }

   /**
    * Pattern-aware and pattern-oblivious hybrid implementation of query
    * specialization result set
    * computation: given a pattern, obtain subgraph instances of pattern
    * specializations of that pattern. A pattern specialization is defined as
    * the given pattern with an additional edge.
    * @param _pattern query
    * @return new fractoid
    */
   def querySpecializationPAPO(_pattern: Pattern)
   : Fractoid[EdgeInducedSubgraph] = {
      _pattern.setInduced(false)
      _pattern.setVertexLabeled(true)
      _pattern.setEdgeLabeled(false)
      val pattern = self.pfractoid(_pattern).pattern
      val app = new QuerySpecializationPAPO(pattern)
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
