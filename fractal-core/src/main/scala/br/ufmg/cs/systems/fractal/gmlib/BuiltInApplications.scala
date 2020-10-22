package br.ufmg.cs.systems.fractal.gmlib

import br.ufmg.cs.systems.fractal.annotation.Experimental
import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.fsm.{FSMPF, FSMPFMCVC, FSMSF, MinImageSupport}
import br.ufmg.cs.systems.fractal.gmlib.motifs.{MotifsPF, MotifsPFMCVC, MotifsSF}
import br.ufmg.cs.systems.fractal.gmlib.periodic.{InducedPeriodicSubgraphsPF, InducedPeriodicSubgraphsPFMCVC, InducedPeriodicSubgraphsSF}
import br.ufmg.cs.systems.fractal.graph.MainGraph
import br.ufmg.cs.systems.fractal.pattern._
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, PatternInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.collection.ObjSet
import br.ufmg.cs.systems.fractal.util.pool.{IntArrayListPool, IntArrayListViewPool}
import br.ufmg.cs.systems.fractal.util.{Logging, SubgraphCallback, Utils}
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
    * Motifs counting by listing a sample of subgraphs uniformly at random
    * and using the SF paradigm
    * @param numVertices motifs size
    * @param fraction sample fraction
    * @return RDD representing a mapping (CanonicalPattern -> FractionCount)
    */
   def motifsSampleSF(numVertices: Int, fraction: Double)
   : RDD[(Pattern,Long)] = {
      self.svfractoid(fraction).expand(numVertices).
         aggregationCanonicalPatternLong(
            s => s.quickPattern(), 0, _ => 1L, _ + _)
   }

   /**
    * All-cliques listing using the SF paradigm
    * @param numVertices number of vertices in the cliques
    * @return fractoid with cliques computation
    */
   def cliquesSF(numVertices: Int): Fractoid[VertexInducedSubgraph] = {
      self.vfractoid.expand(1)
         .filter((e,c) => e.numEdgesAdded == e.getNumVertices - 1)
         .explore(numVertices - 1)
   }

   /**
    * All-cliques listing implementing the efficient DAG structure from
    * [[https://dl.acm.org/citation.cfm?id=3186125]]
    * @return Fractoid with the cliques computation
    */
   def cliquesKClistSF(numVertices: Int): Fractoid[VertexInducedSubgraph] = {
      val enumClass = "br.ufmg.cs.systems.fractal.gmlib.clique.KClistEnumerator"
      self.set("subgraph_enumerator", enumClass).vfractoid.expand(numVertices)
   }

   /**
    * All maximal cliques up to a upper bound on the number of vertices using
    * the PF approach (naive)
    * @param maxNumVertices upper bound on the number of vertices
    * @param callback user defined function to be applied to each partial
    *                 computation
    */
   def maximalCliquesPF(maxNumVertices: Int,
                        callback: Fractoid[PatternInducedSubgraph] => Unit)
   : Unit = {

      // function that verifies whether a subgraph (clique) is maximal
      val isMaximal = (s: PatternInducedSubgraph,
         c: Computation[PatternInducedSubgraph]) => {
         val vertices = s.getVertices
         val graph = c.getConfig.getMainGraph[MainGraph[_,_]]
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

      // explore and extend cliques
      while (pattern.getNumberOfVertices <= maxNumVertices) {
         pattern.setVertexLabeled(false)
         val frac = patternMatchingPF(pattern)
            .explore(pattern.getNumberOfVertices - 1)
            .filter(isMaximal)

         // user callback
         callback(frac)

         // extend to next clique
         val newEdges = (0 until pattern.getNumberOfVertices).toArray
         pattern = PatternUtils.addVertex(pattern, newEdges:_*)
      }
   }

   /**
    * All maximal cliques listing implementing the parallel version of
    * Tomita's algorithm
    * [[https://dl.acm.org/doi/10.1145/3380936]]
    * @param maxNumVertices upper bound on the number of vertices
    * @return new fractoid with the initial state for maximal cliques
    */
   def maximalCliquesQuickSF(maxNumVertices: Int)
   : Fractoid[VertexInducedSubgraph] = {
      val enumClass = "br.ufmg.cs.systems.fractal.gmlib.clique.MaximalCliquesEnumerator"
      self.set("subgraph_enumerator", enumClass).vfractoid
         // explore one step further to ensure that *maxNumVertices* sized
         // cliques are considered
         .expand(maxNumVertices + 1)
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

      val newPatterns = PatternExplorationPlanMCVC.apply(pattern)
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
      self.spfractoid(pattern, fraction).expand(1)
   }

   /**
    * Vertex-induced implementation of quasi-cliques
    * @param numSteps maximum number of steps
    * @param minDensity density of edges between 0 and 1.
    * @return Fractoid with the initial state for quasi-cliques
    */
   def quasiCliques(
                      numSteps: Int,
                      minDensity: Double): Fractoid[VertexInducedSubgraph] = {

      if (numSteps < 1) {
         throw new RuntimeException(
            "Quasi-Cliques: numSteps should be at least 1.")
      }

      // if the quasi-cliques size is bounded by *maxSize* we can actually set min
      // bounds for density at each position, the idea is that if this minimum
      // value is not met in given position, it will be impossible for
      // *minDensity* e reached later on
      val maxDensity = (numSteps + 1) * numSteps / 2.0
      val cummDensities = new Array[Double](numSteps + 1)
      cummDensities(cummDensities.length - 1) = 0.0
      var i = cummDensities.length - 2
      while (i >= 0) {
         cummDensities(i) = cummDensities(i + 1) + (i + 1) / maxDensity
         i -= 1
      }

      logInfo(s"QuasiCliques: maxDensity=${maxDensity}" +
         s" cummDensities=${cummDensities.mkString(",")}")

      self.vfractoid.
         expand(1).
         filter((e,c) => (e.getNumEdges() / maxDensity) +
            cummDensities(e.getNumVertices() - 1) >= minDensity).
         explore(numSteps)
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
}
