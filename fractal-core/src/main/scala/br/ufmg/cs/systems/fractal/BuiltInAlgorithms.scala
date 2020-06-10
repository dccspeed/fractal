package br.ufmg.cs.systems.fractal

import java.util.Map
import java.util.function.Predicate

import br.ufmg.cs.systems.fractal.aggregation.{AggregationStorage, PatternAggregationStorage}
import br.ufmg.cs.systems.fractal.annotation.Experimental
import br.ufmg.cs.systems.fractal.computation.{Computation, SubgraphEnumerator}
import br.ufmg.cs.systems.fractal.gmlib.clique.KClistEnumerator
import br.ufmg.cs.systems.fractal.gmlib.fsm.{DomainSupport, DomainSupportEndAggregationFunction}
import br.ufmg.cs.systems.fractal.graph.MainGraph
import br.ufmg.cs.systems.fractal.pattern.{BasicPattern, Pattern, PatternExplorationPlan, PatternExplorationPlanMCVC, PatternExplorationPlanMCVCOrdering, PatternExplorationPlanMCVCVgroups, PatternUtils}
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, PatternInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.collection.{IntArrayList, ObjArrayList, ObjSet}
import br.ufmg.cs.systems.fractal.util.{Logging, SubgraphCallback, Utils, VertexPredicate}
import br.ufmg.cs.systems.fractal.util.pool.{IntArrayListPool, IntArrayListViewPool}
import com.koloboke.collect.map.ObjObjMap
import com.koloboke.collect.map.hash.HashObjObjMaps
import com.koloboke.collect.set.hash.{HashIntSet, HashIntSets, HashObjSet, HashObjSets}
import org.apache.hadoop.io.{IntWritable, LongWritable, NullWritable}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

class BuiltInAlgorithms(self: FractalGraph) extends Logging {

   /**
    * Induced subgraphs using the pattern-first approach and the MCVC pattern
    * matching optimization
    * @return induced subgraphs fractoids with partial results representing
    *         the minimum connected vertex cover matching
    */
   def inducedSubgraphsPfMCVC(numVertices: Int)
   : ObjArrayList[Fractoid[PatternInducedSubgraph]] = {
      /**
       * Generate all patterns with *numVertices* vertices
       */
      val startPatternGeneration = System.currentTimeMillis()
      var patterns = PatternUtils.singleVertexPatternSet()
      logInfo(s"PatternSet ${patterns}")
      for (i <- 0 until numVertices - 1) {
         patterns = PatternUtils.extendByVertex(patterns, 1)
         logInfo(s"PatternSetExtension[${i + 1}] ${patterns}")
      }
      val elapsedPatternGeneration = System.currentTimeMillis() -
         startPatternGeneration

      logInfo(s"CanonicalPatterns numVertices=${numVertices}" +
         s" numPatterns=${patterns.size()}" +
         s" elapsed=${elapsedPatternGeneration}")

      val fractoids = new ObjArrayList[Fractoid[PatternInducedSubgraph]](
         patterns.size()
      )

      val cur = patterns.cursor()
      while (cur.moveNext()) {
         val patternWithoutPlan = cur.elem()
         patternWithoutPlan.setInduced(true)
         patternWithoutPlan.setVertexLabeled(false)

         val newPatternsCur = PatternExplorationPlanMCVC
            .apply(patternWithoutPlan).cursor()

         while (newPatternsCur.moveNext()) {
            val pattern = newPatternsCur.elem()
            val mcvcSize = pattern.explorationPlan().mcvcSize()
            val partialResult = gquerying(pattern).explore(mcvcSize - 1)
            fractoids.add(partialResult)
         }
      }

      fractoids
   }

   /**
    * Motifs counting using the pattern-first approach
    * @return motif counts
    */
   def vertexInducedPf(numVertices: Int)
   : ObjArrayList[Fractoid[PatternInducedSubgraph]] = {
      /**
       * Generate all patterns with *numVertices* vertices
       */
      val startPatternGeneration = System.currentTimeMillis()
      var patterns = PatternUtils.singleVertexPatternSet()
      logInfo(s"PatternSet ${patterns}")
      for (i <- 0 until numVertices - 1) {
         patterns = PatternUtils.extendByVertex(patterns, 1)
         logInfo(s"PatternSetExtension[${i + 1}] ${patterns}")
      }
      val elapsedPatternGeneration = System.currentTimeMillis() -
         startPatternGeneration

      logInfo(s"CanonicalPatterns numVertices=${numVertices}" +
         s" numPatterns=${patterns.size()}" +
         s" elapsed=${elapsedPatternGeneration}")

      val fractoids = new ObjArrayList[Fractoid[PatternInducedSubgraph]]()

      val cur = patterns.cursor()
      while (cur.moveNext()) {
         val patternWithoutPlan = cur.elem()

         /**
          * Motifs counting consider induced subgraphs, unlabeled at first
          */
         patternWithoutPlan.setInduced(true)
         patternWithoutPlan.setVertexLabeled(false)

         /**
          * Naive exploration plan
          */
         val pattern = PatternExplorationPlan.apply(patternWithoutPlan).get(0)
         pattern.updateSymmetryBreakerVertexUnlabeled()

         val partialResult = gquerying(pattern).explore(numVertices - 1)
         fractoids.add(partialResult)
      }

      fractoids
   }

   /**
    * Motifs counting
    * @return Fractoid with the initial state for motifs
    */
   def motifs: Fractoid[VertexInducedSubgraph] = {
      import br.ufmg.cs.systems.fractal.pattern.Pattern
      import org.apache.hadoop.io.LongWritable

      val AGG_MOTIFS = "motifs"
      self.vfractoid.
         expand(1).
         aggregate [Pattern,LongWritable] (
            AGG_MOTIFS,
            (e,c,k) => { e.quickPattern },
            (e,c,v) => { v.set(1); v },
            (v1,v2) => { v1.set(v1.get() + v2.get()); v1 })
   }

   /**
    * Approximate motifs counting
    * @return Fractoid with the initial state for motifs
    */
   def motifs(fraction: Double): Fractoid[VertexInducedSubgraph] = {
      import br.ufmg.cs.systems.fractal.pattern.Pattern
      import org.apache.hadoop.io.LongWritable

      val AGG_MOTIFS = "motifs"
      self.svfractoid(fraction).
         expand(1).
         aggregate [Pattern,LongWritable] (
            AGG_MOTIFS,
            (e,c,k) => { e.quickPattern },
            (e,c,v) => { v.set(1); v },
            (v1,v2) => { v1.set(v1.get() + v2.get()); v1 })
   }

   /**
    * Motifs counting using the pattern-first approach
    * @return motif counts
    */
   def motifspfmcvc(numVertices: Int): ObjObjMap[Pattern,LongWritable] = {
      val MOTIFS = "motifs"
      val motifsCounts = HashObjObjMaps.newMutableMap[Pattern,LongWritable]()

      /**
       * Generate all patterns with *numVertices* vertices
       */
      val startPatternGeneration = System.currentTimeMillis()
      var patterns = PatternUtils.singleVertexPatternSet()
      logInfo(s"PatternSet ${patterns}")
      for (i <- 0 until numVertices - 1) {
         patterns = PatternUtils.extendByVertex(patterns, 1)
         logInfo(s"PatternSetExtension[${i + 1}] ${patterns}")
      }
      val elapsedPatternGeneration = System.currentTimeMillis() -
         startPatternGeneration

      logInfo(s"CanonicalPatterns numVertices=${numVertices}" +
         s" numPatterns=${patterns.size()}" +
         s" elapsed=${elapsedPatternGeneration}")

      val matchCallback = new SubgraphCallback[PatternInducedSubgraph] {
         private var aggregationStorage: AggregationStorage[Pattern,LongWritable] = _
         private var reusableValue: LongWritable = _

         override def apply(s: PatternInducedSubgraph, c: Computation[PatternInducedSubgraph])
         : Unit = {
            reusableValue.set(1)
            aggregationStorage.aggregateWithReusables(
               s.applyLabels(c.getPattern),
               reusableValue
            )
         }

         override def init(computation: Computation[PatternInducedSubgraph]): Unit = {
            val engine = computation.getExecutionEngine
            if (engine != null) {
               aggregationStorage = engine.getAggregationStorage(MOTIFS)
               reusableValue = aggregationStorage.reusableValue()
            }
         }
      }

      val callback = new SubgraphCallback[PatternInducedSubgraph] {
         val callback = matchCallback
         override def apply(s: PatternInducedSubgraph,
                            c: Computation[PatternInducedSubgraph]): Unit = {
            s.completeMatch(c, c.getPattern, callback)
         }

         override def init(c: Computation[PatternInducedSubgraph]): Unit = {
            callback.init(c)
         }
      }

      val cur = patterns.cursor()
      while (cur.moveNext()) {
         val patternWithoutPlan = cur.elem()
         patternWithoutPlan.setInduced(true)
         patternWithoutPlan.setVertexLabeled(false)

         val newPatternsCur = PatternExplorationPlanMCVC
            .apply(patternWithoutPlan).cursor()

         while (newPatternsCur.moveNext()) {
            val pattern = newPatternsCur.elem()
            val mcvcSize = pattern.explorationPlan().mcvcSize()
            val partialMap = gquerying(pattern)
               .explore(mcvcSize - 1)
               .aggregationMapWithCallback[Pattern,LongWritable](
                  MOTIFS,
                  callback,
                  (v1,v2) => {v1.set(v1.get()+v2.get()); v1}
               )
            for ((m,c) <- partialMap) motifsCounts.put(m,c)
         }
      }

      motifsCounts

   }

   /**
    * Motifs counting using the pattern-first approach
    * @return motif counts
    */
   def motifspf(numVertices: Int): ObjObjMap[Pattern,LongWritable] = {
      val motifsCounts = HashObjObjMaps.newMutableMap[Pattern,LongWritable]()

      /**
       * Generate all patterns with *numVertices* vertices
       */
      val startPatternGeneration = System.currentTimeMillis()
      var patterns = PatternUtils.singleVertexPatternSet()
      logInfo(s"PatternSet ${patterns}")
      for (i <- 0 until numVertices - 1) {
         patterns = PatternUtils.extendByVertex(patterns, 1)
         logInfo(s"PatternSetExtension[${i + 1}] ${patterns}")
      }
      val elapsedPatternGeneration = System.currentTimeMillis() -
         startPatternGeneration

      logInfo(s"CanonicalPatterns numVertices=${numVertices}" +
         s" numPatterns=${patterns.size()}" +
         s" elapsed=${elapsedPatternGeneration}")

      val cur = patterns.cursor()
      while (cur.moveNext()) {
         val patternWithoutPlan = cur.elem()

         /**
          * Motifs counting consider induced subgraphs, unlabeled at first
          */
         patternWithoutPlan.setInduced(true)
         patternWithoutPlan.setVertexLabeled(false)

         /**
          * Naive exploration plan
          */
         val pattern = PatternExplorationPlan.apply(patternWithoutPlan).get(0)

         pattern.updateSymmetryBreakerVertexUnlabeled()

         /**
          * Motifs counting map
          */
         val mapping = gquerying(pattern)
            .explore(numVertices - 1)
            .aggregationMap2[Pattern,LongWritable](
               (s,c,k) => {s.applyLabels(c.getPattern)},
               (s,c,v) => {v.set(1); v},
               (v1,v2) => {v1.set(v1.get() + v2.get()); v1}
            )

         motifsCounts.putAll(mapping.asJava)
      }

      motifsCounts

   }

   /**
    * All-cliques listing.
    * @return Fractoid with the initial state for cliques
    */
   def cliques: Fractoid[VertexInducedSubgraph] = {
      self.vfractoid.
         expand(1).
         filter { (e,c) =>
            e.numEdgesAdded == e.getNumVertices - 1
         }
   }

   /**
    * All-cliques listing implementing the efficient DAG structure from
    * [[https://dl.acm.org/citation.cfm?id=3186125]]
    * @return Fractoid with the initial state for cliques
    */
   def cliquesKClist: Fractoid[VertexInducedSubgraph] = {
      self.vfractoid.
         expand(1).
         set ("subgraph_enumerator",
            "br.ufmg.cs.systems.fractal.gmlib.clique.KClistEnumerator")
   }

   /**
    * All maximal cliques listing using the naive approach of enumerating
    * cliques and verifying whether they are maximal or not
    * @return new fractoid with the initial state for maximal cliques
    */
   def maximalCliquesPf(maxNumVertices: Int): Long = {
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


      var pattern = PatternUtils.singleVertexPattern()
      var totalNumMaximalCliques = 0L

      while (pattern.getNumberOfVertices <= maxNumVertices) {
         pattern.setVertexLabeled(false)
         val maximalCliquesRes = gquerying(pattern)
            .explore(pattern.getNumberOfVertices - 1)
            .filter(isMaximal)

         val numMaximalCliques = maximalCliquesRes.compute()("valid_subgraphs")

         totalNumMaximalCliques += numMaximalCliques
         val newEdges = (0 until pattern.getNumberOfVertices).toArray
         pattern = PatternUtils.addVertex(pattern, newEdges:_*)
      }

      totalNumMaximalCliques
   }

   /**
    * All maximal cliques listing implementing the parallel version of
    * Tomita's algorithm
    * [[https://dl.acm.org/doi/10.1145/3380936]]
    * @return new fractoid with the initial state for maximal cliques
    */
   def maximalCliques: Fractoid[VertexInducedSubgraph] = {
      self.vfractoid
         .expand(1)
         .set("subgraph_enumerator",
            "br.ufmg.cs.systems.fractal.gmlib.clique.MaximalCliquesEnumerator")
   }

   /**
    * Frequent subgraph mining using the "pattern-first" approach and the MNI
    * (Minimum Node Image) as support metric
    * @param supportThreshold Minimum support value
    * @param maxNumEdges Hard limit on the size of the interesting patterns
    * @return a map with frequent patterns and their current support
    */
   def fsmpf(supportThreshold: Int, maxNumEdges: Int)
   : ObjObjMap[Pattern, DomainSupport] = {
      var unlabeledPatterns: HashObjSet[Pattern] = null
      var lastInfrequentPatterns = HashObjSets.newMutableSet[Pattern]()
      val lastFrequentPatterns = HashObjObjMaps.newMutableMap[Pattern,DomainSupport]()
      val frequentPatterns = HashObjObjMaps.newMutableMap[Pattern,DomainSupport]()
      var numEdges = 0

      do {
         //logInfo(s"ExtendingPatterns ${frequentPatterns}")
         unlabeledPatterns = PatternUtils
            .extendByEdge(lastFrequentPatterns.keySet(), Int.MinValue)
         numEdges += 1
         lastFrequentPatterns.clear()

         //logInfo(s"PatternSetBeforeRemovingInfrequent " +
         //   s"size=${unlabeledPatterns.size()} ${unlabeledPatterns}")

         if (!lastInfrequentPatterns.isEmpty) {
            //logInfo(s"InfrequentPatternsBeforeExtension " +
            //   s"size=${infrequentPatterns.size()} ${infrequentPatterns}")
            lastInfrequentPatterns = PatternUtils
               .extendByEdge(lastInfrequentPatterns, Int.MinValue)
            //logInfo(s"InfrequentPatternsAfterExtension " +
            //   s"size=${infrequentPatterns.size()} ${infrequentPatterns}")
            unlabeledPatterns.removeAll(lastInfrequentPatterns)
            lastInfrequentPatterns.clear()
         }

         //logInfo(s"PatternSetAfterRemovingInfrequent" +
         //   s" size=${unlabeledPatterns.size()} ${unlabeledPatterns}")

         val patternsCur = unlabeledPatterns.cursor()
         while (patternsCur.moveNext()) {
            val patternWithoutPlan = patternsCur.elem()
            if (patternWithoutPlan.getNumberOfEdges == 1) {
               patternWithoutPlan.setVertexLabeled(false)
            } else {
               patternWithoutPlan.setVertexLabeled(true)
            }
            val pattern = PatternExplorationPlan
               .apply(patternWithoutPlan)
               .get(0)
            if (pattern.getNumberOfEdges > 1
               && pattern.getConfig.getMainGraph[MainGraph[_,_]]
               .vertexLabel(pattern.getVertices.getLast) == Int.MinValue) {
               pattern.explorationPlan().setVertexPredicate(
                  VertexPredicate.trueVertexPredicate,
                  pattern.getNumberOfVertices - 1)
            }
            pattern.updateSymmetryBreakerVertexUnlabeled()
            val minSupport = supportThreshold
            val gqueryingRes = gquerying(pattern).
               explore(pattern.getNumberOfVertices - 1).
               aggregate [Pattern, DomainSupport] (
                  "support",
                  (s,c,k) => {s.applyLabels(c.getPattern)},
                  (s,c,v) => {v.setSupport(minSupport); v.setSubgraph(s); v},
                  (v1,v2) => {v1.aggregate(v2); v1}
               )

            val patternSupport = gqueryingRes
               .aggregationMap[Pattern, DomainSupport]("support")

            for ((labeledPattern, support) <- patternSupport) {
               val existingSupport = lastFrequentPatterns.get(labeledPattern)
               if (existingSupport != null) {
                  existingSupport.aggregate(support)
               } else {
                  lastFrequentPatterns.put(labeledPattern, support)
               }
            }
         }

         // filter infrequent patterns
         val candidatesIter = lastFrequentPatterns.cursor()
         while (candidatesIter.moveNext()) {
            val labeledPattern = candidatesIter.key()
            val support = candidatesIter.value()
            if (!support.hasEnoughSupport) {
               candidatesIter.remove()
               lastInfrequentPatterns.add(labeledPattern)
            } else {
               frequentPatterns.put(labeledPattern, support)
            }
         }

      } while (!lastFrequentPatterns.isEmpty && numEdges < maxNumEdges)

      frequentPatterns
   }

   /**
    * Frequent subgraph mining using the "pattern-first" approach and the MNI
    * (Minimum Node Image) as support metric and using the optimized MCVC
    * approach for pattern matching
    * @param supportThreshold Minimum support value
    * @param maxNumEdges Hard limit on the size of the interesting patterns
    * @return a map with frequent patterns and their current support
    */
   def fsmpf2(supportThreshold: Int, maxNumEdges: Int)
   : ObjObjMap[Pattern, DomainSupport] = {
      val frequentLabels = HashIntSets.newMutableSet()
      val lastInfrequentPatterns = HashObjSets.newMutableSet[Pattern]()
      val lastFrequentPatterns = HashObjObjMaps.newMutableMap[Pattern,DomainSupport]()
      val frequentPatterns = HashObjObjMaps.newMutableMap[Pattern,DomainSupport]()
      var numEdges = 0
      val minSupport = supportThreshold

      {
         val start = System.currentTimeMillis()
         val singleEdgePattern = PatternUtils.singleEdgePattern()
         singleEdgePattern.setVertexLabeled(false)
         val edgesSupports = gquerying(singleEdgePattern)
            .explore(singleEdgePattern.getNumberOfVertices - 1)
            .aggregationMap2[Pattern,DomainSupport](
               (s,c,k) => s.applyLabels(c.getPattern),
               (s,c,v) => {v.setSupport(minSupport); v.setSubgraph(s); v},
               (v1,v2) => {v1.aggregate(v2); v1}
            )

         for ((p,s) <- edgesSupports) {
            if (s.hasEnoughSupport) {
               val pedge = p.getEdges.get(0)
               frequentLabels.add(pedge.getSrcLabel)
               frequentLabels.add(pedge.getDestLabel)
               lastFrequentPatterns.put(p, s)
               frequentPatterns.put(p, s)
            } else {
               lastInfrequentPatterns.add(p)
            }
         }
         val elapsed = System.currentTimeMillis() - start
         logInfo(s"FrequentLabels ${elapsed}ms ${frequentLabels}")
      }

      numEdges = 1

      while (!lastFrequentPatterns.isEmpty && numEdges < maxNumEdges) {
         val candPatterns = HashObjSets.newMutableSet[Pattern]()

         {
            val start = System.currentTimeMillis()
            val frequentLabelsCur = frequentLabels.cursor()
            while (frequentLabelsCur.moveNext()) {
               candPatterns.addAll(
                  PatternUtils.extendByEdge(lastFrequentPatterns.keySet(),
                     frequentLabelsCur.elem()))
            }
            lastFrequentPatterns.clear()
            val elapsed = System.currentTimeMillis() - start
            logInfo(s"AddingFrequentExtensions ${elapsed}ms" +
               s" candidatesSize=${candPatterns.size()}")
         }

         {
            val start = System.currentTimeMillis()
            if (!lastInfrequentPatterns.isEmpty) {
               val frequentLabelsCur = frequentLabels.cursor()
               while (frequentLabelsCur.moveNext()) {
                  candPatterns.removeAll(
                     PatternUtils.extendByEdge(lastInfrequentPatterns,
                        frequentLabelsCur.elem())
                  )
               }
               lastInfrequentPatterns.clear()
            }
            val elapsed = System.currentTimeMillis() - start
            logInfo(s"RemovingInfrequentExtensions ${elapsed}ms" +
               s" candidatesSize=${candPatterns.size()}")
         }

         val patternsCur = candPatterns.cursor()
         while (patternsCur.moveNext()) {
            val patternWithoutPlan = patternsCur.elem()
            patternWithoutPlan.setVertexLabeled(true)

            val newPatterns = PatternExplorationPlan.apply(patternWithoutPlan)
            val newPatternsCur = newPatterns.cursor()

            while (newPatternsCur.moveNext()) {
               val pattern = newPatternsCur.elem()

               val patternSupport = gquerying(pattern)
                  .explore(pattern.getNumberOfVertices - 1)
                  .aggregationMap2[Pattern,DomainSupport](
                     (s,c,k) => s.applyLabels(c.getPattern),
                     (s,c,v) => {v.setSupport(minSupport); v.setSubgraph(s); v},
                     (v1,v2) => {v1.aggregate(v2); v1}
                  )

               for ((p,s) <- patternSupport) {
                  if (s.hasEnoughSupport) {
                     lastFrequentPatterns.put(p, s)
                     frequentPatterns.put(p, s)
                  } else {
                     lastInfrequentPatterns.add(p)
                  }
               }
            }
         }
         numEdges += 1
      }

      frequentPatterns
   }

   /**
    * Frequent subgraph mining using the "pattern-first" approach and the MNI
    * (Minimum Node Image) as support metric and using the optimized MCVC
    * approach for pattern matching
    * @param supportThreshold Minimum support value
    * @param maxNumEdges Hard limit on the size of the interesting patterns
    * @return a map with frequent patterns and their current support
    */
   def fsmpfmcvc2(supportThreshold: Int, maxNumEdges: Int)
   : ObjObjMap[Pattern, DomainSupport] = {
      val frequentLabels = HashIntSets.newMutableSet()
      val lastInfrequentPatterns = HashObjSets.newMutableSet[Pattern]()
      val lastFrequentPatterns = HashObjObjMaps.newMutableMap[Pattern,DomainSupport]()
      val frequentPatterns = HashObjObjMaps.newMutableMap[Pattern,DomainSupport]()
      var numEdges = 0
      val FSM = "fsm"
      val minSupport = supportThreshold

      val matchCallback = new SubgraphCallback[PatternInducedSubgraph] {
         private var aggregationStorage: AggregationStorage[Pattern,DomainSupport] = _
         private var reusableValue: DomainSupport = _

         override def apply(s: PatternInducedSubgraph, c: Computation[PatternInducedSubgraph])
         : Unit = {
            reusableValue.setSupport(minSupport)
            reusableValue.setSubgraph(s)
            aggregationStorage.aggregateWithReusables(
               s.applyLabels(c.getPattern),
               reusableValue
            )
         }

         override def init(computation: Computation[PatternInducedSubgraph]): Unit = {
            val engine = computation.getExecutionEngine
            if (engine != null) {
               aggregationStorage = engine.getAggregationStorage(FSM)
               reusableValue = aggregationStorage.reusableValue()
            }
         }
      }

      val callback = new SubgraphCallback[PatternInducedSubgraph] {
         val callback = matchCallback
         override def apply(s: PatternInducedSubgraph,
                            c: Computation[PatternInducedSubgraph]): Unit = {
            s.completeMatch(c, c.getPattern, callback)
         }

         override def init(c: Computation[PatternInducedSubgraph]): Unit = {
            callback.init(c)
         }
      }

      {
         val start = System.currentTimeMillis()
         val singleEdgePattern = PatternUtils.singleEdgePattern()
         singleEdgePattern.setVertexLabeled(false)
         val edgesSupports = gquerying(singleEdgePattern)
            // not explored because of MCVC partial match: 1 vertex in this case
            .aggregationMapWithCallback[Pattern,DomainSupport](
               FSM,
               callback,
               (v1,v2) => {v1.aggregate(v2); v1}
            )

         for ((p,s) <- edgesSupports) {
            if (s.hasEnoughSupport) {
               val pedge = p.getEdges.get(0)
               frequentLabels.add(pedge.getSrcLabel)
               frequentLabels.add(pedge.getDestLabel)
               lastFrequentPatterns.put(p, s)
               frequentPatterns.put(p, s)
            } else {
               lastInfrequentPatterns.add(p)
            }
         }
         val elapsed = System.currentTimeMillis() - start
         logInfo(s"FrequentLabels ${elapsed}ms ${frequentLabels}")
      }

      numEdges = 1

      while (!lastFrequentPatterns.isEmpty && numEdges < maxNumEdges) {
         val candPatterns = HashObjSets.newMutableSet[Pattern]()

         {
            val start = System.currentTimeMillis()
            val frequentLabelsCur = frequentLabels.cursor()
            while (frequentLabelsCur.moveNext()) {
               candPatterns.addAll(
                  PatternUtils.extendByEdge(lastFrequentPatterns.keySet(),
                     frequentLabelsCur.elem()))
            }
            lastFrequentPatterns.clear()
            val elapsed = System.currentTimeMillis() - start
            logInfo(s"AddingFrequentExtensions ${elapsed}ms" +
               s" candidatesSize=${candPatterns.size()}")
         }

         {
            val start = System.currentTimeMillis()
            if (!lastInfrequentPatterns.isEmpty) {
               val frequentLabelsCur = frequentLabels.cursor()
               while (frequentLabelsCur.moveNext()) {
                  candPatterns.removeAll(
                     PatternUtils.extendByEdge(lastInfrequentPatterns,
                        frequentLabelsCur.elem())
                  )
               }
               lastInfrequentPatterns.clear()
            }
            val elapsed = System.currentTimeMillis() - start
            logInfo(s"RemovingInfrequentExtensions ${elapsed}ms" +
               s" candidatesSize=${candPatterns.size()}")
         }

         val patternsCur = candPatterns.cursor()
         while (patternsCur.moveNext()) {
            val patternWithoutPlan = patternsCur.elem()
            patternWithoutPlan.setVertexLabeled(true)

            val newPatterns = PatternExplorationPlanMCVC
               .apply(patternWithoutPlan)
            val newPatternsCur = newPatterns.cursor()

            while (newPatternsCur.moveNext()) {
               val pattern = newPatternsCur.elem()

               val explorationPlanMCVC = pattern.explorationPlan()
               val mcvcSize = explorationPlanMCVC.mcvcSize()

               val patternSupport = gquerying(pattern)
                  .explore(mcvcSize - 1)
                  .aggregationMapWithCallback[Pattern,DomainSupport](
                     FSM,
                     callback,
                     (v1,v2) => {v1.aggregate(v2); v1}
                  )

               for ((p,s) <- patternSupport) {
                  if (s.hasEnoughSupport) {
                     lastFrequentPatterns.put(p, s)
                     frequentPatterns.put(p, s)
                  } else {
                     lastInfrequentPatterns.add(p)
                  }
               }
            }
         }
         numEdges += 1
      }

      frequentPatterns
   }

   /**
    * Frequent subgraph mining using the "pattern-first" approach and the MNI
    * (Minimum Node Image) as support metric and using the optimized MCVC
    * approach for pattern matching
    * @param supportThreshold Minimum support value
    * @param maxNumEdges Hard limit on the size of the interesting patterns
    * @return a map with frequent patterns and their current support
    */
   def fsmpfmcvc(supportThreshold: Int, maxNumEdges: Int)
   : ObjObjMap[Pattern, DomainSupport] = {
      var unlabeledPatterns: HashObjSet[Pattern] = null
      var lastInfrequentPatterns = HashObjSets.newMutableSet[Pattern]()
      val lastFrequentPatterns = HashObjObjMaps.newMutableMap[Pattern,DomainSupport]()
      val frequentPatterns = HashObjObjMaps.newMutableMap[Pattern,DomainSupport]()
      val labeledToUnlabeleds = HashObjObjMaps.newMutableMap[Pattern,
         HashObjSet[Pattern]]()
      var numEdges = 0
      val FSM = "fsm"
      val minSupport = supportThreshold

      val matchCallback = new SubgraphCallback[PatternInducedSubgraph] {
         private var aggregationStorage: AggregationStorage[Pattern,DomainSupport] = _
         private var reusableValue: DomainSupport = _

         override def apply(s: PatternInducedSubgraph, c: Computation[PatternInducedSubgraph])
         : Unit = {
            reusableValue.setSupport(minSupport)
            reusableValue.setSubgraph(s)
            aggregationStorage.aggregateWithReusables(
               s.applyLabels(c.getPattern),
               reusableValue
            )
         }

         override def init(computation: Computation[PatternInducedSubgraph]): Unit = {
            val engine = computation.getExecutionEngine
            if (engine != null) {
               aggregationStorage = engine.getAggregationStorage(FSM)
               reusableValue = aggregationStorage.reusableValue()
            }
         }
      }

      val callback = new SubgraphCallback[PatternInducedSubgraph] {
         val callback = matchCallback
         override def apply(s: PatternInducedSubgraph,
                            c: Computation[PatternInducedSubgraph]): Unit = {
            s.completeMatch(c, c.getPattern, callback)
         }

         override def init(c: Computation[PatternInducedSubgraph]): Unit = {
            callback.init(c)
         }
      }

      do {
         logInfo(s"ExtendingPatterns ${lastFrequentPatterns}")
         unlabeledPatterns = PatternUtils
            .extendByEdge(lastFrequentPatterns.keySet(), Int.MinValue)
         numEdges += 1
         lastFrequentPatterns.clear()

         logInfo(s"PatternSetBeforeRemovingInfrequent " +
            s"size=${unlabeledPatterns.size()} ${unlabeledPatterns}")

         if (!lastInfrequentPatterns.isEmpty) {
            logInfo(s"InfrequentPatternsBeforeExtension " +
               s"size=${lastInfrequentPatterns.size()} ${lastInfrequentPatterns}")
            lastInfrequentPatterns = PatternUtils
               .extendByEdge(lastInfrequentPatterns, Int.MinValue)
            logInfo(s"InfrequentPatternsAfterExtension " +
               s"size=${lastInfrequentPatterns.size()} ${lastInfrequentPatterns}")
            unlabeledPatterns.removeAll(lastInfrequentPatterns)
            lastInfrequentPatterns.clear()
         }

         logInfo(s"PatternSetAfterRemovingInfrequent" +
            s" size=${unlabeledPatterns.size()} ${unlabeledPatterns}")

         val patternsCur = unlabeledPatterns.cursor()
         while (patternsCur.moveNext()) {
            val patternWithoutPlan = patternsCur.elem()
            if (patternWithoutPlan.getNumberOfEdges == 1) {
               patternWithoutPlan.setVertexLabeled(false)
            } else {
               patternWithoutPlan.setVertexLabeled(true)
            }

            val newPatterns = PatternExplorationPlanMCVC
               .apply(patternWithoutPlan)
            val newPatternsCur = newPatterns.cursor()

            while (newPatternsCur.moveNext()) {
               val pattern = newPatternsCur.elem()

               if (pattern.getNumberOfEdges > 1
                  && pattern.getConfig.getMainGraph[MainGraph[_,_]]
                  .vertexLabel(pattern.getVertices.getLast) == Int.MinValue) {
                  pattern.explorationPlan().setVertexPredicate(
                     VertexPredicate.trueVertexPredicate,
                     pattern.getNumberOfVertices - 1)
               }

               // TODO: this may cause issues in case we use several
               //  orderings in the exploration plan generator
               pattern.updateSymmetryBreakerVertexUnlabeled()

               val explorationPlanMCVC = pattern.explorationPlan()
               val mcvcSize = explorationPlanMCVC.mcvcSize()

               val patternSupport = gquerying(pattern)
                  .explore(mcvcSize - 1)
                  .aggregationMapWithCallback[Pattern,DomainSupport](
                     FSM,
                     callback,
                     (v1,v2) => {v1.aggregate(v2); v1}
                  )

               for ((labeledPattern,support) <- patternSupport) {
                  val unlabeleds = labeledToUnlabeleds
                     .getOrDefault(labeledPattern,
                        HashObjSets.newMutableSet[Pattern]())
                  unlabeleds.add(pattern)
                  labeledToUnlabeleds.putIfAbsent(labeledPattern, unlabeleds)
                  val existingSupport = lastFrequentPatterns.get(labeledPattern)
                  if (existingSupport != null) {
                     existingSupport.aggregate(support)
                  } else {
                     lastFrequentPatterns.put(labeledPattern, support)
                  }
               }
            }
         }

         // filter infrequent patterns
         val candidatesCur = lastFrequentPatterns.cursor()
         while (candidatesCur.moveNext()) {
            val labeledPattern = candidatesCur.key()
            val support = candidatesCur.value()
            if (!support.hasEnoughSupport) {
               candidatesCur.remove()
               lastInfrequentPatterns.add(labeledPattern)
               val unlabeledsCur = labeledToUnlabeleds.get(labeledPattern).cursor()
               while (unlabeledsCur.moveNext()) {
                  logInfo(s"InfrequentPatternPartial ${labeledPattern} " +
                     s"${unlabeledsCur.elem()}" +
                     s" ${support}")
               }
            } else {
               frequentPatterns.put(labeledPattern, support)
               val unlabeledsCur = labeledToUnlabeleds.get(labeledPattern).cursor()
               while (unlabeledsCur.moveNext()) {
                  logInfo(s"FrequentPatternPartial ${labeledPattern} " +
                     s"${unlabeledsCur.elem()}" +
                     s" ${support}")
               }
            }
         }

      } while (!lastFrequentPatterns.isEmpty && numEdges < maxNumEdges)

      frequentPatterns
   }

   /**
    * Frequent subgraph Mining (FSM)
    * @param support threshold to determine what is frequent according to
    *                the (minimum image)
    * @param numSteps maximum number of exploration steps
    * @return Fractoid with the initial state for FSM
    */
   def fsm(support: Int, numSteps: Int): ObjObjMap[Pattern,DomainSupport] = {
      val frequentPatternsSupports = HashObjObjMaps
         .newMutableMap[Pattern,DomainSupport]()
      val quickPatterns = new ObjSet[Pattern]()
      val sc = self.fractalContext.sparkContext

      def filterInfrequents
      (aggStorage: PatternAggregationStorage[Pattern,DomainSupport]): Unit = {
         val patternSupports = aggStorage.getMapping
         val iter = aggStorage.getQuickToCanonicalMapping.entrySet().iterator()
         while (iter.hasNext) {
            val e = iter.next()
            val (quick, cann) = (e.getKey, e.getValue)
            val supp = patternSupports.get(cann)
            if (supp.hasEnoughSupport) {
               quickPatterns.add(quick)
               frequentPatternsSupports.put(cann, supp)
            }
         }
      }

      val bootstrap = self.efractoid.expand(1)

      var iteration = 0
      var freqFrac = bootstrap
      var freqPatts = bootstrap
         .aggregationStorage2[Pattern,DomainSupport](
            (s,c,k) => {s.quickPattern},
            (s,c,v) => {v.setSupport(support); v.setSubgraph(s); v},
            (v1,v2) => {v1.aggregate(v2); v1}
         )
         .asInstanceOf[PatternAggregationStorage[Pattern,DomainSupport]]

      filterInfrequents(freqPatts)

      //logInfo(s"QuickPatterns ${quickPatterns}")
      //val cur = frequentPatternsSupports.cursor()
      //while (cur.moveNext()) {
      //   logInfo(s"FrequentPatternPartial iteration=${iteration}" +
      //      s" ${cur.key()} ${cur.value()}")
      //}

      var remainingSteps = numSteps
      var continue = freqPatts.getNumberMappings > 0 && remainingSteps > 0

      while (continue) {
         iteration += 1
         val quickPatternsBc = sc.broadcast(quickPatterns)
         freqFrac = freqFrac
            .filter((s,c) => quickPatternsBc.value.contains(s.quickPattern))
            .expand(1)

         freqPatts = freqFrac
            .aggregationStorage2[Pattern,DomainSupport](
               (s,c,k) => {s.quickPattern},
               (s,c,v) => {v.setSupport(support); v.setSubgraph(s); v},
               (v1,v2) => {v1.aggregate(v2); v1}
            )
            .asInstanceOf[PatternAggregationStorage[Pattern,DomainSupport]]

         filterInfrequents(freqPatts)

         //logInfo(s"QuickPatterns ${quickPatterns}")
         //val cur = frequentPatternsSupports.cursor()
         //while (cur.moveNext()) {
         //   logInfo(s"FrequentPatternPartial iteration=${iteration}" +
         //      s" ${cur.key()} ${cur.value()}")
         //}

         remainingSteps -= 1
         continue = freqPatts.getNumberMappings > 0 && remainingSteps > 0
      }

      frequentPatternsSupports
   }

   /**
    * Frequent subgraph Mining (FSM)
    * @param support threshold to determine what is frequent according to
    *                the (minimum image)
    * @param numSteps maximum number of exploration steps
    * @return Fractoid with the initial state for FSM
    */
   def fsm2(support: Int, numSteps: Int): ObjObjMap[Pattern,DomainSupport] = {
      val frequentPatternsSupports = HashObjObjMaps
         .newMutableMap[Pattern,DomainSupport]()
      val quickPatterns = new ObjSet[Pattern]()
      val sc = self.fractalContext.sparkContext

      def canonicalPatternMap(quickPatternRDD: RDD[(Pattern,DomainSupport)])
      : Map[Pattern,(ObjSet[Pattern],DomainSupport)] = {
         quickPatternRDD
            .map { case (quickPatern,supp) =>
               val canonicalPattern = quickPatern.copy()
               canonicalPattern.turnCanonical()
               supp.handleConversionFromQuickToCanonical(quickPatern, canonicalPattern)
               val quickPatterns = new ObjSet[Pattern]()
               quickPatterns.add(quickPatern)
               (canonicalPattern, (quickPatterns,supp))
            }
            .reduceByKey { case ((p1, s1), (p2, s2)) =>
               p1.addAll(p2)
               s1.aggregate(s2)
               (p1, s1)
            }
            .collectAsMap()
            .asJava
      }

      def filterInfrequents
      (patternMap: Map[Pattern,(ObjSet[Pattern],DomainSupport)]): Unit = {
         val iter = patternMap.entrySet().iterator()
         while (iter.hasNext) {
            val e = iter.next()
            val (cann, (quicks, supp)) = (e.getKey, e.getValue)
            if (supp.hasEnoughSupport) {
               quickPatterns.addAll(quicks)
               frequentPatternsSupports.put(cann, supp)
            } else {
               iter.remove()
            }
         }
      }

      val bootstrap = self.efractoid.expand(1)

      var iteration = 0
      var freqFrac = bootstrap
      var quickPatternMapRDD = bootstrap
         .aggregationObjObj[Pattern,DomainSupport](
            s => s.quickPattern,
            s => {
               val supp = new DomainSupport()
               supp.setSubgraph(s)
               supp.setSupport(support)
               supp
            },
            (v1,v2) => v1.aggregate(v2)
         )

      var freqPatts = canonicalPatternMap(quickPatternMapRDD)

      filterInfrequents(freqPatts)

      //logInfo(s"QuickPatterns ${quickPatterns}")
      //val cur = frequentPatternsSupports.cursor()
      //while (cur.moveNext()) {
      //   logInfo(s"FrequentPatternPartial iteration=${iteration}" +
      //      s" ${cur.key()} ${cur.value()}")
      //}

      var remainingSteps = numSteps
      var continue = freqPatts.size() > 0 && remainingSteps > 0

      while (continue) {
         iteration += 1
         val quickPatternsBc = sc.broadcast(quickPatterns)
         freqFrac = freqFrac
            .filter((s,c) => quickPatternsBc.value.contains(s.quickPattern))
            .expand(1)

         quickPatternMapRDD = freqFrac
            .aggregationObjObj[Pattern,DomainSupport](
               s => s.quickPattern,
               s => {
                  val ds = new DomainSupport()
                  ds.setSubgraph(s)
                  ds.setSupport(support)
                  ds
               },
               (v1,v2) => v1.aggregate(v2)
            )

         freqPatts = canonicalPatternMap(quickPatternMapRDD)

         filterInfrequents(freqPatts)

         //logInfo(s"QuickPatterns ${quickPatterns}")
         //val cur = frequentPatternsSupports.cursor()
         //while (cur.moveNext()) {
         //   logInfo(s"FrequentPatternPartial iteration=${iteration}" +
         //      s" ${cur.key()} ${cur.value()}")
         //}

         remainingSteps -= 1
         continue = freqPatts.size > 0 && remainingSteps > 0
      }

      frequentPatternsSupports
   }

   /**
    * Match pattern using Minimum Connected Vertex Cover (MCVC) strategy
    * TODO: these fractoids returned use a simple callback as last process
    * steps and thus, it is not safe to grow this workflow
    * @param pattern representing a query
    * @return an array of fractoids with partial query results
    */
   def gqueryingmcvc(pattern: Pattern,
                     callback: SubgraphCallback[PatternInducedSubgraph] =
                     SubgraphCallback.defaultPatternInducedCallback)
   : Array[Fractoid[PatternInducedSubgraph]] = {
      logInfo(s"PatternBeforePlan ${pattern}")

      val newPatterns = PatternExplorationPlanMCVC.apply(pattern)
      val partialResults = new Array[Fractoid[PatternInducedSubgraph]](newPatterns.size())

      var i = 0
      while (i < newPatterns.size()) {
         val nextPattern = newPatterns.getu(i)
         val explorationPlanMCVC = nextPattern.explorationPlan()
         val mcvcSize = explorationPlanMCVC.mcvcSize()

         val gquerying = this.gquerying(nextPattern)
            .explore(mcvcSize - 1)
            .process((s,c) => s.completeMatch(c, c.getPattern, callback))

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
   def gquerying(pattern: Pattern): Fractoid[PatternInducedSubgraph] = {
      logInfo (s"Querying pattern ${pattern} in ${this}.")
      self.pfractoid(pattern).expand(1)
   }

   /**
    * Subgraph querying (pattern matching) - sampling version
    * @param pattern representing the subgraphs structure
    * @return new fractoid
    */
   def gquerying(pattern: Pattern, fraction: Double): Fractoid[PatternInducedSubgraph] = {
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
      // *minDensity* be reached later on
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

   @Experimental
   def keywordSearch(
                       numPartitions: Int,
                       keywords: Array[String]): Fractoid[EdgeInducedSubgraph] = {
      import java.util.function.IntConsumer

      import br.ufmg.cs.systems.fractal.util.collection._
      import com.koloboke.collect.ObjCursor
      import com.koloboke.collect.set.hash.HashObjSet
      import org.apache.hadoop.io._

      import scala.collection.mutable.Map

      logInfo (s"KeywordSearch keywords=${keywords.mkString(",")}")

      val INVERTED_INDEX = "inverted_index"
      val PREDICATE_INDEX = "predicate_index"
      val VALID_VERTICES = "valid_vertices"

      var start = 0L
      var elapsed = 0L

      /**
       * KeywordSearchFirstPass: construct intermediate structures from triples,
       * i.e. from edges
       */

      start = System.currentTimeMillis()

      val docIterator = (e: EdgeInducedSubgraph,
                         c: Computation[EdgeInducedSubgraph]) => {
         val vertices = e.getVertices()
         val edges = e.getEdges()
         new Iterator[String] {
            private var index = 0
            private var cur: ObjCursor[String] = _
            private val curs: Array[ObjCursor[String]] = {
               val _curs = new Array[ObjCursor[String]](
                  vertices.size() + edges.size())

               // get vertex properties
               var i = 0
               while (i < vertices.size()) {
                  val prop = e.vertex[HashObjSet[String]](
                     vertices.getu(i)).getProperty()
                  if (prop != null) {
                     _curs(i) = prop.cursor()
                  }
                  i += 1
               }

               // get edge properties
               var j = 0
               while (i < _curs.length) {
                  val prop = e.edge[HashObjSet[String]](
                     edges.getu(j)).getProperty()
                  if (prop != null) {
                     _curs(i) = prop.cursor()
                  }
                  j += 1
                  i += 1
               }

               _curs
            }

            def hasNext(): Boolean = {
               while (index < curs.length) {
                  cur = curs(index)
                  if (cur != null && cur.moveNext()) {
                     return true
                  }
                  index += 1
               }
               false
            }

            def next(): String = {
               cur.elem()
            }
         }
      }

      val idxRes = self.efractoid.
         expand(1).
         set ("num_partitions", numPartitions).
         set ("input_graph_class", "br.ufmg.cs.systems.fractal.gmlib.keywordsearch.KeywordSearchGraph").
         set ("edge_labelled", true).
         aggregateAll [Text,InvertedIndexMap] (
            INVERTED_INDEX,
            (e: EdgeInducedSubgraph, c: Computation[EdgeInducedSubgraph]) => {
               val reusableTuple = (new Text(), new InvertedIndexMap())
               val singleEdge = e.getEdges().getu(0)
               docIterator(e,c).filter (w => keywords.contains(w)).map { word =>
                  reusableTuple._1.set(word)
                  reusableTuple._2.clear()
                  reusableTuple._2.appendDoc(singleEdge, 1)
                  reusableTuple
               }
            },
            (ii1: InvertedIndexMap, ii2: InvertedIndexMap) => {ii1.merge(ii2); ii1}
         ).
         aggregateAll [Text,InvertedIndexMap] (
            PREDICATE_INDEX,
            (e: EdgeInducedSubgraph, c: Computation[EdgeInducedSubgraph]) => {
               val reusableTuple = (new Text(), new InvertedIndexMap())
               val singleEdge = e.getEdges().getu(0)
               val predicate = e.labelledEdge(singleEdge).getEdgeLabel()
               docIterator(e,c).map { word =>
                  reusableTuple._1.set(word)
                  reusableTuple._2.clear()
                  reusableTuple._2.appendDoc(predicate, 1)
                  reusableTuple
               }
            },
            (ii1: InvertedIndexMap, ii2: InvertedIndexMap) => {ii1.merge(ii2); ii1}
         ).
         aggregateAll [Text,IntSet] (
            VALID_VERTICES,
            (e: EdgeInducedSubgraph, c: Computation[EdgeInducedSubgraph]) => {
               if (!docIterator(e,c).filter (w => keywords.contains(w)).isEmpty) {
                  val reusableTuple = (new Text(VALID_VERTICES), new IntSet())
                  val edgeId = e.getEdges().getu(0)
                  val edge = e.labelledEdge(edgeId)
                  val vertices = Iterator(edge.getSourceId(), edge.getDestinationId())
                  vertices.map { v =>
                     reusableTuple._2.clear()
                     reusableTuple._2.add(v)
                     reusableTuple
                  }
               } else {
                  Iterator.empty
               }
            },
            (s1: IntSet, s2: IntSet) => {s1.union(s2); s1}
         )

      // mapping from words to their respective inverted indexes
      val wordToIdx = idxRes.
         aggregationMap[Text,InvertedIndexMap](INVERTED_INDEX).toArray

      // mapping from words to their respective predicate
      val wordToPredicate = idxRes.
         aggregationMap[Text,InvertedIndexMap](PREDICATE_INDEX).toArray

      // valid vertices
      val validVertexIds = idxRes.aggregationMap[Text,IntSet](
         VALID_VERTICES)(new Text(VALID_VERTICES))

      elapsed = System.currentTimeMillis() - start

      logInfo (s"KeywordSearchFirstPass" +
         s" distinctWords=${wordToPredicate.length} took ${elapsed} ms")

      /**
       * Local aggregations
       */

      start = System.currentTimeMillis()

      // consumer to select only relevant edge ids
      val validEdgeIds = new IntSet()
      val consumerIdx = new IntConsumer {
         def accept(e: Int): Unit = {
            validEdgeIds.add(e)
         }
      }

      // consumer to select only relevant edge labels
      val validEdgeLabels = new IntSet()
      val consumerLabel = new IntConsumer {
         def accept(e: Int): Unit = {
            validEdgeLabels.add(e)
         }
      }

      var totalFreq = 0L
      val keywordToIndex = Map.empty[String,Int]
      val totalInvPredicate = new InvertedIndexMap()
      val invPredicates = new Array[InvertedIndexMap](wordToPredicate.length)
      var i = 0
      while (i < wordToPredicate.length) {
         val (pword, invPredicate) = wordToPredicate(i)
         val wordStr = pword.toString()
         if (keywords.contains(wordStr)) {
            keywordToIndex.update (wordStr, i)
         }
         invPredicate.forEachDoc(consumerLabel)
         totalInvPredicate.merge(invPredicate)
         invPredicates(i) = invPredicate
         totalFreq += invPredicate.getTotalFreq()
         i += 1
      }

      val keywordIndex = new Array[Int](wordToIdx.length)
      val totalInvIdx = new InvertedIndexMap()
      val invIdxs = new Array[InvertedIndexMap](wordToIdx.length)
      val invPredicates2 = new Array[InvertedIndexMap](wordToIdx.length)
      i = 0
      while (i < wordToIdx.length) {
         val (word, invIdx) = wordToIdx(i)
         val idx = keywordToIndex(word.toString())
         keywordIndex(i) = idx
         invPredicates2(i) = invPredicates(idx)
         invIdx.forEachDoc(consumerIdx)
         totalInvIdx.merge(invIdx)
         invIdxs(i) = invIdx
         i += 1
      }

      elapsed = System.currentTimeMillis() - start

      logInfo (s"KeywordSearchLocalAggregation validEdgeIds=${validEdgeIds}" +
         s" validVertexIds=${validVertexIds}" +
         s" totalFreq=${totalFreq} totalInvIdx=${totalInvIdx}" +
         s" totalInvPredicate=${totalInvPredicate}" +
         s" took ${elapsed} ms")

      /**
       * KeywordSearchSecondPass: actual enumeration of subgraphs
       */

      start = System.currentTimeMillis()

      val fc = self.fractalContext
      val validEdgeIdsBc = fc.sparkContext.broadcast(validEdgeIds)
      val validVertexIdsBc = fc.sparkContext.broadcast(validVertexIds)
      val invIdxsBc = fc.sparkContext.broadcast(invIdxs)

      val lastWordIsValid = (e: EdgeInducedSubgraph,
                             c: Computation[EdgeInducedSubgraph]) => {
         val words = e.getWords()
         val numWords = words.size()
         val lastWord = words.getLast()
         val invIdxs = invIdxsBc.value
         var valid = false

         if (validEdgeIdsBc.value.contains(lastWord)) {
            var i = 0
            while (i < invIdxs.length) {
               val ii = invIdxs(i)
               if (ii.containsDoc(lastWord)) {
                  var j = 0
                  while (j < numWords - 1 && !ii.containsDoc(words.get(j))) {
                     j += 1
                  }
                  if (j == numWords - 1) {
                     valid = true
                     i = invIdxs.length - 1
                  }
               }
               i += 1
            }
         }

         valid
      }

      // filtered input graph
      var kws = self.efractoid.
         efilter [HashObjSet[String]] (e => validEdgeIdsBc.value.contains(e.getEdgeId())).
         set ("num_partitions", numPartitions).
         set ("input_graph_class", "br.ufmg.cs.systems.fractal.gmlib.keywordsearch.KeywordSearchGraph").
         set ("edge_labelled", true).
         set ("keep_maximal", true)

      for (i <- 0 until keywords.size) {
         kws = kws.expand(1).filter(lastWordIsValid)
      }

      kws
   }
}
