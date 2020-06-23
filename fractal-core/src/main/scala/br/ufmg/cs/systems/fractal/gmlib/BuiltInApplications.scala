package br.ufmg.cs.systems.fractal.gmlib

import br.ufmg.cs.systems.fractal.aggregation.AggregationStorage
import br.ufmg.cs.systems.fractal.annotation.Experimental
import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.fsm.MinImageSupport
import br.ufmg.cs.systems.fractal.gmlib.periodic.{InducedPeriodicSubgraphsPF, InducedPeriodicSubgraphsPFMCVC, InducedPeriodicSubgraphsSF}
import br.ufmg.cs.systems.fractal.graph.MainGraph
import br.ufmg.cs.systems.fractal.pattern.{Pattern, PatternExplorationPlan, PatternExplorationPlanMCVC, PatternUtils}
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, PatternInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.collection.{ObjArrayList, ObjSet}
import br.ufmg.cs.systems.fractal.util.pool.{IntArrayListPool, IntArrayListViewPool}
import br.ufmg.cs.systems.fractal.util.{Logging, SubgraphCallback, Utils}
import br.ufmg.cs.systems.fractal.{FractalGraph, Fractoid}
import com.koloboke.collect.map.ObjObjMap
import com.koloboke.collect.map.hash.HashObjObjMaps
import com.koloboke.collect.set.hash.{HashIntSets, HashObjSets}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

class BuiltInApplications(self: FractalGraph) extends Logging {

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
            val mcvcSize = pattern.explorationPlan.mcvcSize()
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

   def motifs2(numVertices: Int): RDD[(Pattern,Long)] = {
      self.vfractoid.expand(numVertices)
         .aggregationCanonicalPatternLong(
            s => s.quickPattern(), 0, _ => 1L, _ + _)
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

   def motifs2(numVertices: Int, fraction: Double)
   : RDD[(Pattern,Long)] = {
      self.svfractoid(fraction).expand(numVertices).
         aggregationCanonicalPatternLong(
            s => s.quickPattern(), 0, _ => 1L, _ + _)
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


   def motifspfmcvc2(numVertices: Int): RDD[(Pattern,Long)] = {
      var motifCountRDD = self.fractalContext.sparkContext
         .emptyRDD[(Pattern,Long)]

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
         patternWithoutPlan.setInduced(true)
         patternWithoutPlan.setVertexLabeled(false)

         val newPatternsCur = PatternExplorationPlanMCVC
            .apply(patternWithoutPlan).cursor()

         while (newPatternsCur.moveNext()) {
            val pattern = newPatternsCur.elem()
            val mcvcSize = pattern.explorationPlan().mcvcSize()

            val callback: (
               PatternInducedSubgraph,
                  Computation[PatternInducedSubgraph],
                  SubgraphCallback[PatternInducedSubgraph]) => Unit =
               (s,c,cb) => {
                  s.completeMatch(c, pattern, cb)
               }

            val partialMapRDD = gquerying(pattern)
               .explore(mcvcSize - 1)
               .aggregationCanonicalPatternLongWithCallback(
                  s => s.applyLabels(pattern),
                  0L,
                  _ => 1L,
                  _ + _,
                  callback
               )

            motifCountRDD = motifCountRDD.union(partialMapRDD)
         }
      }

      motifCountRDD

   }

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

   def motifspf2(numVertices: Int): RDD[(Pattern,Long)] = {
      var motifCountRDD = self.fractalContext.sparkContext
         .emptyRDD[(Pattern,Long)]

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
          * Motifs counting RDD
          */
         val mappingRDD = gquerying(pattern).explore(numVertices - 1)
               .aggregationCanonicalPatternLong(
                  s => s.applyLabels(pattern),
                  0L,
                  _ => 1L,
                  _ + _
               )
         motifCountRDD = motifCountRDD.union(mappingRDD)
      }

      motifCountRDD
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

   def fsmpf(supportThreshold: Int, maxNumEdges: Int)
   : RDD[(Pattern, MinImageSupport)] = {
      var frequentPatternSupportRDD = self.fractalContext.sparkContext
         .emptyRDD[(Pattern, MinImageSupport)]

      val frequentLabels = HashIntSets.newMutableSet()
      val lastInfrequentPatterns = HashObjSets.newMutableSet[Pattern]()
      val lastFrequentPatterns = HashObjObjMaps.newMutableMap[Pattern,MinImageSupport]()
      val frequentPatterns = HashObjObjMaps.newMutableMap[Pattern,MinImageSupport]()
      var numEdges = 0
      val minSupport = supportThreshold

      /**
       * Aggregation functions
       */
      val minImageSupport = new MinImageSupport()
      val value: PatternInducedSubgraph => MinImageSupport = s => {
         minImageSupport.setSupport(minSupport)
         minImageSupport.setSubgraph(s)
         minImageSupport
      }
      val aggregate: (MinImageSupport,MinImageSupport) => Unit = (s1,s2) => {
         s1.aggregate(s2)
      }

      /**
       * Auxiliary functions
       */
      def canonicalPatternMap(quickPatternRDD: RDD[(Pattern,MinImageSupport)])
      : RDD[(Pattern,MinImageSupport)] = {
         quickPatternRDD
            .map { case (quickPatern,supp) =>
               val canonicalPattern = quickPatern.copy()
               canonicalPattern.turnCanonical()
               supp.handleConversionFromQuickToCanonical(quickPatern, canonicalPattern)
               (canonicalPattern, supp)
            }
            .reduceByKey((s1,s2) => {s1.aggregate(s2); s1})
      }


      {
         val start = System.currentTimeMillis()
         val singleEdgePattern = PatternUtils.singleEdgePattern()
         val key: PatternInducedSubgraph => Pattern = s => s.applyLabels(singleEdgePattern)
         singleEdgePattern.setVertexLabeled(false)
         val edgesSupportsRDD = gquerying(singleEdgePattern)
            .explore(singleEdgePattern.getNumberOfVertices - 1)
            .aggregationObjObj[Pattern,MinImageSupport](
               key, value, aggregate)

         val canonicalEdgesSupportsRDD = canonicalPatternMap(edgesSupportsRDD)
               .cache()

         canonicalEdgesSupportsRDD.collect().foreach { case (p,s) =>
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

         frequentPatternSupportRDD = frequentPatternSupportRDD.union(
            canonicalEdgesSupportsRDD.filter(_._2.hasEnoughSupport)
         )

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

               val key: PatternInducedSubgraph => Pattern =
                  s => s.applyLabels(pattern)
               val patternSupportRDD = gquerying(pattern)
                  .explore(pattern.getNumberOfVertices - 1)
                  .aggregationObjObj[Pattern,MinImageSupport](
                     key, value, aggregate)

               val canonicalPatternSupportRDD = canonicalPatternMap(patternSupportRDD)
                     .cache()

               canonicalPatternSupportRDD.collect().foreach { case (p,s) =>
                  if (s.hasEnoughSupport) {
                     lastFrequentPatterns.put(p, s)
                     frequentPatterns.put(p, s)
                  } else {
                     lastInfrequentPatterns.add(p)
                  }
               }

               frequentPatternSupportRDD = frequentPatternSupportRDD.union(
                  canonicalPatternSupportRDD.filter(_._2.hasEnoughSupport)
               )
            }
         }
         numEdges += 1
      }

      frequentPatternSupportRDD
   }

   /**
    * Frequent subgraph mining using the "pattern-first" approach and the MNI
    * (Minimum Node Image) as support metric and using the optimized MCVC
    * approach for pattern matching
    * @param supportThreshold Minimum support value
    * @param maxNumEdges Hard limit on the size of the interesting patterns
    * @return an RDD with frequent patterns and their current support
    */
   def fsmpfmcvc(supportThreshold: Int, maxNumEdges: Int)
   : RDD[(Pattern, MinImageSupport)] = {
      var frequentPatternSupportRDD = self.fractalContext.sparkContext
         .emptyRDD[(Pattern, MinImageSupport)]

      val frequentLabels = HashIntSets.newMutableSet()
      val lastInfrequentPatterns = HashObjSets.newMutableSet[Pattern]()
      val lastFrequentPatterns = HashObjObjMaps.newMutableMap[Pattern,MinImageSupport]()
      val frequentPatterns = HashObjObjMaps.newMutableMap[Pattern,MinImageSupport]()
      var numEdges = 0
      val minSupport = supportThreshold

      /**
       * Aggregation functions
       */
      val minImageSupport = new MinImageSupport()
      val value: PatternInducedSubgraph => MinImageSupport = s => {
         minImageSupport.setSupport(minSupport)
         minImageSupport.setSubgraph(s)
         minImageSupport
      }
      val aggregate: (MinImageSupport,MinImageSupport) => Unit = (s1,s2) => {
         s1.aggregate(s2)
      }

      /**
       * Auxiliary functions
       */
      def canonicalPatternMap(quickPatternRDD: RDD[(Pattern,MinImageSupport)])
      : RDD[(Pattern,MinImageSupport)] = {
         quickPatternRDD
            .map { case (quickPatern,supp) =>
               val canonicalPattern = quickPatern.copy()
               canonicalPattern.turnCanonical()
               supp.handleConversionFromQuickToCanonical(quickPatern, canonicalPattern)
               (canonicalPattern, supp)
            }
            .reduceByKey((s1,s2) => {s1.aggregate(s2); s1})
      }


      {
         val start = System.currentTimeMillis()
         val singleEdgePattern = PatternUtils.singleEdgePattern()
         singleEdgePattern.setVertexLabeled(false)

         val key: PatternInducedSubgraph => Pattern =
            s => s.applyLabels(singleEdgePattern)
         val callback: (
            PatternInducedSubgraph,
               Computation[PatternInducedSubgraph],
               SubgraphCallback[PatternInducedSubgraph]) => Unit =
            (s,c,cb) => {
               s.completeMatch(c, c.getPattern, cb)
            }

         val edgesSupportsRDD = gquerying(singleEdgePattern)
            // not explored because of MCVC partial match: 1 vertex in this case
            .aggregationObjObjWithCallback[Pattern,MinImageSupport](key,
               value, aggregate, callback)

         val canonicalEdgeSupportsRDD = canonicalPatternMap(edgesSupportsRDD)

         canonicalEdgeSupportsRDD.collect(). foreach {case (p,s) =>
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

         frequentPatternSupportRDD = frequentPatternSupportRDD.union(
            canonicalEdgeSupportsRDD.filter(_._2.hasEnoughSupport)
         )

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

               val key: PatternInducedSubgraph => Pattern =
                  s => s.applyLabels(pattern)
               val callback: (
                  PatternInducedSubgraph,
                     Computation[PatternInducedSubgraph],
                     SubgraphCallback[PatternInducedSubgraph]) => Unit =
                  (s,c,cb) => {
                     s.completeMatch(c, c.getPattern, cb)
                  }

               val patternSupport = gquerying(pattern)
                  .explore(mcvcSize - 1)
                  .aggregationObjObjWithCallback[Pattern,MinImageSupport](
                     key, value, aggregate, callback)

               val canonicalPatternSupportRDD = canonicalPatternMap(patternSupport)

               canonicalPatternSupportRDD.collect().foreach { case (p,s) =>
                  if (s.hasEnoughSupport) {
                     lastFrequentPatterns.put(p, s)
                     frequentPatterns.put(p, s)
                  } else {
                     lastInfrequentPatterns.add(p)
                  }
               }

               frequentPatternSupportRDD = frequentPatternSupportRDD.union(
                  canonicalPatternSupportRDD.filter(_._2.hasEnoughSupport)
               )
            }
         }
         numEdges += 1
      }

      frequentPatternSupportRDD
   }

   /**
    * Frequent subgraph Mining (FSM)
    * @param support threshold to determine what is frequent according to
    *                the (minimum image)
    * @param maxNumEdges
    * @return an RDD with frequent patterns and their support
    */
   def fsm(support: Int, maxNumEdges: Int): RDD[(Pattern,MinImageSupport)] = {
      var frequentPatternSupportRDD = self.fractalContext.sparkContext
         .emptyRDD[(Pattern,MinImageSupport)]

      if (maxNumEdges < 1) return frequentPatternSupportRDD

      val quickPatterns = new ObjSet[Pattern]()
      val sc = self.fractalContext.sparkContext

      def canonicalPatternMap(quickPatternRDD: RDD[(Pattern,MinImageSupport)])
      : RDD[(Pattern,(ObjSet[Pattern],MinImageSupport))] = {
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
      }

      def filterInfrequents
      (patternSupportRDD: RDD[(Pattern,(ObjSet[Pattern],MinImageSupport))])
      : Boolean = {
         val stepFrequentPatternSupportRDD = patternSupportRDD
            .filter(_._2._2.hasEnoughSupport)
            .cache()

         val quickPatternsBefore = quickPatterns.size()
         stepFrequentPatternSupportRDD
            .map(kv => kv._2._1)
            .collect()
            .foreach(ps => quickPatterns.addAll(ps))
         val quickPatternsAfter = quickPatterns.size()

         frequentPatternSupportRDD = frequentPatternSupportRDD.union(
            stepFrequentPatternSupportRDD.map(kv => (kv._1, kv._2._2))
         )

         quickPatternsAfter > quickPatternsBefore
      }

      val bootstrap = self.efractoid.expand(1)

      /**
       * Functions used for aggregation
       */
      val key: EdgeInducedSubgraph => Pattern = s => s.quickPattern()
      val minImageSupp = new MinImageSupport()
      val value: EdgeInducedSubgraph => MinImageSupport = s => {
         minImageSupp.setSupport(support)
         minImageSupp.setSubgraph(s)
         minImageSupp
      }
      val aggregate: (MinImageSupport,MinImageSupport) => Unit =
         (minImageSupp1, minImageSupp2) => {
            minImageSupp1.aggregate(minImageSupp2)
         }

      var freqFrac = bootstrap
      var quickPatternMapRDD = bootstrap
         .aggregationObjObj[Pattern,MinImageSupport](key, value, aggregate)

      var numEdges = 1
      var stepPatternSupportRDD = canonicalPatternMap(quickPatternMapRDD)
      var foundFrequentPattern = filterInfrequents(stepPatternSupportRDD)
      var continue = numEdges < maxNumEdges && foundFrequentPattern

      while (continue) {
         val quickPatternsBc = sc.broadcast(quickPatterns)
         freqFrac = freqFrac
            .filter((s,c) => quickPatternsBc.value.contains(s.quickPattern))
            .expand(1)

         quickPatternMapRDD = freqFrac
            .aggregationObjObj[Pattern,MinImageSupport](key, value, aggregate)

         stepPatternSupportRDD = canonicalPatternMap(quickPatternMapRDD)
         numEdges += 1
         foundFrequentPattern = filterInfrequents(stepPatternSupportRDD)
         continue = numEdges < maxNumEdges && foundFrequentPattern

         quickPatternsBc.unpersist()
      }

      frequentPatternSupportRDD
   }

   /**
    * Match pattern using Minimum Connected Vertex Cover (MCVC) strategy
    * TODO: these fractoids returned use a simple callback as last process
    * steps and thus, it is not safe to grow this workflow
    * @param pattern representing a query
    * @return an array of fractoids with partial query results
    */
   def gqueryingmcvc(pattern: Pattern)
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

   def periodicInducedSubgraphsSF(periodicThreshold: Int)
   : Fractoid[VertexInducedSubgraph] = {
      new InducedPeriodicSubgraphsSF(periodicThreshold).apply(self)
   }

   def periodicInducedSubgraphsPF
   (periodicThreshold: Int, numVertices: Int,
    callback: (Pattern, Fractoid[PatternInducedSubgraph]) => Unit): Unit = {
      new InducedPeriodicSubgraphsPF(periodicThreshold, numVertices, callback)
         .apply(self)
   }

   def periodicInducedSubgraphsPFMCVC
   (periodicThreshold: Int, numVertices: Int,
    callback: (InducedPeriodicSubgraphsPFMCVC, Pattern,
       Fractoid[PatternInducedSubgraph]) => Unit): Unit = {
      new InducedPeriodicSubgraphsPFMCVC(periodicThreshold, numVertices, callback)
         .apply(self)
   }
}
