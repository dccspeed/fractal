package br.ufmg.cs.systems.fractal

import br.ufmg.cs.systems.fractal.aggregation.PatternAggregationStorage
import br.ufmg.cs.systems.fractal.annotation.Experimental
import br.ufmg.cs.systems.fractal.computation.{Computation, SubgraphEnumerator}
import br.ufmg.cs.systems.fractal.gmlib.clique.KClistEnumerator
import br.ufmg.cs.systems.fractal.gmlib.fsm.{DomainSupport, DomainSupportEndAggregationFunction}
import br.ufmg.cs.systems.fractal.graph.MainGraph
import br.ufmg.cs.systems.fractal.pattern.{BasicPattern, Pattern, PatternExplorationPlan, PatternExplorationPlanMCVC, PatternExplorationPlanMCVCOrdering, PatternExplorationPlanMCVCVgroups, PatternUtils}
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, PatternInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.collection.{IntArrayList, ObjSet}
import br.ufmg.cs.systems.fractal.util.{Logging, SubgraphCallback, Utils, VertexPredicate}
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool
import com.koloboke.collect.map.ObjObjMap
import com.koloboke.collect.map.hash.HashObjObjMaps
import com.koloboke.collect.set.hash.{HashObjSet, HashObjSets}
import org.apache.hadoop.io.{IntWritable, LongWritable, NullWritable}

class BuiltInAlgorithms(self: FractalGraph) extends Logging {

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
            (e,c,k) => { e.getPattern },
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
            (e,c,k) => { e.getPattern },
            (e,c,v) => { v.set(1); v },
            (v1,v2) => { v1.set(v1.get() + v2.get()); v1 })
   }

   /**
    *
    * @return
    */
   def motifspf(numVertices: Int): Array[Fractoid[PatternInducedSubgraph]] = {

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
      val elapsedPatternGeneration = System.currentTimeMillis() - startPatternGeneration

      logInfo(s"CanonicalPatterns numVertices=${numVertices}" +
         s" numPatterns=${patterns.size()}" +
         s" elapsed=${elapsedPatternGeneration}")

      /**
       * Fractoids array
       */
      val fractoids = new Array[Fractoid[PatternInducedSubgraph]](patterns.size())
      var i = 0

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
          * Setup fractoid with motifs aggregation
          */
         val gqueryingRes = gquerying(pattern)
            .aggregate[Pattern,LongWritable](
               "motifs",
               (s,c,k) => {s.labeledPattern(c.getPattern)},
               (s,c,v) => {v.set(1); v},
               (v1,v2) => {v1.set(v1.get() + v2.get()); v1}
            )
            .explore(numVertices - 1)

         fractoids(i) = gqueryingRes

         i += 1
      }

      fractoids
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
    * @param cliqueSize
    * @return Fractoid with the initial state for cliques
    */
   def cliquesKClist(cliqueSize: Int): Fractoid[VertexInducedSubgraph] = {
      self.vfractoid.
         expand(1).
         set ("subgraph_enumerator",
            "br.ufmg.cs.systems.fractal.gmlib.clique.KClistEnumerator")
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
      var infrequentPatterns = HashObjSets.newMutableSet[Pattern]()
      val frequentPatterns = HashObjSets.newMutableSet[Pattern]()
      var patternsMap = HashObjObjMaps.newMutableMap[Pattern,DomainSupport]()
      var numEdges = 0

      do {
         logInfo(s"ExtendingPatterns ${frequentPatterns}")
         unlabeledPatterns = PatternUtils
            .extendByEdge(frequentPatterns, Int.MinValue)
         numEdges += 1
         frequentPatterns.clear()

         logInfo(s"PatternSetBeforeRemovingInfrequent " +
            s"size=${unlabeledPatterns.size()} ${unlabeledPatterns}")

         if (!infrequentPatterns.isEmpty) {
            logInfo(s"InfrequentPatternsBeforeExtension " +
               s"size=${infrequentPatterns.size()} ${infrequentPatterns}")
            infrequentPatterns = PatternUtils
               .extendByEdge(infrequentPatterns, Int.MinValue)
            logInfo(s"InfrequentPatternsAfterExtension " +
               s"size=${infrequentPatterns.size()} ${infrequentPatterns}")
            unlabeledPatterns.removeAll(infrequentPatterns)
            infrequentPatterns.clear()
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
                  (s,c,k) => {s.labeledPattern(c.getPattern)},
                  (s,c,v) => {v.setSupport(minSupport); v.setSubgraph(s); v},
                  (v1,v2) => {v1.aggregate(v2); v1}
               )

            val patternSupport = gqueryingRes
               .aggregationMap[Pattern, DomainSupport]("support")
            for ((labeledPattern, support) <- patternSupport) {
               if (!support.hasEnoughSupport) {
                  infrequentPatterns.add(labeledPattern)
                  logInfo(s"InfrequentPattern" +
                     s" unlabeledPattern=${pattern} ${labeledPattern}" +
                     s" support=${support} minSupport=${minSupport}")
               } else {
                  frequentPatterns.add(labeledPattern)
                  patternsMap.put(labeledPattern, support)
                  logInfo(s"FrequentPattern" +
                     s" unlabaledPattern=${pattern} " +
                     s" labeledPattern=${labeledPattern} support=${support}" +
                     s" minSupport=${minSupport}")
               }
            }
         }

      } while (!frequentPatterns.isEmpty && numEdges < maxNumEdges)

      patternsMap
   }

   /**
    * Frequent subgraph Mining (FSM)
    * @param support threshold to determine what is frequent according to
    *                the (minimum image)
    * @param numSteps maximum number of exploration steps
    * @return Fractoid with the initial state for FSM
    */
   def fsm(support: Int, numSteps: Int): ObjObjMap[Pattern,DomainSupport] = {
      import scala.collection.JavaConverters._

      val AGG_FREQS = "frequent_patterns"

      var frequentPatternsSupports = HashObjObjMaps
         .newMutableMap[Pattern,DomainSupport]()
      var quickPatterns = new ObjSet[Pattern]()

      val bootstrap = self.efractoid.
         expand(1).
         aggregate [Pattern,DomainSupport] (AGG_FREQS,
            (e,c,k) => { e.getPattern },
            (e,c,v) => { v.setSupport(support); v.setSubgraph(e); v },
            (v1,v2) => { v1.aggregate(v2); v1 },
            new DomainSupportEndAggregationFunction(),
            isIncremental = false)

      var iteration = 0
      var freqFrac = bootstrap
      var freqPatts = bootstrap
         .aggregationStorage[Pattern,DomainSupport](AGG_FREQS)
         .asInstanceOf[PatternAggregationStorage[Pattern,DomainSupport]]

      quickPatterns.addAll(freqPatts.getQuickToCanonicalMapping.keySet())
      logInfo(s"QuickPatterns ${quickPatterns}")
      frequentPatternsSupports.putAll(freqPatts.getMapping)
      val cur = frequentPatternsSupports.cursor()
      while (cur.moveNext()) {
         logInfo(s"FrequentPattern iteration=${iteration}" +
            s" ${cur.key()} ${cur.value()}")
      }

      var remainingSteps = numSteps
      var continue = freqPatts.getNumberMappings > 0 && remainingSteps > 0

      while (continue) {
         iteration += 1
         freqFrac = freqFrac
            .filter{ (s,c) =>
               quickPatterns.contains(s.getPattern)
            }
            .expand(1)
            .aggregate [Pattern,DomainSupport] (AGG_FREQS,
               (e,c,k) => { e.getPattern },
               (e,c,v) => { v.setSupport(support); v.setSubgraph(e); v },
               (v1,v2) => { v1.aggregate(v2); v1 },
               new DomainSupportEndAggregationFunction(),
               isIncremental = false)

         freqPatts = freqFrac
            .aggregationStorage[Pattern,DomainSupport](AGG_FREQS)
            .asInstanceOf[PatternAggregationStorage[Pattern,DomainSupport]]

         quickPatterns.addAll(freqPatts.getQuickToCanonicalMapping.keySet())
         logInfo(s"QuickPatterns ${quickPatterns}")
         frequentPatternsSupports.putAll(freqPatts.getMapping)
         val cur = frequentPatternsSupports.cursor()
         while (cur.moveNext()) {
            logInfo(s"FrequentPattern iteration=${iteration}" +
               s" ${cur.key()} ${cur.value()}")
         }

         remainingSteps -= 1
         continue = freqPatts.getNumberMappings > 0 && remainingSteps > 0
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

         val gquerying = this.gquerying(nextPattern).
            explore(mcvcSize - 1).
            process((s,c) => s.completeMatch(c, c.getPattern, callback))

         logInfo(s"MCVCPartialResult pattern=${nextPattern} sbLower=${nextPattern.vsymmetryBreakerLowerBound()}" +
            s" sbUpper=${nextPattern.vsymmetryBreakerUpperBound()} plan=${explorationPlanMCVC}" +
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
