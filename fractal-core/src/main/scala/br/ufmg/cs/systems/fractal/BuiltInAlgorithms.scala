package br.ufmg.cs.systems.fractal

import br.ufmg.cs.systems.fractal.annotation.Experimental
import br.ufmg.cs.systems.fractal.computation.{Computation, SubgraphEnumerator}
import br.ufmg.cs.systems.fractal.gmlib.clique.KClistEnumerator
import br.ufmg.cs.systems.fractal.graph.MainGraph
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, PatternInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import br.ufmg.cs.systems.fractal.util.{Logging, Utils}
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool
import org.apache.hadoop.io.{IntWritable, LongWritable}

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
    * Frequent subgraph Mining (FSM)
    * @param support threshold to determine what is frequent according to
    *                the (minimum image)
    * @param numSteps maximum number of exploration steps
    * @return Fractoid with the initial state for FSM
    */
  def fsm(support: Int, numSteps: Int): Fractoid[EdgeInducedSubgraph] = {
    import br.ufmg.cs.systems.fractal.gmlib.fsm._
    import br.ufmg.cs.systems.fractal.pattern.Pattern

    import scala.collection.JavaConverters._

    val AGG_FREQS = "frequent_patterns"

    val bootstrap = self.efractoid.
      expand(1).
      aggregate [Pattern,DomainSupport] (AGG_FREQS,
        (e,c,k) => { e.getPattern },
        (e,c,v) => { v.setSupport(support); v.setFromSubgraph(e); v },
        (v1,v2) => { v1.aggregate(v2); v1 },
        new DomainSupportEndAggregationFunction(),
        isIncremental = true)

    var iteration = 0
    var freqFrac = bootstrap
    var freqPatts = bootstrap.
      aggregationStorage[Pattern,DomainSupport](AGG_FREQS)

    freqPatts.getMapping().asScala.foreach { case (pattern,supp) =>
      logInfo(s"FrequentPattern iteration=${iteration} ${pattern} ${supp}")
    }

    var remainingSteps = numSteps
    var numPatts = freqPatts.getNumberMappings()
    var continue = numPatts > 0 && remainingSteps > 0

    while (continue) {
      iteration += 1
      freqFrac = freqFrac.
        filter [Pattern,DomainSupport] (AGG_FREQS) {
          (e,a) =>
            a.containsKey(e.getPattern)
        }.
        expand(1).
        aggregate [Pattern,DomainSupport] (AGG_FREQS,
          (e,c,k) => { e.getPattern },
          (e,c,v) => { v.setSupport(support); v.setFromSubgraph(e); v },
          (v1,v2) => { v1.aggregate(v2); v1 },
          new DomainSupportEndAggregationFunction(),
          isIncremental = true)

      freqPatts = freqFrac.
        aggregationStorage[Pattern,DomainSupport](AGG_FREQS)

      freqPatts.getMapping().asScala.foreach { case (pattern,supp) =>
        logInfo(s"FrequentPattern iteration=${iteration} ${pattern} ${supp}")
      }

      remainingSteps -= 1
      continue = freqPatts.getNumberMappings() > numPatts && remainingSteps > 0
      numPatts = freqPatts.getNumberMappings()
    }

    freqFrac
  }

  /**
    * subgraph Querying
    * @param subgraph query graph
    * @return Fractoid with the initial state for subraph querying
    */
  def gquerying(subgraph: FractalGraph): Fractoid[PatternInducedSubgraph] = {
    val qpattern = subgraph.asPattern
    logInfo (s"Querying pattern ${qpattern} in ${this}")
    self.pfractoid(qpattern).expand(1)
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

  /**
    * Experimental algorithms
    /def gque
    */
  @Experimental
  def maximalcliques: Fractoid[VertexInducedSubgraph] = {
    import java.util.Random

    import com.koloboke.collect.map.hash.HashIntObjMap

    val MAXIMAL_CLIQUE_COUNTING = "maximal_clique_counting"

    val vertexNeighborhood = (c: Computation[_], vertexId: Int) => {
      val neighborhood = c.getConfig().
        getMainGraph[MainGraph[_,_]]().
        getVertexNeighbourhood(vertexId)
      if (neighborhood != null) {
        neighborhood.getOrderedVertices()
      } else {
        null.asInstanceOf[IntArrayList]
      }
    }

    val aggregateMaximalClique = (c: Computation[_], k: Int, v: Long) => {
      val aggStorage = c.getAggregationStorage [
      IntWritable,LongWritable] (MAXIMAL_CLIQUE_COUNTING)
      val reusableKey = aggStorage.reusableKey()
      val reusableValue = aggStorage.reusableValue()
      reusableKey.set(k)
      reusableValue.set(v)
      aggStorage.aggregateWithReusables(reusableKey, reusableValue)
    }


    val generatePivot = (c: Computation[_], p: IntArrayList, x: IntArrayList) => {
      val seed = 13 * p.hashCode() * x.hashCode()
      val totalSize = p.size() + x.size()
      var pivot = -1
      if (totalSize > 0) {
        val pivotIdx = new Random(seed).nextInt(totalSize)
        if (pivotIdx < p.size()) {
          pivot = p.get(pivotIdx)
        } else {
          pivot = x.get(pivotIdx - p.size())
        }
      }
      pivot
    }

    val generateBestPivot2 = (
        c: Computation[_], p: IntArrayList, x: IntArrayList) => {

      var bestU = -1
      var bestUSize = Int.MinValue

      var i = 0
      while (i < p.size()) {
        val u = p.get(i)
        val uSize = vertexNeighborhood(c, u).size()
        if (bestU == -1 || uSize > bestUSize) {
          bestU = u
          bestUSize = uSize
        }
        i +=1
      }

      i = 0
      while (i < x.size()) {
        val u = x.get(i)
        val uSize = vertexNeighborhood(c, u).size()
        if (bestU == -1 || uSize > bestUSize) {
          bestU = u
          bestUSize = uSize
        }
        i +=1
      }

      bestU
    }

    val generateBestPivot = (
        c: Computation[_], p: IntArrayList, x: IntArrayList) => {

      var i = 0
      var bestU = -1
      var bestUSize = Int.MinValue

      val px = IntArrayListPool.instance().createObject()
      val intersectRes = IntArrayListPool.instance().createObject()

      Utils.sunion(p, x, 0, p.size(), 0, x.size(), px)
      val pxSize = px.size()

      while (i < pxSize) {
        val u = px.get(i)
        val orderedVertices = vertexNeighborhood(c, u)

        intersectRes.clear()
        Utils.sintersect(px, orderedVertices,
          0, px.size(), 0, orderedVertices.size(), intersectRes)

        val uSize = intersectRes.size()

        if (bestU == -1 || uSize > bestUSize) {
          bestU = u
          bestUSize = uSize
        }

        i += 1
      }

      IntArrayListPool.instance().reclaimObject(px)
      IntArrayListPool.instance().reclaimObject(intersectRes)
      bestU
    }

    self.vfractoid.
      extend { (e,c) =>
        val numWords = e.getNumWords
        val cacheStore = e.cacheStore().asInstanceOf[HashIntObjMap[IntArrayList]]
        val extensions = e.extensions()
        if (numWords == 0) {
          e.computeExtensions(c)
        } else if (numWords == 1 || !cacheStore.containsKey((numWords - 1) * 3)) {
          val vertices = e.getVertices()
          var p = IntArrayListPool.instance().createObject()
          var x = IntArrayListPool.instance().createObject()
          var candidates = IntArrayListPool.instance().createObject()
          var aux = IntArrayListPool.instance().createObject()

          var vi = 0
          var vertexId = vertices.get(vi)
          var orderedVertices = vertexNeighborhood(c, vertexId)

          if (orderedVertices != null) {
            var numOrderedVertices = orderedVertices.size()

            var i = 0
            x.clear()
            while (i < numOrderedVertices && orderedVertices.get(i) < vertexId) {
              x.add(orderedVertices.get(i))
              i += 1
            }

            p.clear()
            while (i < numOrderedVertices) {
              p.add(orderedVertices.get(i))
              i += 1
            }

            vi += 1
            while (vi < numWords) {
              vertexId = vertices.get(vi)

              //val pivot = generatePivot(c, p, x)
              val pivot = generateBestPivot(c, p, x)
              //val pivot = generateBestPivot2(c, p, x)

              candidates.clear()
              orderedVertices = vertexNeighborhood(c, pivot)
              numOrderedVertices = orderedVertices.size()
              Utils.sdifference(p, orderedVertices,
                0, p.size(), 0, numOrderedVertices, candidates)

              var partitionIdx = candidates.binarySearch(vertexId)
              if (partitionIdx < 0) {
                partitionIdx = candidates.size()
              }

              if (partitionIdx >= 0) {
                aux.clear()
                Utils.sdifference(p, candidates,
                  0, p.size(), 0, partitionIdx, aux)
                var tmp = p
                p = aux
                aux = tmp

                aux.clear()
                Utils.sunion(x, candidates,
                  0, x.size(), 0, partitionIdx, aux)
                tmp = x
                x = aux
                aux = tmp
              }

              orderedVertices = vertexNeighborhood(c, vertexId)
              numOrderedVertices = orderedVertices.size()

              aux.clear()
              Utils.sintersect(p, orderedVertices,
                0, p.size(), 0, numOrderedVertices, aux)
              var tmp = p
              p = aux
              aux = tmp

              aux.clear()
              Utils.sintersect(x, orderedVertices,
                0, x.size(), 0, numOrderedVertices, aux)
              tmp = x
              x = aux
              aux = tmp

              vi += 1
            }

            extensions.clear()
            extensions.addAll(p)

            if (p.size() == 0 && x.size() == 0) {
              aggregateMaximalClique(c, numWords, 1)
            }

            aggregateMaximalClique(c, 0, 1)

          }

          val pivotArr = IntArrayListPool.instance().createObject()
          pivotArr.add(generateBestPivot(c, p, x))
          cacheStore.put(numWords * 3, p)
          cacheStore.put(numWords * 3 + 1, x)
          cacheStore.put(numWords * 3 + 2, pivotArr)

          //IntArrayListPool.instance().reclaimObject(p)
          //IntArrayListPool.instance().reclaimObject(x)
          IntArrayListPool.instance().reclaimObject(candidates)
          IntArrayListPool.instance().reclaimObject(aux)

        } else {
          val vertices = e.getVertices()
          var p = IntArrayListPool.instance().createObject()
          var x = IntArrayListPool.instance().createObject()
          var candidates = IntArrayListPool.instance().createObject()
          var aux = IntArrayListPool.instance().createObject()

          val lastP = cacheStore.get(
            (numWords - 1) * 3).asInstanceOf[IntArrayList]
          val lastX = cacheStore.get(
            (numWords - 1) * 3 + 1).asInstanceOf[IntArrayList]
          val lastPivotArr = cacheStore.get(
            (numWords - 1) * 3 + 2).asInstanceOf[IntArrayList]

          val pivot = lastPivotArr.get(0)

          val vertexId = vertices.get(numWords - 1)

          //val pivot = generatePivot(c, lastP, lastX)
          //val pivot = generateBestPivot(c, lastP, lastX)
          //val pivot = generateBestPivot2(c, lastP, lastX)

          candidates.clear()
          var orderedVertices = vertexNeighborhood(c, pivot)
          var numOrderedVertices = orderedVertices.size()
          Utils.sdifference(lastP, orderedVertices,
            0, lastP.size(), 0, numOrderedVertices, candidates)

          var partitionIdx = candidates.binarySearch(vertexId)
          if (partitionIdx < 0) {
            partitionIdx = candidates.size()
          }

          aux.clear()
          Utils.sdifference(lastP, candidates,
            0, lastP.size(), 0, partitionIdx, aux)
          var tmp = p
          p = aux
          aux = tmp

          aux.clear()
          Utils.sunion(lastX, candidates,
            0, lastX.size(), 0, partitionIdx, aux)
          tmp = x
          x = aux
          aux = tmp

          orderedVertices = vertexNeighborhood(c, vertexId)
          numOrderedVertices = orderedVertices.size()

          aux.clear()
          Utils.sintersect(p, orderedVertices,
            0, p.size(), 0, numOrderedVertices, aux)
          tmp = p
          p = aux
          aux = tmp

          aux.clear()
          Utils.sintersect(x, orderedVertices,
            0, x.size(), 0, numOrderedVertices, aux)
          tmp = x
          x = aux
          aux = tmp

          extensions.clear()
          extensions.addAll(p)

          val pivotArr = IntArrayListPool.instance().createObject()
          pivotArr.add(generateBestPivot(c, p, x))
          cacheStore.put(numWords * 3, p)
          cacheStore.put(numWords * 3 + 1, x)
          cacheStore.put(numWords * 3 + 2, pivotArr)

          if (p.size() == 0 && x.size() == 0) {
            aggregateMaximalClique(c, numWords, 1)
          }

          aggregateMaximalClique(c, 1, 1)

        }
        extensions
      }.
      aggregate [IntWritable,LongWritable] (
        MAXIMAL_CLIQUE_COUNTING,
        (e,c,k) => { k },
        (e,c,v) => { v },
        (v1,v2) => { v1.set(v1.get() + v2.get()); v1 })
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
              vertices.getUnchecked(i)).getProperty()
            if (prop != null) {
              _curs(i) = prop.cursor()
            }
            i += 1
          }

          // get edge properties
          var j = 0
          while (i < _curs.length) {
            val prop = e.edge[HashObjSet[String]](
              edges.getUnchecked(j)).getProperty()
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
          val singleEdge = e.getEdges().getUnchecked(0)
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
          val singleEdge = e.getEdges().getUnchecked(0)
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
            val edgeId = e.getEdges().getUnchecked(0)
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
