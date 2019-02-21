package br.ufmg.cs.systems.fractal

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import br.ufmg.cs.systems.fractal.computation._
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.graph.{BasicMainGraph, MainGraph}
import br.ufmg.cs.systems.fractal.pattern._
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util._
import br.ufmg.cs.systems.fractal.util.collection._
import br.ufmg.cs.systems.fractal.util.pool._
import org.apache.hadoop.io._

import scala.collection.mutable.Map
import scala.reflect.{ClassTag, classTag}

/**
  * Creates an [[br.ufmg.cs.systems.fractal.FractalGraph]] used for calling fractal graph
  * algorithms
  *
  * @param path  a string indicating the path for input graph
  * @param local TODO
  * @param arab  an [[br.ufmg.cs.systems.fractal.FractalContext]] instance
  */
class FractalGraph(
                    path: String,
                    graphClass: String,
                    local: Boolean,
                    arab: FractalContext,
                    logLevel: String) extends Logging {

  private val uuid: UUID = UUID.randomUUID

  private val graphId: Int = FractalGraph.newGraphId()

  private val confs: Map[String,Any] = Map.empty

  private lazy val mainGraph: MainGraph[String,String] = {
    import Configuration._
    val config = new SparkConfiguration
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("input_graph_class", graphClass)
    config.setMainGraphClass (
      config.getClass (CONF_MAINGRAPH_CLASS, CONF_MAINGRAPH_CLASS_DEFAULT).
      asInstanceOf[Class[_ <: MainGraph[_,_]]]
    )
    config.createGraph()
    config.getMainGraph[MainGraph[String,String]]
  }

  def asPattern: Pattern = {
    val computation: Computation[EdgeInducedSubgraph] =
      new EComputationContainer()
    val config = new SparkConfiguration[EdgeInducedSubgraph]
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("edge_labelled", true)
    config.setSubgraphClass (computation.getSubgraphClass())
    config.setMainGraphId (graphId)
    config.initialize()
    val subgraph = config.createSubgraph[EdgeInducedSubgraph]

    val graph = config.getMainGraph[BasicMainGraph[_,_]]
    val numEdges = graph.getNumberEdges()
    val edges = graph.getEdges()
    var i = 0
    while (i < numEdges) {
      subgraph.addWord(edges(i).getEdgeId())
      i += 1
    }

    val pattern = config.createPattern().asInstanceOf[BasicPattern]
    pattern.setSubgraph(subgraph)
    pattern.readSymmetryBreakingConditions(s"${path}.sb")
    logInfo(s"SymmetryBreakingConditions: ${pattern.vsymmetryBreaker()}")
    pattern
  }

  def tmpPath: String = s"${arab.tmpPath}/graph-${uuid}"

  def arabContext: FractalContext = arab

  def this(path: String, arab: FractalContext, logLevel: String) = {
    this (path, Configuration.CONF_MAINGRAPH_CLASS_DEFAULT,
      false, arab, logLevel)
  }

  def this(path: String, graphClass: String,
           arab: FractalContext, logLevel: String) = {
    this (path, graphClass, false, arab, logLevel)
  }

  private def resultHandler [E <: Subgraph : ClassTag] (
      config: SparkConfiguration[E], stepByStep: Boolean = true)
    : Fractoid[E] = {
    config.set ("log_level", logLevel)
    confs.foreach { case (k,v) =>
      config.set(k, v)
      logInfo(s"Setting (${k},${v}) from graph")
    }
    config.setMainGraphId (graphId)
    new Fractoid [E] (this, config).copy (stepByStep = stepByStep)
  }

  /**
   * Return a motif computation containing:
   * - Sum aggregation with key=Pattern and value=LongWritable
   */
  def motifs: Fractoid[VertexInducedSubgraph] = {
    import br.ufmg.cs.systems.fractal.pattern.Pattern
    import org.apache.hadoop.io.LongWritable

    val AGG_MOTIFS = "motifs"
    vfractoid.
      aggregate [Pattern,LongWritable] (
        AGG_MOTIFS,
        (e,c,k) => { e.getPattern },
        (e,c,v) => { v.set(1); v },
        (v1,v2) => { v1.set(v1.get() + v2.get()); v1 })
  }

  /**
   * Return a motif computation containing:
   * - Sum aggregation with key=Pattern and value=LongWritable
   */
  def motifsGtrie(size: Int): Fractoid[VertexInducedSubgraph] = {
    import br.ufmg.cs.systems.fractal.gmlib.motif.GtrieExtender
    import org.apache.hadoop.io.{IntWritable, LongWritable}

    val AGG_MOTIFS = "motifs"
    vfractoid.
      extend { (e,c) =>
        var extender = e.getExtender()
        if (extender == null) {
          extender = GtrieExtender.create(size)
          e.setExtender(extender)
        }
        extender.extend(e,c)
      }.
      aggregate [IntWritable,LongWritable] (
        AGG_MOTIFS,
        (e,c,k) => { k.set(e.getExtender().pattern(e)); k },
        //(e,c,k) => { k.set(e.getExtender().pattern(e)); println(s"${e} ${Integer.toBinaryString(k.get())} ${e.getPattern}"); k },
        (e,c,v) => { v.set(1); v },
        (v1,v2) => { v1.set(v1.get() + v2.get()); v1 })
  }

  def fsm(support: Int, numSteps: Int): Fractoid[EdgeInducedSubgraph] = {
    import br.ufmg.cs.systems.fractal.gmlib.fsm._
    import br.ufmg.cs.systems.fractal.pattern.Pattern

    import scala.collection.JavaConverters._

    val AGG_FREQS = "frequent_patterns"

    val bootstrap = efractoid.
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
        filterByAgg [Pattern,DomainSupport] (AGG_FREQS) {
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
   */
  def cliques: Fractoid[VertexInducedSubgraph] = {
    val CLIQUE_COUNTING = "clique_counting"
    vfractoid.
      filter { (e,c) =>
        e.getNumEdgesAddedWithExpansion == e.getNumVertices - 1
      }.
      aggregate [IntWritable,LongWritable] (
        CLIQUE_COUNTING,
        (e,c,k) => { k.set(0); k },
        (e,c,v) => { v.set(1); v },
        (v1,v2) => { v1.set(v1.get() + v2.get()); v1 })
  }

  def cliquesOpt(cliqueSize: Int): Fractoid[VertexInducedSubgraph] = {
    import br.ufmg.cs.systems.fractal.optimization.CliqueInducedSubgraphs
    val CLIQUE_COUNTING = "clique_counting"
    vfractoid.
      extend { (e,c) =>
        var state = e.getState()
        if (state == null) {
          state = new CliqueInducedSubgraphs(cliqueSize)
          val extensions = state.extensions(e, c)
          e.setState(state)
          extensions
        } else {
          state.extensions(e, c)
        }
      }
  }

  def maximalcliquesNaive: Fractoid[VertexInducedSubgraph] = {
    import java.util.Random

    import com.koloboke.collect.set.hash.HashIntSet

    val MAXIMAL_CLIQUE_COUNTING = "maximal_clique_counting"

    val generatePivot = (p: HashIntSet, x: HashIntSet, seed: Int) => {
      val pivotCandidates = IntArrayListPool.instance().createObject()
      pivotCandidates.addAll(p)
      pivotCandidates.addAll(x)
      var pivot = -1
      if (pivotCandidates.size() > 0) {
        val pivotIdx = new Random(seed).nextInt(pivotCandidates.size())
        pivot = pivotCandidates.get(pivotIdx)
      }
      IntArrayListPool.instance().reclaimObject(pivotCandidates)
      pivot
    }

    vfractoid.
      extend { (e,c) =>
        val numWords = e.getNumWords
        if (numWords == 0) {
          e.extensions(c)
        } else {
          val vertices = e.getVertices()
          var p = HashIntSetPool.instance().createObject()
          var x = HashIntSetPool.instance().createObject()
          var aux = HashIntSetPool.instance().createObject()

          var i = 0
          while (i < numWords) {
            val vertexId = vertices.get(i)
            val neighborhood = c.getConfig().getMainGraph[MainGraph[_,_]]().
              getVertexNeighbourhood(vertexId)
            val orderedVertices = if (neighborhood != null) {
              neighborhood.getOrderedVertices()
            } else {
              IntArrayListPool.instance().createObject()
            }
            val numOrderedVertices = orderedVertices.size()

            aux.clear()
            var j = 0
            while (j < numOrderedVertices && orderedVertices.get(j) < vertexId) {
              val neighborId = orderedVertices.get(j)
              if (i == 0) {
                aux.add(neighborId)
              } else if (p.contains(neighborId) || x.contains(neighborId)) {
                aux.add(neighborId)
              }
              j += 1
            }

            var tmp = x
            x = aux
            aux = tmp

            aux.clear()
            while (j < numOrderedVertices && orderedVertices.get(j) > vertexId) {
              val neighborId = orderedVertices.get(j)
              if (i == 0) {
                aux.add(neighborId)
              } else if (p.contains(neighborId)) {
                aux.add(neighborId)
              }
              j += 1
            }

            tmp = p
            p = aux
            aux = tmp

            i += 1
          }

          i = 0
          while (i < numWords) {
            val vId = vertices.get(i)
            p.removeInt(vId)
            x.removeInt(vId)
            i += 1
          }

          val cur = p.cursor()
          while (cur.moveNext()) {
            e.extensions().add(cur.elem())
          }

          if (p.size() == 0 && x.size() == 0) {
            println (s"MaximalClique size=${e.getVertices().size()} vertices=${e.getVertices()}")
          } else {
            println (s"NonMaximalClique size=${e.getVertices().size()} vertices=${e.getVertices()} |P|=${p.size()} |X|=${x.size()}")
          }

          HashIntSetPool.instance().reclaimObject(p)
          HashIntSetPool.instance().reclaimObject(x)
          HashIntSetPool.instance().reclaimObject(aux)

          e.extensions()
        }
      }.
      aggregate [IntWritable,LongWritable] (
        MAXIMAL_CLIQUE_COUNTING,
        (e,c,k) => { k.set(0); k },
        (e,c,v) => { v.set(1); v },
        (v1,v2) => { v1.set(v1.get() + v2.get()); v1 })

  }

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

    vfractoid.
      extend { (e,c) =>
        val numWords = e.getNumWords
        val cacheStore = e.cacheStore().asInstanceOf[HashIntObjMap[IntArrayList]]
        val extensions = e.extensions()
        if (numWords == 0) {
          e.extensions(c)
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
      withAggregationRegistered [IntWritable,LongWritable] (MAXIMAL_CLIQUE_COUNTING) (
        (e,c,k) => k,
        (e,c,v) => v,
        (v1,v2) => { v1.set(v1.get() + v2.get()); v1 }
        )
  }

  def maximalcliquesSets: Fractoid[VertexInducedSubgraph] = {
    import java.util.Random

    import com.koloboke.collect.set.hash.HashIntSet

    val MAXIMAL_CLIQUE_COUNTING = "maximal_clique_counting"

    val generatePivot = (p: HashIntSet, x: HashIntSet) => {
      val seed = 13 * p.hashCode() * x.hashCode()
      val pivotCandidates = IntArrayListPool.instance().createObject()
      pivotCandidates.addAll(p)
      pivotCandidates.addAll(x)
      pivotCandidates.sort()
      var pivot = -1
      if (pivotCandidates.size() > 0) {
        val pivotIdx = new Random(seed).nextInt(pivotCandidates.size())
        pivot = pivotCandidates.get(pivotIdx)
      }
      IntArrayListPool.instance().reclaimObject(pivotCandidates)
      pivot
    }

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

    vfractoid.
      extend { (e,c) =>
        val numWords = e.getNumWords
        if (numWords == 0) {
          e.extensions(c)
        } else {
          val extensions = e.extensions()
          val vertices = e.getVertices()
          var p = HashIntSetPool.instance().createObject()
          var x = HashIntSetPool.instance().createObject()
          var candidates = HashIntSetPool.instance().createObject()
          var aux = HashIntSetPool.instance().createObject()
          var aux2 = HashIntSetPool.instance().createObject()

          var vi = 0
          var vertexId = vertices.get(vi)
          var orderedVertices = vertexNeighborhood(c, vertexId)

          if (orderedVertices != null) {
            var numOrderedVertices = orderedVertices.size()

            var i = 0
            aux.clear()
            while (i < numOrderedVertices && orderedVertices.get(i) < vertexId) {
              aux.add(orderedVertices.get(i))
              i += 1
            }

            var tmp = x
            x = aux
            aux = tmp

            aux.clear()
            while (i < numOrderedVertices) {
              aux.add(orderedVertices.get(i))
              i += 1
            }

            tmp = p
            p = aux
            aux = tmp

            println (s"${vi}. p=${p} x=${x}")

            vi += 1
            while (vi < numWords) {
              vertexId = vertices.get(vi)

              val pivot = generatePivot(p, x)
              candidates.clear()
              candidates.addAll(p)
              orderedVertices = vertexNeighborhood(c, pivot)
              numOrderedVertices = orderedVertices.size()
              println (s"${vi}. candidates=${candidates} pivot=${pivot}")
              i = 0
              while (i < numOrderedVertices) {
                candidates.removeInt(orderedVertices.get(i))
                i += 1
              }

              val candidatesArr = new IntArrayList(candidates)
              candidatesArr.sort()
              println (s"${vi}. candidates=${candidatesArr}")
              val ccur = candidatesArr.cursor()
              while (ccur.moveNext() && ccur.elem() != vertexId) {
                p.removeInt(ccur.elem())
                x.add(ccur.elem())
              }

              println (s"${vi}. p=${p} x=${x}")

              orderedVertices = vertexNeighborhood(c, vertexId)
              numOrderedVertices = orderedVertices.size()
              i = 0
              aux.clear()
              aux2.clear()
              while (i < numOrderedVertices) {
                val vId = orderedVertices.get(i)
                if (p.contains(vId)) {
                  aux.add(vId)
                }
                if (x.contains(vId)) {
                  aux2.add(vId)
                }
                i += 1
              }

              tmp = p
              p = aux
              aux = tmp

              tmp = x
              x = aux2
              aux2 = tmp

              i = 0
              while (i <= vi) {
                if (p.contains(vertices.get(i)) || x.contains(vertices.get(i))) {
                  throw new RuntimeException(s"Invalid sets p=${p} x=${x}")
                }
                i += 1
              }

              println (s"${vi}. p=${p} x=${x}")

              vi += 1
            }

            extensions.clear()
            extensions.addAll(p)

            if (p.size() == 0 && x.size() == 0) {
              println (s"MaximalClique size=${e.getVertices().size()} vertices=${e.getVertices()}")
            } else {
              println (s"NonMaximalClique size=${e.getVertices().size()} vertices=${e.getVertices()} |P|=${p.size()} |X|=${x.size()}")
            }

          } else {
            println (s"MaximalClique size=${e.getVertices().size()} vertices=${e.getVertices()}")
          }

          HashIntSetPool.instance().reclaimObject(p)
          HashIntSetPool.instance().reclaimObject(x)
          HashIntSetPool.instance().reclaimObject(candidates)
          HashIntSetPool.instance().reclaimObject(aux)
          HashIntSetPool.instance().reclaimObject(aux2)

          extensions
        }
      }.
      aggregate [IntWritable,LongWritable] (
        MAXIMAL_CLIQUE_COUNTING,
        (e,c,k) => { k.set(0); k },
        (e,c,v) => { v.set(1); v },
        (v1,v2) => { v1.set(v1.get() + v2.get()); v1 })

  }

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

    vfractoid.
      withFilter((e,c) => (e.getNumEdges() / maxDensity) +
        cummDensities(e.getNumVertices() - 1) >= minDensity).
      exploreExp(numSteps)
  }

  def keywordSearch(
      numPartitions: Int,
      _keywords: Array[String]): Fractoid[EdgeInducedSubgraph] = {
    import java.io._
    import java.util.Comparator
    import java.util.function.IntConsumer

    import br.ufmg.cs.systems.fractal.gmlib.keywordsearch.QueryScorer
    import br.ufmg.cs.systems.fractal.util._
    import br.ufmg.cs.systems.fractal.util.collection._
    import com.koloboke.collect.ObjCursor
    import com.koloboke.collect.set.hash.HashObjSet
    import org.apache.hadoop.io._

    import scala.collection.mutable.Map

    val keywords = _keywords

    logInfo (s"KeywordSearch keywords=${_keywords.mkString("[", ",", "]")}" +
      s" stemmedKeywords=${keywords.mkString("[", ",", "]")}")

    val INVERTED_INDEX = "inverted_index"
    val PREDICATE_INDEX = "predicate_index"
    val VALID_VERTICES = "valid_vertices"
    val SCORES = "scores"

    // TODO: should be parameter
    val maxResults = 20
    val alpha = 0.5
    val beta = 0.5

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

    val idxRes = efractoid.
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
      aggregation[Text,InvertedIndexMap](INVERTED_INDEX).toArray

    // mapping from words to their respective predicate
    val wordToPredicate = idxRes.
      aggregation[Text,InvertedIndexMap](PREDICATE_INDEX).toArray

    // valid vertices
    val validVertexIds = idxRes.aggregation[Text,IntSet](
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
      s" totalFreq=${totalFreq} totalInvIdx=${totalInvIdx}" +
      s" totalInvPredicate=${totalInvPredicate}" +
      s" took ${elapsed} ms")

    /**
     * KeywordSearchSecondPass: generate and rank subgraphs
     */

    start = System.currentTimeMillis()

    val keywordIndexBc = arab.sparkContext.broadcast(keywordIndex)
    val validEdgeIdsBc = arab.sparkContext.broadcast(validEdgeIds)
    val validVertexIdsBc = arab.sparkContext.broadcast(validVertexIds)
    val invIdxsBc = arab.sparkContext.broadcast(invIdxs)
    //val invPredicatesBc = arab.sparkContext.broadcast(invPredicates)
    val invPredicatesBc = arab.sparkContext.broadcast(invPredicates2)
    val totalInvIdxBc = arab.sparkContext.broadcast(totalInvIdx)
    val totalInvPredicateBc = arab.sparkContext.broadcast(totalInvPredicate)

    //val configBc = idxRes.masterEngine.configBc
    //idxRes.masterEngine.superstepRDD.mapPartitions { iter =>
    //  configBc.value.initializeWithTag(
    //    //new VAtomicBitSetArray(),
    //    new EAtomicBitSetArray(validVertexIdsBc.value),
    //    new EAtomicBitSetArray(validEdgeIdsBc.value))
    //  iter
    //}.foreachPartition(_ => {})

    // equation 6
    val pqiDj = (qi: Int, dj: Int) => {
      (alpha) *
        (invIdxsBc.value(qi).getFreq(dj) / totalInvIdxBc.value.getFreq(dj).toDouble) +
      (1 - alpha) *
        (invPredicatesBc.value(qi).getTotalFreq() / totalFreq.toDouble)
    }

    // equation 6
    val pqiRj = (qi: Int, rj: Int) => {
      (alpha) *
        (invPredicatesBc.value(qi).getFreq(rj) / totalInvPredicateBc.value.getFreq(rj).toDouble) +
      (1 - alpha) *
        (invPredicatesBc.value(qi).getTotalFreq() / totalFreq.toDouble)
    }

    val pR = (r: Int) => {
      totalInvPredicateBc.value.getFreq(r) / totalFreq.toDouble
    }

    val total = new Array[Double](invIdxs.length)

    val cur = totalInvPredicateBc.value.docCursor()
    while (cur.moveNext()) {
      val r = cur.elem()
      var qi = 0
      while (qi < total.length) {
        total(qi) += pqiRj(qi, r) * pR(r)
        qi += 1
      }
    }

    val totalBc = arab.sparkContext.broadcast(total)

    // equation 7
    val pRjqi = (rj: Int, qi: Int) => {
      val part = pqiRj(qi, rj) * pR(rj)
      part / totalBc.value(qi)
    }

    // equation 5
    val pqiDjrj = (qi: Int, dj: Int, rj: Int) => {
      val qiDj = pqiDj(qi, dj)
      (beta) * qiDj * pRjqi (rj, qi) + (1 - beta) * qiDj
    }

    // equation 2
    val pqiG = (qi: Int, e: EdgeInducedSubgraph) => {
      val n = e.getNumWords()
      val words = e.getWords()
      var total = 0.0
      var i = 0
      while (i < n) {
        val dj = words.getUnchecked(i)
        val rj = e.labelledEdge(dj).getEdgeLabel()
        total += (1 / n.toDouble) * pqiDjrj(qi, dj, rj)
        i += 1
      }
      total
    }

    // equation 1
    val pQG = (e: EdgeInducedSubgraph) => {
      var total = 1.0
      var qi = 0
      while (qi < invIdxsBc.value.length) {
        total *= pqiG(qi, e)
        qi += 1
      }
      total
    }

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

    val lastWordIsValid2 = (e: EdgeInducedSubgraph, w: Int,
        c: Computation[EdgeInducedSubgraph]) => {
      val words = e.getWords()
      val numWords = words.size()
      val invIdxs = invIdxsBc.value
      var valid = false
      if (validEdgeIdsBc.value.contains(w)) {
        var i = 0
        while (i < invIdxs.length) {
          val ii = invIdxs(i)
          if (ii.containsDoc(w)) {
            var j = 0
            while (j < numWords && !ii.containsDoc(words.get(j))) j += 1
            if (j == numWords) {
              valid = true
              i = invIdxs.length - 1
            }
          }
          i += 1
        }
      }
      valid
    }

    val lastWordIsValid3 = new WordFilterFunc[EdgeInducedSubgraph] {
      def apply(e: EdgeInducedSubgraph, w: Int, c: Computation[EdgeInducedSubgraph]): Boolean = {
        val words = e.getWords()
        val numWords = words.size()
        val invIdxs = invIdxsBc.value
        var valid = false
        if (validEdgeIdsBc.value.contains(w)) {
          var i = 0
          while (i < invIdxs.length) {
            val ii = invIdxs(i)
            if (ii.containsDoc(w)) {
              var j = 0
              while (j < numWords && !ii.containsDoc(words.getUnchecked(j))) j += 1
              if (j == numWords) {
                valid = true
                i = invIdxs.length - 1
              }
            }
            i += 1
          }
        }
        valid
      }
    }

    val scorer = new QueryScorer(keywordIndexBc,invIdxsBc, totalInvIdxBc,
      invPredicatesBc, totalInvPredicateBc, totalBc, totalFreq, alpha, beta)

    // we use reverse ordering to always remove the least scored subgraph from
    // the bounded priority queue
    //val ord = Ordering.by[PairWritable[DoubleWritable,IntArrayList], Double](t => - t.getLeft().get())
    val comparator = new Comparator[PairWritable[DoubleWritable,IntArrayList]] with Serializable {
      def compare(p1: PairWritable[DoubleWritable,IntArrayList],
          p2: PairWritable[DoubleWritable,IntArrayList]) = {
        - p1.getLeft().compareTo(p2.getLeft())
      }
    }

    val kwsRes = efractoid.
      set ("num_partitions", numPartitions).
      set ("input_graph_class", "br.ufmg.cs.systems.fractal.gmlib.keywordsearch.KeywordSearchGraph").
      set ("edge_labelled", true).
      set ("keep_maximal", true).
      withFilter (lastWordIsValid).
      //filter (lastWordIsValid).
      //efilter (lastWordIsValid2).
      //withWordFilter(lastWordIsValid3).
      exploreExp(keywords.size - 1)//.
      //aggregate [IntWritable,BoundedPriorityQueue[PairWritable[DoubleWritable,IntArrayList]]] (
      //  SCORES,
      //  (e,c,k) => { k.set(maxResults); k },
      //  (e,c,v) => {
      //    var p = v.peek()
      //    if (p == null) {
      //      v.init(maxResults, comparator)
      //      p = new PairWritable(new DoubleWritable(), new IntArrayList())
      //    }
      //    //p.getLeft().set(scorer.score(e))
      //    p.getLeft().set(0.0)
      //    p.getRight().clear()
      //    p.getRight().addAll(e.getVertices)
      //    v
      //  },
      //  //new Function3[EdgeInducedSubgraph,Computation[EdgeInducedSubgraph],BoundedPriorityQueue[PairWritable[DoubleWritable,IntArrayList]],BoundedPriorityQueue[PairWritable[DoubleWritable,IntArrayList]]] with Serializable {
      //  //  @transient lazy val reusableDouble = new DoubleWritable()
      //  //  @transient lazy val reusableArray = new IntArrayList()
      //  //  @transient lazy val reusablePair = new PairWritable[DoubleWritable,IntArrayList](reusableDouble, reusableArray)
      //  //  @transient lazy val pqueue = new BoundedPriorityQueue(maxResults, reusablePair, comparator)
      //  //  def apply(e: EdgeInducedSubgraph, c: Computation[EdgeInducedSubgraph], v: BoundedPriorityQueue[PairWritable[DoubleWritable,IntArrayList]]) = {
      //  //    //reusableDouble.set(pQG(e))
      //  //    reusableDouble.set(0.0)
      //  //    reusableArray.clear()
      //  //    reusableArray.addAll(e.getVertices())
      //  //    pqueue
      //  //  }
      //  //},
      //  (q1,q2) => { q1.merge(q2); q1 }
      //)

    kwsRes.subgraphs((_, _) => false).count

    // get aggregation scores
    //val topResults = kwsRes.
    //  aggregation [IntWritable,BoundedPriorityQueue[PairWritable[DoubleWritable,IntArrayList]]] (SCORES).
    //  get (new IntWritable(maxResults)).get

    elapsed = System.currentTimeMillis() - start

    logInfo (s"KeywordSearchScoring took ${elapsed} ms")

    //topResults.foreach { pair =>
    //  logInfo (s"KeywordSearchScore words=${pair.getLeft()} score=${pair.getRight()}")
    //}

    kwsRes
  }

  def gquerying(subgraph: FractalGraph): Fractoid[PatternInducedSubgraph] = {
    val qpattern = subgraph.asPattern

    logInfo (s"Querying pattern ${qpattern} in ${this}")

    val SUBGRAPH_COUNTING = "subgraph_counting"

    val computation = pfractoid(qpattern).
      extend { (e,c) =>
        e.extensions(c, c.getPattern)
      }.
      aggregate [IntWritable,LongWritable] (
        SUBGRAPH_COUNTING,
        (e,c,k) => { k.set(0); k },
        (e,c,v) => { v.set(1); v },
        (v1,v2) => { v1.set(v1.get() + v2.get()); v1 })

    computation
  }

  def gqueryingNaive(subgraph: FractalGraph): Fractoid[PatternInducedSubgraph] = {
    val qpattern = subgraph.asPattern

    logInfo (s"Querying pattern ${qpattern} in ${this}")

    val SUBGRAPH_COUNTING = "subgraph_counting"

    val computation = pfractoid(qpattern).
      extend ((e,c) => e.extensions(c)).
      filter { (e,c) =>
        val p = e.getPattern
        p.equals(c.getPattern, p.getNumberOfEdges)
      }.
      aggregate [IntWritable,LongWritable] (
        SUBGRAPH_COUNTING,
        (e,c,k) => { k.set(0); k },
        (e,c,v) => { v.set(1); v },
        (v1,v2) => { v1.set(v1.get() + v2.get()); v1 })

    computation
  }

  /** api for custom computations **/

  /**
   * Build a computation based on the subgraph class type
   */
  def computation [E <: Subgraph : ClassTag]: Fractoid[E] = {
    val eClass = classTag[E].runtimeClass
    if (eClass == classOf[VertexInducedSubgraph]) {
      vfractoid.asInstanceOf[Fractoid[E]]
    } else if (eClass == classOf[EdgeInducedSubgraph]) {
      efractoid.asInstanceOf[Fractoid[E]]
    } else {
      throw new RuntimeException (s"Unsupported subgraph type ${eClass}")
    }
  }

  /**
   * Create an empty computation with the container cleared
   */
  def emptyComputation [E <: Subgraph : ClassTag]: Fractoid[E] = {
    assert (computation.config.clearComputationContainer)
    computation
  }

  /**
   * Returns a new result with a configurable computation container.
   *
    * {{{
    *   import br.ufmg.cs.systems.fractal.fractalContext
    *   val input_graph = "fractalDir/data/cube.graph"
    *
    *   val graph = arab.textFile(input_graph)
    *   val res = arabGraph.
    *     edgeInducedComputation {(e,c) =>
    *       if (e.getNumWords == 3) {
    *         c.output (e)
    *       }
    *     }.
    *     withFilter ((e,c) => e.getNumWords == 3).
    *     withShouldExpand ((e,c) => e.getNumWords < 3)
    *
    *   res.Subgraphs.count()
    *   res.Subgraphs.collect()
    * }}}
    *
    * @param process function that is called for each subgraph produced
    * @return an [[br.ufmg.cs.systems.fractal.Fractoid]] carrying odags and Subgraphs
   */
  def efractoid(
      process: (EdgeInducedSubgraph,
                Computation[EdgeInducedSubgraph]) => Unit)
    : Fractoid[EdgeInducedSubgraph] = {
    val computation: Computation[EdgeInducedSubgraph] =
      new EComputationContainer(processOpt = Option(process))
    val config = new SparkConfiguration[EdgeInducedSubgraph].
      withNewComputation (computation)
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("input_graph_class", graphClass)
    config.set ("output_path", s"${tmpPath}/edge-computation-${config.getId}")
    customComputation [EdgeInducedSubgraph] (config)
  }

  def efractoid: Fractoid[EdgeInducedSubgraph] =
    efractoid (null)

  /**
   * Returns a new result with a configurable computation container.
   *
    * {{{
    *   import br.ufmg.cs.systems.fractal.fractalContext
    *   val input_graph = "fractalDir/data/cube.graph"
    *
    *   val graph = arab.textFile(input_graph)
    *   val res = arabGraph.
    *     vertexInducedComputation {(e,c) =>
    *       if (e.getNumWords == 3) {
    *         c.output (e)
    *       }
    *     }.
    *     withFilter ((e,c) => e.getNumWords == 3).
    *     withShouldExpand ((e,c) => e.getNumWords < 3)
    *
    *   res.Subgraphs.count()
    *   res.Subgraphs.collect()
    * }}}
    *
    * @param process function that is called for each subgraph produced
    * @return an [[br.ufmg.cs.systems.fractal.Fractoid]] carrying odags and Subgraphs
   */
  def vfractoid(
      process: (VertexInducedSubgraph,
                Computation[VertexInducedSubgraph]) => Unit)
    : Fractoid[VertexInducedSubgraph] = {
    val computation: Computation[VertexInducedSubgraph] =
      new VComputationContainer(processOpt = Option(process))
    val config = new SparkConfiguration[VertexInducedSubgraph].
      withNewComputation (computation)
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("input_graph_class", graphClass)
    config.set ("output_path", s"${tmpPath}/vertex-computation-${config.getId}")
    customComputation [VertexInducedSubgraph] (config)
  }

  def vfractoid: Fractoid[VertexInducedSubgraph] =
    vfractoid (null)

  /**
   * Returns a new result with a configurable computation container.
   *
    * {{{
    *   import br.ufmg.cs.systems.fractal.fractalContext
    *   val input_graph = "fractalDir/data/cube.graph"
    *
    *   val graph = arab.textFile(input_graph)
    *   val res = arabGraph.
    *     edgeInducedComputation {(e,c) =>
    *       if (e.getNumWords == 3) {
    *         c.output (e)
    *       }
    *     }.
    *     withFilter ((e,c) => e.getNumWords == 3).
    *     withShouldExpand ((e,c) => e.getNumWords < 3)
    *
    *   res.Subgraphs.count()
    *   res.Subgraphs.collect()
    * }}}
    *
    * @param process function that is called for each subgraph produced
    * @return an [[br.ufmg.cs.systems.fractal.Fractoid]] carrying odags and Subgraphs
   */
  def pfractoid(
                 process: (PatternInducedSubgraph,
                Computation[PatternInducedSubgraph]) => Unit,
                 pattern: Pattern): Fractoid[PatternInducedSubgraph] = {
    val computation: Computation[PatternInducedSubgraph] =
      new VEComputationContainer(processOpt = Option(process),
        patternOpt = Option(pattern))
    val config = new SparkConfiguration[PatternInducedSubgraph].
      withNewComputation (computation)
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("input_graph_class", graphClass)
    config.set ("output_path",
      s"${tmpPath}/vertex-edge-computation-${config.getId}")
    customComputation [PatternInducedSubgraph] (config)
  }

  def pfractoid: Fractoid[PatternInducedSubgraph] =
    pfractoid (null, null)

  def pfractoid(
      pattern: Pattern): Fractoid[PatternInducedSubgraph] =
    pfractoid (null, pattern)

  def customComputation [E <: Subgraph: ClassTag] (
      config: SparkConfiguration[E]): Fractoid[E] = {
    resultHandler [E](config, true)
  }

  def set(key: String, value: Any): Unit = {
    confs.update (key, value)
  }

  override def toString(): String = s"fractalGraph(${path})"
}

class VAtomicBitSetArray extends AtomicBitSetArray {
  override def contains(v: Int) = true
}

class EAtomicBitSetArray(validEdgeIds: IntSet) extends AtomicBitSetArray {
  override def contains(v: Int) = validEdgeIds.contains(v)
}

object FractalGraph {
  val nextGraphId: AtomicInteger = new AtomicInteger(0)
  def newGraphId(): Int = nextGraphId.getAndIncrement()

  val KWS_FILTER = "kws-filter"
  val KWS_FILTER_OUTERLOOP = "kws-filter-outerLoop"
  val KWS_FILTER_INNERLOOP = "kws-filter-innerLoop"
}
