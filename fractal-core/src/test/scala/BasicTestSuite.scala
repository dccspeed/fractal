package br.ufmg.cs.systems.fractal

import br.ufmg.cs.systems.fractal.gmlib.fsm.DomainSupport
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Tag}

import scala.io.Source

class BasicTestSuite extends FunSuite with BeforeAndAfterAll with Logging {
   private val numPartitions: Int = 8
   private val appName: String = "fractal-test"
   private val logLevel: String = "error"

   private var master: String = _
   private var sc: SparkContext = _
   private var fc: FractalContext = _
   private var fgraph: FractalGraph = _
   private var fgraphEdgeLabel: FractalGraph = _
   private var citeseerSingleLabelGraph: FractalGraph = _
   private var citeseerGraph: FractalGraph = _

   /**
    * Ground truth for testing pattern matching application
    */
   private val patternMatchingGt: Map[(String,String,Int),Long] = {
      var patternMatchingGt = Map.empty[(String,String,Int),Long]
      val in = Source.fromFile("../data/pattern-matching-test.gt")
      for (line <- in.getLines) {
         val toks = line.trim.split(" ")
         val (graph, query, numVertices, numSubgraphs) = (toks(0), toks(1),
            toks(2), toks(3))
         patternMatchingGt = patternMatchingGt +
            ((graph,query,numVertices.toInt) -> numSubgraphs.toLong)
      }
      in.close()
      patternMatchingGt
   }

   /**
    * Ground-truth for testing the FSM application
    */
   private val fsmGt: Map[(String,Int),Long] = {
      var fsmGt = Map.empty[(String,Int),Long]
      val in = Source.fromFile("../data/fsm-test.gt")
      for (line <- in.getLines) {
         val toks = line.trim.split(" ")
         val (graph, minSupport, numPatterns) = (toks(0), toks(1), toks(2))
         fsmGt = fsmGt + ((graph,minSupport.toInt) -> numPatterns.toLong)
      }
      in.close()
      fsmGt
   }

   /**
    * Ground-truth for testing the maximal cliques application
    */
   private val maximalCliqueGt: Map[(String,Int),Long] = {
      var maximalCliqueGt = Map.empty[(String,Int),Long]
      val in = Source.fromFile("../data/maximal-clique-test.gt")
      for (line <- in.getLines) {
         val toks = line.trim.split(" ")
         val (graph, maxSize, numSubgraphs) = (toks(0), toks(1), toks(2))
         maximalCliqueGt = maximalCliqueGt + ((graph,maxSize.toInt) ->
            numSubgraphs.toLong)
      }
      in.close()
      maximalCliqueGt
   }

   /** set up spark context */
   override def beforeAll: Unit = {
      master = s"local[${numPartitions}]"
      // spark conf and context
      val conf = new SparkConf().
         setMaster(master).
         setAppName(appName)

      sc = new SparkContext(conf)
      sc.setLogLevel(logLevel)
      fc = new FractalContext(sc, logLevel)

      fgraph = fc.textFile ("../data/cube.graph")

      fgraphEdgeLabel = fc.textFile ("../data/cube-edge-label.graph")

      citeseerSingleLabelGraph = fc.textFile(
         "../data/citeseer-single-label.sc",
         graphClass = "br.ufmg.cs.systems.fractal.graph.SuccinctMainGraph")

      citeseerGraph = fc.textFile(
         "../data/citeseer.sc",
         graphClass = "br.ufmg.cs.systems.fractal.graph.SuccinctMainGraph")
   }

   /** stop spark context */
   override def afterAll: Unit = {
      if (sc != null) {
         sc.stop()
         fc.stop()
      }
   }

   test ("[cube,motifs]", Tag("cube.motifs")) {
      // Test output for motifs for subgraph with size 0 to 3

      // Expected output
      val numSubgraph = List(8, 12, 24)

      for (k <- 0 to (numSubgraph.size - 1)) {
         val motifsRes = fgraph.motifs.
            set ("num_partitions", numPartitions).
            explore(k)
         val subgraphs = motifsRes.subgraphs

         assert(subgraphs.count() == numSubgraph(k))
      }

   }

   test ("[cube,cliques]", Tag("cube.cliques")) {
      val numSubgraph = List(8, 12, 0)

      for (k <- 0 to (numSubgraph.size - 1)) {
         val cliqueRes = fgraph.cliques.
            set ("num_partitions", numPartitions).
            explore(k)

         val subgraphs = cliqueRes.subgraphs
         assert(subgraphs.count == numSubgraph(k))
      }
   }

   test ("[cube,cliquesopt]", Tag("cube.cliquesopt")) {
      val numSubgraph = List(8, 12, 0)

      for (k <- 0 to (numSubgraph.size - 1)) {
         val cliqueRes = fgraph.cliquesKClist(k+1).
            set ("num_partitions", numPartitions).
            explore(k)

         val subgraphs = cliqueRes.subgraphs
         assert(subgraphs.count == numSubgraph(k))
      }
   }

   test ("[maximalcliques,gt]", Tag("maximalcliques.gt")) {
      for (((graph,maxSize),numSubgraphs) <- maximalCliqueGt) {
         val chosenGraph = if (graph == "citeseer-single-label") {
            citeseerSingleLabelGraph
         } else {
            throw new RuntimeException("Unknown graph")
         }

         val numMaximalCliquesAccum = sc.longAccumulator
         val maximalCliques = chosenGraph.maximalCliques
            .explore(maxSize - 1)
         maximalCliques.compute((_,_) => numMaximalCliquesAccum.add(1))

         assert (numMaximalCliquesAccum.value == numSubgraphs)
      }
   }

   test ("[cube,fsm]", Tag("cube.fsm")) {
      import br.ufmg.cs.systems.fractal.gmlib.fsm.DomainSupport
      import br.ufmg.cs.systems.fractal.pattern.Pattern

      // Critical test
      // Test output for fsm with support 2 for Subgraphs with size 2 to 3
      val support = 2

      // Expected output
      val numFreqPatterns = List(3, 3+4, 3+4+7)

      for (k <- 0 to (numFreqPatterns.size - 1)) {
         fgraph.set("num_partitions", numPartitions)
         val freqPatterns = fgraph.fsm(support, k)

         assert(freqPatterns.size == numFreqPatterns(k))
      }
   }

   test ("[cube,gquerying]", Tag("cube.gquerying")) {
      // Expected output
      val numSubgraph = Map("triangles" -> 0, "squares" -> 2)

      // triangles
      val triangle = new FractalGraph("../data/triangle-test.graph", fgraph.fractalContext)
      val triangles = fgraph.gquerying(triangle.asPattern).
         set ("num_partitions", numPartitions).
         explore(2)
      assert(triangles.numValidSubgraphs() == numSubgraph("triangles"))

      // squares
      val square = new FractalGraph("../data/square-test.graph", fgraph.fractalContext)
      val squares = fgraph.gquerying(square.asPattern).
         set ("num_partitions", numPartitions).
         explore(3)
      assert (squares.numValidSubgraphs() == numSubgraph("squares"))

   }

   test ("[cube,vfilter]", Tag("cube.vfilter")) {
      val numSubgraph = List(3)
      for (k <- 0 to (numSubgraph.size - 1)) {
         val frac = fgraph.vfractoidAndExpand.
            vfilter [String] (v => v.getVertexLabel() == 1).
            set ("num_partitions", numPartitions)
         val subgraphs = frac.subgraphs
         assert(subgraphs.count == numSubgraph(k))
      }
   }

   test ("[cube,efilter]", Tag("cube.efilter")) {
      val numSubgraph = List(2)
      for (k <- 0 to (numSubgraph.size - 1)) {
         val frac = fgraph.vfractoid.
            expand(1).
            efilter [String] (e => e.getSourceId() == 1).
            expand(1).
            set ("num_partitions", numPartitions)
         val subgraphs = frac.subgraphs
         assert(subgraphs.count == numSubgraph(k))
      }
   }

   test ("[cube,kws]", Tag("cube.kws")) {
      val keywords = Array("a", "b")
      val kws = fgraphEdgeLabel.keywordSearch(numPartitions, keywords)

      assert (kws.subgraphs.count == 2)
   }

   test ("[sampling.motifs.3]", Tag("sampling.motifs.3")) {
      import br.ufmg.cs.systems.fractal.pattern.Pattern
      import org.apache.hadoop.io.LongWritable

      // 3-motifs
      val motifs3 = citeseerSingleLabelGraph.motifs.explore(2)
      val motifsCounts3 = motifs3.aggregationMap [Pattern,LongWritable] ("motifs")

      // 3-motifs estimate
      val fraction = 0.5
      val motifs3est = citeseerSingleLabelGraph.svfractoid(fraction).
         set("num_partitions", numPartitions).
         expand(3).
         aggregate [Pattern,LongWritable] (
            "motifs",
            (e,c,k) => { e.getPattern },
            (e,c,v) => { v.set(1); v },
            (v1,v2) => { v1.set(v1.get() + v2.get()); v1 })
      val motifsCounts3est = motifs3est.aggregationMap [Pattern,LongWritable] ("motifs")

      for ((p,c) <- motifsCounts3) {
         val gt = c.get()
         val estimate = motifsCounts3est(p).get() / fraction
         val error = 100 * (gt - estimate).abs / gt.toDouble
         logInfo(s"pattern=${p}; ground-truth=${gt}; fraction=${fraction}; estimate=${estimate}" +
            s"; error(%)=${error}")

         // TODO: find a better way to assert correctness for sampling
         assert (error <= 15)
      }
   }

   test ("[sampling.motifs.4]", Tag("sampling.motifs.4")) {
      import br.ufmg.cs.systems.fractal.pattern.Pattern
      import org.apache.hadoop.io.LongWritable

      // 4-motifs
      val motifs4 = citeseerSingleLabelGraph.motifs.explore(3)
      val motifsCounts4 = motifs4.aggregationMap [Pattern,LongWritable] ("motifs")

      // 4-motifs estimate
      val fraction = 0.5
      val motifs4est = citeseerSingleLabelGraph.svfractoid(fraction).
         set("num_partitions", numPartitions).
         expand(4).
         aggregate [Pattern,LongWritable] (
            "motifs",
            (e,c,k) => { e.getPattern },
            (e,c,v) => { v.set(1); v },
            (v1,v2) => { v1.set(v1.get() + v2.get()); v1 })
      val motifsCounts4est = motifs4est.aggregationMap [Pattern,LongWritable] ("motifs")

      for ((p,c) <- motifsCounts4) {
         val gt = c.get()
         val estimate = motifsCounts4est(p).get() / fraction
         val error = 100 * (gt - estimate).abs / gt.toDouble
         logInfo(s"pattern=${p}; ground-truth=${gt}; fraction=${fraction}; estimate=${estimate}" +
            s"; error(%)=${error}")

         // TODO: find a better way to assert correctness for sampling
         assert (error <= 30)
      }
   }

   test ("[sampling.gquerying.triangle]", Tag("sampling.gquerying.triangle")) {
      // ground-truth 1: 3-cliques
      val gt1 = citeseerSingleLabelGraph.cliques.explore(2).numValidSubgraphs()

      // ground-truth 2: triangle querying
      val triangle = new FractalGraph("../data/triangle-test.graph", fc)
      val triangles = citeseerSingleLabelGraph.gquerying(triangle.asPattern).
         set("num_partitions", numPartitions).
         explore(2)
      val gt2 = triangles.numValidSubgraphs()

      // sanity check
      assert(gt1 == gt2)

      // 50% estimate
      val fraction = 0.5
      val sample = citeseerSingleLabelGraph.spfractoid(triangle.asPattern, fraction).
         expand(3).
         set("num_partitions", numPartitions)
      val estimate = sample.numValidSubgraphs() / fraction

      // error
      val error = 100 * (gt1 - estimate).abs / gt1.toDouble
      logInfo(s"ground-truth=${gt1}; fraction=${fraction}; estimate=${estimate}" +
         s"; error(%)=${error}")

      // TODO: find a better way to assert correctness for sampling
      assert(error.round <= 15)
   }

   test ("[sampling.gquerying.square]", Tag("sampling.gquerying.square")) {
      // ground-truth: square querying
      val square = new FractalGraph("../data/q2-square.graph", fc)
      val squares = citeseerSingleLabelGraph.gquerying(square.asPattern).
         set("num_partitions", numPartitions).
         explore(3)
      val gt = squares.numValidSubgraphs()

      // 10% estimate
      val fraction = 0.1
      val sample = citeseerSingleLabelGraph.spfractoid(square.asPattern, fraction).
         expand(4).
         set("num_partitions", numPartitions)
      val estimate = sample.numValidSubgraphs() / fraction

      // error
      val error = 100 * (gt - estimate).abs / gt.toDouble
      logInfo(s"ground-truth=${gt}; fraction=${fraction}; estimate=${estimate}" +
         s"; error(%)=${error}")

      // TODO: find a better way to assert correctness for sampling
      assert (error <= 20)
   }

   test ("[citeseer.gquerying.gt]", Tag("citeseer.gquerying.gt")) {
      for (((_,query,numVertices),numSubgraphs) <- patternMatchingGt.filter(_._1._1 == "citeseer")) {

         val queryGraph = new FractalGraph(s"../data/${query}.graph", citeseerSingleLabelGraph.fractalContext)
         val res = citeseerSingleLabelGraph.gquerying(queryGraph.asPattern).
            set ("num_partitions", numPartitions).
            explore(numVertices - 1)

         assert(res.numValidSubgraphs() == numSubgraphs, s"query=${query} numVertices=${numVertices}")

         val queryPartialResults = citeseerSingleLabelGraph.gqueryingmcvc(queryGraph.asPattern)
         var totalNumSubgraphs = 0L
         for (queryPartialResult <- queryPartialResults) {
            totalNumSubgraphs += queryPartialResult.
               set("num_partitions", numPartitions).
               numValidSubgraphs()
         }
         assert(totalNumSubgraphs == numSubgraphs, s"MCVC query=${query} numVertices=${numVertices}")
      }
   }

   test ("[citeseer.fsm.gt]", Tag("citeseer.fsm.gt")) {
      for (((graph,minSupport), numPatterns) <- fsmGt) {
         val chosenGraph = if (graph == "citeseer-single-label") {
            citeseerSingleLabelGraph
         } else if (graph == "citeseer") {
            citeseerGraph
         } else {
            throw new RuntimeException(s"Invalid graph string ${graph}")
         }

         chosenGraph.set("num_partitions", numPartitions)

         // subgraph-first approach: edge by edge
         val frequentPatterns = chosenGraph
            .fsm(minSupport, Int.MaxValue)
            .keys

         assert(frequentPatterns.size == numPatterns)

         // pattern-first approach: pattern-matching on every possible pattern
         val frequentPatternsPf = chosenGraph
            .fsmpf(minSupport, Int.MaxValue)
            .keys

         assert(frequentPatternsPf.size == numPatterns)

         // assert patterns found are the same
         assert(frequentPatterns.equals(frequentPatternsPf))
      }
   }
}
