package br.ufmg.cs.systems.fractal

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
    * Ground truth for testing motifs application
    */
   private val motifsGt: Map[(String,Int),Array[Long]] = {
      var motifsGt = Map.empty[(String,Int),Array[Long]]
      val in = Source.fromFile("../data/motifs-test.gt")
      for (line <- in.getLines) {
         val toks = line.trim.split(" ")
         val (graph, numVertices, numMotifs) = (toks(0), toks(1), toks(2))
         val counts = new Array[Long](numMotifs.toInt)
         for (i <- 0 until numMotifs.toInt) counts(i) = toks(i+3).toLong
         java.util.Arrays.sort(counts)
         motifsGt = motifsGt +
            ((graph,numVertices.toInt) -> counts)
      }
      in.close()
      motifsGt
   }

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
      val conf = new SparkConf()
         .setMaster(master)
         .setAppName(appName)

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

   test ("[cube,vsubgraphs]", Tag("cube.vsubgraphs")) {
      // Test output for motifs for subgraph with size 0 to 3

      // Expected output
      val numSubgraph = List(8, 12, 24)

      for (k <- 0 to (numSubgraph.size - 1)) {
         val vsubgraphsFrac = fgraph.vfractoid.expand(1)
            .set("num_partitions", numPartitions)
            .explore(k)
         val numSubgraphs = vsubgraphsFrac.aggregationCount

         assert(numSubgraphs == numSubgraph(k))
      }

   }

   test ("[cube,cliques]", Tag("cube.cliques")) {
      val numSubgraph = List(8, 12, 0)

      for (k <- 0 to (numSubgraph.size - 1)) {
         val cliquesFrac = fgraph.cliques
            .set("num_partitions", numPartitions)
            .explore(k)

         val numCliques = cliquesFrac.aggregationCount
         assert(numCliques == numSubgraph(k))
      }
   }

   test ("[cube,cliquesopt]", Tag("cube.cliquesopt")) {
      val numSubgraph = List(8, 12, 0)

      for (k <- 0 to (numSubgraph.size - 1)) {
         val cliqueFrac = fgraph.cliquesKClist
            .set("num_partitions", numPartitions)
            .explore(k)

         val numCliques = cliqueFrac.aggregationCount

         assert(numCliques == numSubgraph(k))
      }
   }

   test ("[maximalcliques,gt]", Tag("maximalcliques.gt")) {
      for (((graph,maxSize),numSubgraphs) <- maximalCliqueGt) {
         val chosenGraph = if (graph == "citeseer-single-label") {
            citeseerSingleLabelGraph
         } else {
            throw new RuntimeException("Unknown graph")
         }

         val maximalCliquesFrac = chosenGraph.maximalCliques
            .explore(maxSize - 1)

         val numMaximalCliques = maximalCliquesFrac.aggregationCount

         assert (numMaximalCliques == numSubgraphs)
      }
   }

   test ("[cube,fsm]", Tag("cube.fsm")) {
      // Critical test
      // Test output for fsm with support 2 for Subgraphs with size 2 to 3
      val support = 2

      // Expected output
      val numFreqPatterns = List(3, 3+4, 3+4+7)

      for (k <- 1 to numFreqPatterns.size) {
         fgraph.set("num_partitions", numPartitions)
         val freqPatterns = fgraph.fsm(support, k).collectAsMap()

         assert(freqPatterns.size == numFreqPatterns(k - 1))
      }
   }

   test ("[cube,gquerying]", Tag("cube.gquerying")) {
      // Expected output
      val numSubgraph = Map("triangles" -> 0, "squares" -> 2)

      // triangles
      val triangle = new FractalGraph("../data/triangle-test.graph",
         fgraph.fractalContext)
      val trianglesFrac = fgraph.gquerying(triangle.asPattern).
         set ("num_partitions", numPartitions).
         explore(2)
      val numTriangles = trianglesFrac.aggregationCount
      assert(numTriangles == numSubgraph("triangles"))

      // squares
      val square = new FractalGraph("../data/square-test.graph",
         fgraph.fractalContext)
      val squaresFrac = fgraph.gquerying(square.asPattern).
         set ("num_partitions", numPartitions).
         explore(3)
      val numSquares = squaresFrac.aggregationCount
      assert (numSquares == numSubgraph("squares"))

   }

   /*
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
      val kwsFrac = fgraphEdgeLabel.keywordSearch(numPartitions, keywords)
      val numSubgraphs = kwsFrac.aggregationLong(0, _ => 1L, _ + _)
      assert (numSubgraphs == 2)
   }
    */

   test ("[sampling.motifs]", Tag("sampling.motifs")) {
      citeseerSingleLabelGraph.set("num_partitions", numPartitions)

      for (k <- Array(3, 4)) {

         // k-motifs
         val motifsCounts = citeseerSingleLabelGraph.motifs2(k)
            .collectAsMap()

         // k-motifs estimate
         val fraction = 0.5
         val motifsCountsEst = citeseerSingleLabelGraph.motifs2(k, fraction)
            .collectAsMap()

         for ((p,gt) <- motifsCounts) {
            val estimate = motifsCountsEst(p) / fraction
            val error = 100 * (gt - estimate).abs / gt.toDouble
            logInfo(s"pattern=${p}; ground-truth=${gt};" +
               s" fraction=${fraction}; estimate=${estimate}" +
               s"; error(%)=${error}")

            // TODO: find a better way to assert correctness for sampling
            assert (error <= 30)
         }
      }
   }

   test ("[sampling.gquerying.triangle]", Tag("sampling.gquerying.triangle")) {
      citeseerSingleLabelGraph.set("num_partitions", numPartitions)
      // ground-truth 1: 3-cliques
      val gt1 = citeseerSingleLabelGraph.cliques.explore(2).aggregationCount

      // ground-truth 2: triangle querying
      val triangle = new FractalGraph("../data/triangle-test.graph", fc)
      val trianglesFrac = citeseerSingleLabelGraph.gquerying(triangle.asPattern).
         explore(2)
      val gt2 = trianglesFrac.aggregationCount

      // sanity check
      assert(gt1 == gt2)

      // 50% estimate
      val fraction = 0.5
      val sample = citeseerSingleLabelGraph.spfractoid(triangle.asPattern, fraction).
         expand(3)
      val estimate = sample.aggregationCount / fraction

      // error
      val error = 100 * (gt1 - estimate).abs / gt1.toDouble
      logInfo(s"ground-truth=${gt1}; fraction=${fraction}; estimate=${estimate}" +
         s"; error(%)=${error}")

      // TODO: find a better way to assert correctness for sampling
      assert(error.round <= 15)
   }

   test ("[sampling.gquerying.square]", Tag("sampling.gquerying.square")) {
      citeseerSingleLabelGraph.set("num_partitions", numPartitions)

      // ground-truth: square querying
      val square = new FractalGraph("../data/q2-square.graph", fc)
      val squares = citeseerSingleLabelGraph.gquerying(square.asPattern).
         explore(3)
      val gt = squares.aggregationCount

      // 10% estimate
      val fraction = 0.1
      val sample = citeseerSingleLabelGraph.spfractoid(square.asPattern, fraction).
         expand(4)
      val estimate = sample.aggregationCount / fraction

      // error
      val error = 100 * (gt - estimate).abs / gt.toDouble
      logInfo(s"ground-truth=${gt}; fraction=${fraction}; estimate=${estimate}" +
         s"; error(%)=${error}")

      // TODO: find a better way to assert correctness for sampling
      assert (error <= 20)
   }

   test ("[citeseer.motifs.gt]", Tag("citeseer.motifs.gt")) {
      for (((graph,numVertices),counts) <- motifsGt) {
         val chosenGraph = if (graph == "citeseer-single-label") {
            citeseerSingleLabelGraph
         } else if (graph == "citeseer") {
            citeseerGraph
         } else {
            throw new RuntimeException(s"Invalid graph string ${graph}")
         }

         chosenGraph.set("num_partitions", numPartitions)

         val motifsMap1 = chosenGraph.motifs2(numVertices).collectAsMap()
         val counts1 = motifsMap1.values.toArray
         java.util.Arrays.sort(counts1)
         assert(java.util.Arrays.equals(counts, counts1))

         val motifsMap2 = chosenGraph.motifspf2(numVertices).collectAsMap()
         assert (motifsMap2.equals(motifsMap1), s"${motifsMap1} ${motifsMap2}")

         val motifsMap3 = chosenGraph.motifspfmcvc2(numVertices).collectAsMap()
         assert (motifsMap3.equals(motifsMap1), s"${motifsMap1} ${motifsMap3}")
      }
   }

   test ("[citeseer.gquerying.gt]", Tag("citeseer.gquerying.gt")) {
      for (((_,query,numVertices),numSubgraphs) <- patternMatchingGt.filter(_._1._1 == "citeseer")) {

         citeseerSingleLabelGraph.set("num_partitions", numPartitions)

         val queryGraph = new FractalGraph(s"../data/${query}.graph",
            citeseerSingleLabelGraph.fractalContext)

         val gqueryingFrac = citeseerSingleLabelGraph
            .gquerying(queryGraph.asPattern)
            .explore(numVertices - 1)

         assert(gqueryingFrac.aggregationCount == numSubgraphs,
            s"query=${query} numVertices=${numVertices}")

         val queryPartialResults = citeseerSingleLabelGraph
            .gqueryingmcvc(queryGraph.asPattern)
         var totalNumSubgraphs = 0L
         for (queryPartialResult <- queryPartialResults) {
            totalNumSubgraphs += queryPartialResult
               .aggregationCountWithCallback(
                  (s,c,cb) => {s.completeMatch(c, c.getPattern, cb)})
         }
         assert(totalNumSubgraphs == numSubgraphs,
            s"MCVC query=${query} numVertices=${numVertices}")
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
            .collectAsMap()
         assert(frequentPatterns.size == numPatterns)

         // pattern-first approach: pattern-matching on every possible pattern
         val frequentPatternsPf = chosenGraph
            .fsmpf(minSupport, Int.MaxValue)
            .collectAsMap()
         assert(frequentPatternsPf.size == numPatterns)
         assert(frequentPatterns.equals(frequentPatternsPf),
            s"${frequentPatterns} ${frequentPatternsPf}")

         // pattern-first approach using MCVC optimization
         val frequentPatternsPf2 = chosenGraph
            .fsmpfmcvc(minSupport, Int.MaxValue)
            .collectAsMap()
         assert(frequentPatternsPf2.size == numPatterns,
            s"${frequentPatterns} ${frequentPatternsPf2}")
         assert(frequentPatterns.equals(frequentPatternsPf2),
            s"${frequentPatterns} ${frequentPatternsPf2}")
      }
   }
}
