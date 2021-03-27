package br.ufmg.cs.systems.fractal

import br.ufmg.cs.systems.fractal.subgraph.{PatternInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.ScalaFractalFuncs.CustomSubgraphCallback
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Tag}

import scala.io.Source

class BasicTestSuite extends FunSuite with BeforeAndAfterAll with Logging {
   private val numPartitions: Int = 20
   private val appName: String = "fractal-test"
   private val logLevel: String = "warn"

   private var master: String = _
   private var sc: SparkContext = _
   private var fc: FractalContext = _
   private var cubeGraph: FractalGraph = _
   private var citeseerSingleLabelGraph: FractalGraph = _
   private var citeseerGraph: FractalGraph = _

   /**
    * Ground truth for testing motifs application
    */
   private val motifsGt: Map[(String, Int), Array[Long]] = {
      var motifsGt = Map.empty[(String, Int), Array[Long]]
      val in = Source.fromFile("../data/motifs-test.gt")
      for (line <- in.getLines) {
         val toks = line.trim.split(" ")
         val (graph, numVertices, numMotifs) = (toks(0), toks(1), toks(2))
         val counts = new Array[Long](numMotifs.toInt)
         for (i <- 0 until numMotifs.toInt) counts(i) = toks(i + 3).toLong
         java.util.Arrays.sort(counts)
         motifsGt = motifsGt +
            ((graph, numVertices.toInt) -> counts)
      }
      in.close()
      motifsGt
   }

   /**
    * Ground truth for testing pattern matching application
    */
   private val patternMatchingGt: Map[(String, String, Int), Long] = {
      var patternMatchingGt = Map.empty[(String, String, Int), Long]
      val in = Source.fromFile("../data/pattern-matching-test.gt")
      for (line <- in.getLines) {
         val toks = line.trim.split(" ")
         val (graph, query, numVertices, numSubgraphs) = (toks(0), toks(1),
            toks(2), toks(3))
         patternMatchingGt = patternMatchingGt +
            ((graph, query, numVertices.toInt) -> numSubgraphs.toLong)
      }
      in.close()
      patternMatchingGt
   }

   /**
    * Ground-truth for testing the FSM application
    */
   private val fsmGt: Map[(String, Int), Long] = {
      var fsmGt = Map.empty[(String, Int), Long]
      val in = Source.fromFile("../data/fsm-test.gt")
      for (line <- in.getLines) {
         val toks = line.trim.split(" ")
         val (graph, minSupport, numPatterns) = (toks(0), toks(1), toks(2))
         fsmGt = fsmGt + ((graph, minSupport.toInt) -> numPatterns.toLong)
      }
      in.close()
      fsmGt
   }

   /**
    * Ground-truth for testing the maximal cliques application
    */
   private val maximalCliqueGt: Map[(String, Int), Long] = {
      var maximalCliqueGt = Map.empty[(String, Int), Long]
      val in = Source.fromFile("../data/maximal-clique-test.gt")
      for (line <- in.getLines) {
         val toks = line.trim.split(" ")
         val (graph, maxSize, numSubgraphs) = (toks(0), toks(1), toks(2))
         maximalCliqueGt = maximalCliqueGt + ((graph, maxSize.toInt) ->
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

      cubeGraph = fc.textFile("../data/cube.sc",
         graphClass = "br.ufmg.cs.systems.fractal.graph.VELabeledMainGraph")

      citeseerSingleLabelGraph = fc.textFile(
         "../data/citeseer-single-label.sc",
         graphClass = "br.ufmg.cs.systems.fractal.graph.VELabeledMainGraph")

      citeseerGraph = fc.textFile(
         "../data/citeseer.sc",
         graphClass = "br.ufmg.cs.systems.fractal.graph.VELabeledMainGraph")
   }

   /** stop spark context */
   override def afterAll: Unit = {
      if (sc != null) {
         sc.stop()
         fc.stop()
      }
   }

   test("[cube,vsubgraphs]", Tag("cube.vsubgraphs")) {
      // Test output for motifs for subgraph with size 0 to 3

      // Expected output
      val numSubgraph = List(8, 12, 24)
      cubeGraph.set("num_partitions", numPartitions)

      for (k <- 1 to (numSubgraph.size - 1)) {
         val vsubgraphsFrac = cubeGraph.vfractoid.expand(1).explore(k)
         val numSubgraphs = vsubgraphsFrac.aggregationCount

         assert(numSubgraphs == numSubgraph(k))
      }
   }

   test("[cube,cliques]", Tag("cube.cliques")) {
      val numSubgraph = List(8, 12, 0)

      cubeGraph.set("num_partitions", numPartitions)

      for (k <- 0 to (numSubgraph.size - 1)) {
         val cliquesFrac = cubeGraph.cliquesSF(k + 1)

         val numCliques = cliquesFrac.aggregationCount
         assert(numCliques == numSubgraph(k))
      }
   }


   test("[cube,cliquesopt]", Tag("cube.cliquesopt")) {
      val numSubgraph = List(8, 12, 0)

      cubeGraph.set("num_partitions", numPartitions)

      for (k <- 0 to (numSubgraph.size - 1)) {
         val cliqueFrac = cubeGraph.cliquesKClistSF(k + 1)
         val numCliques = cliqueFrac.aggregationCount
         assert(numCliques == numSubgraph(k))
      }
   }


   test("[cube,fsm]", Tag("cube.fsm")) {
      // Critical test
      // Test output for fsm with support 2 for Subgraphs with size 2 to 3
      val support = 2

      // Expected output
      val numFreqPatterns = List(3, 3 + 4, 3 + 4 + 7)

      for (k <- 1 to numFreqPatterns.size) {
         cubeGraph.set("num_partitions", numPartitions)
         val freqPatterns = cubeGraph.fsmSF(support, k).collectAsMap()

         assert(freqPatterns.size == numFreqPatterns(k - 1))
      }
   }


   test("[cube,gquerying]", Tag("cube.gquerying")) {
      // Expected output
      val numSubgraph = Map("triangles" -> 0, "squares" -> 2)

      cubeGraph.set("num_partitions", numPartitions)

      // triangles
      val triangle = new FractalGraph("../data/triangle-test.graph",
         cubeGraph.fractalContext,
         "br.ufmg.cs.systems.fractal.graph.BasicMainGraph")
      val trianglesFrac = cubeGraph.patternMatchingPF(triangle.asPattern).
         explore(2)
      val numTriangles = trianglesFrac.aggregationCount
      assert(numTriangles == numSubgraph("triangles"))

      // squares
      val square = new FractalGraph("../data/square-test.graph",
         cubeGraph.fractalContext,
         "br.ufmg.cs.systems.fractal.graph.BasicMainGraph")
      val squaresFrac = cubeGraph.patternMatchingPF(square.asPattern).
         explore(3)
      val numSquares = squaresFrac.aggregationCount
      assert(numSquares == numSubgraph("squares"))

   }

   test("[maximalcliques,gt]", Tag("maximalcliques.gt")) {
      for (((graph, maxSize), numSubgraphs) <- maximalCliqueGt) {
         val chosenGraph = if (graph == "citeseer-single-label") {
            citeseerSingleLabelGraph
         } else {
            throw new RuntimeException("Unknown graph")
         }

         // Subgraph-first optimized approach
         val numMaximalCliques = {
            val frac = chosenGraph.maximalCliquesQuickSF(maxSize)
            val callback: CustomSubgraphCallback[VertexInducedSubgraph] =
               (s, c, cb) => {
                  if (s.getNumVertices <= maxSize) {
                     cb.apply(s, c)
                  }
               }
            frac.aggregationCountWithCallback(callback)
         }

         assert(numMaximalCliques == numSubgraphs,
            s"SubgraphFirst numVertices=${maxSize}")

         // Pattern-first naive approach
         val numMaximalCliques2 = {
            var numMaximalCliques = 0L
            val callback: Fractoid[PatternInducedSubgraph] => Unit = frac => {
               numMaximalCliques += frac.aggregationCount
            }
            chosenGraph.maximalCliquesPF(maxSize, callback)
            numMaximalCliques
         }

         assert(numMaximalCliques2 == numSubgraphs,
            s"PatternFirst numVertices=${maxSize}")
      }
   }

   test("[sampling.motifs]", Tag("sampling.motifs")) {
      citeseerSingleLabelGraph.set("num_partitions", numPartitions)

      for (k <- Array(3, 4)) {

         // k-motifs
         val motifsCounts = citeseerSingleLabelGraph.motifsSF(k)
            .collectAsMap()

         // k-motifs estimate
         val fraction = 0.5
         val motifsCountsEst = citeseerSingleLabelGraph
            .motifsSampleSF(k, fraction)
            .collectAsMap()

         for ((p, gt) <- motifsCounts) {
            val estimate = motifsCountsEst(p) / fraction
            val error = 100 * (gt - estimate).abs / gt.toDouble
            logInfo(s"pattern=${p}; ground-truth=${gt};" +
               s" fraction=${fraction}; estimate=${estimate}" +
               s"; error(%)=${error}")

            // TODO: find a better way to assert correctness for sampling
            assert(error <= 30)
         }
      }
   }


   test("[sampling.gquerying.triangle]", Tag("sampling.gquerying.triangle")) {
      citeseerSingleLabelGraph.set("num_partitions", numPartitions)
      // ground-truth 1: 3-cliques
      val gt1 = citeseerSingleLabelGraph.cliquesSF(3).aggregationCount

      // ground-truth 2: triangle querying
      val triangle = new FractalGraph("../data/triangle-test.graph", fc,
         "br.ufmg.cs.systems.fractal.graph.BasicMainGraph")
      val trianglesFrac = citeseerSingleLabelGraph
         .patternMatchingPF(triangle.asPattern).
         explore(2)
      val gt2 = trianglesFrac.aggregationCount

      // sanity check
      assert(gt1 == gt2)

      // 50% estimate
      val fraction = 0.5
      val sample = citeseerSingleLabelGraph
         .spfractoid(triangle.asPattern, fraction).
         expand(3)
      val estimate = sample.aggregationCount / fraction

      // error
      val error = 100 * (gt1 - estimate).abs / gt1.toDouble
      logInfo(
         s"ground-truth=${gt1}; fraction=${fraction}; estimate=${estimate}" +
            s"; error(%)=${error}")

      // TODO: find a better way to assert correctness for sampling
      assert(error.round <= 15)
   }


   test("[sampling.gquerying.square]", Tag("sampling.gquerying.square")) {
      citeseerSingleLabelGraph.set("num_partitions", numPartitions)

      // ground-truth: square querying
      val square = new FractalGraph("../data/q2-square.graph", fc,
         "br.ufmg.cs.systems.fractal.graph.BasicMainGraph")
      val squares = citeseerSingleLabelGraph.patternMatchingPF(square.asPattern)
         .
            explore(3)
      val gt = squares.aggregationCount

      // 10% estimate
      val fraction = 0.1
      val sample = citeseerSingleLabelGraph
         .spfractoid(square.asPattern, fraction).
         expand(4)
      val estimate = sample.aggregationCount / fraction

      // error
      val error = 100 * (gt - estimate).abs / gt.toDouble
      logInfo(
         s"ground-truth=${gt}; fraction=${fraction}; estimate=${estimate}" +
            s"; error(%)=${error}")

      // TODO: find a better way to assert correctness for sampling
      assert(error <= 20)
   }

   test("[citeseer.motifs.gt]", Tag("citeseer.motifs.gt")) {
      for (((graph, numVertices), counts) <- motifsGt) {
         val chosenGraph = if (graph == "citeseer-single-label") {
            citeseerSingleLabelGraph
         } else if (graph == "citeseer") {
            citeseerGraph
         } else {
            throw new RuntimeException(s"Invalid graph string ${graph}")
         }

         chosenGraph.set("num_partitions", numPartitions)

         val motifsMap1 = chosenGraph.motifsSF(numVertices).collectAsMap()
         val counts1 = motifsMap1.values.toArray
         java.util.Arrays.sort(counts1)
         assert(java.util.Arrays.equals(counts, counts1),
            s"numVertices=${numVertices} ${counts.sum} ${counts1.sum}"
         )

         val motifsMap2 = chosenGraph.motifsPF(numVertices).collectAsMap()
         assert(motifsMap2.equals(motifsMap1),
            s"${motifsMap1.values.sum} ${motifsMap2.values.sum} " +
               s"\n${
                  motifsMap1 -- motifsMap2.keys
               }\n${motifsMap1}\n${motifsMap2}")

         val motifsMap3 = chosenGraph.motifsPFMCVC(numVertices).collectAsMap()
         assert(motifsMap3.equals(motifsMap1),
            s"${motifsMap1.values.sum} ${motifsMap3.values.sum}" +
               s"${motifsMap1} ${motifsMap3}")
      }
   }

   test("[citeseer.gquerying.gt]", Tag("citeseer.gquerying.gt")) {
      for (((_, query, numVertices), numSubgraphs) <- patternMatchingGt
         .filter(_._1._1 == "citeseer")) {

         citeseerSingleLabelGraph.set("num_partitions", numPartitions)

         val queryGraph = new FractalGraph(s"../data/${query}.graph",
            citeseerSingleLabelGraph.fractalContext,
            "br.ufmg.cs.systems.fractal.graph.BasicMainGraph")

         val gqueryingFrac = citeseerSingleLabelGraph
            .patternMatchingPF(queryGraph.asPattern)
            .explore(numVertices - 1)

         assert(gqueryingFrac.aggregationCount == numSubgraphs,
            s"query=${query} numVertices=${numVertices}")

         val queryPartialResults = citeseerSingleLabelGraph
            .patternMatchingPFMCVC(queryGraph.asPattern)
         var totalNumSubgraphs = 0L
         for (queryPartialResult <- queryPartialResults) {
            totalNumSubgraphs += queryPartialResult
               .aggregationCountWithCallback(
                  (s, c, cb) => {
                     s.completeMatch(c, c.getPattern, cb)
                  })
         }
         assert(totalNumSubgraphs == numSubgraphs,
            s"MCVC query=${query} numVertices=${numVertices}")
      }
   }


   test("[citeseer.fsm.gt]", Tag("citeseer.fsm.gt")) {
      for (((graph, minSupport), numPatterns) <- fsmGt) {
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
            .fsmSF(minSupport, Int.MaxValue)
            .collectAsMap()
         assert(frequentPatterns.size == numPatterns)

         // pattern-first approach: pattern-matching on every possible pattern
         val frequentPatternsPf = chosenGraph
            .fsmPF(minSupport, Int.MaxValue)
            .collectAsMap()
         assert(frequentPatternsPf.size == numPatterns)
         assert(frequentPatterns.equals(frequentPatternsPf), {
            var debugStr = "\n"
            for (p <- frequentPatterns.keySet) {
               debugStr = s"${debugStr}${p} -> ${frequentPatterns.get(p)} " +
                  s":: ${frequentPatternsPf.get(p)}\n"
            }
            debugStr
         })

         // pattern-first approach using MCVC optimization
         val frequentPatternsPf2 = chosenGraph
            .fsmPFMCVC(minSupport, Int.MaxValue)
            .collectAsMap()
         assert(frequentPatternsPf2.size == numPatterns,
            s"${frequentPatterns} ${frequentPatternsPf2}")
         assert(frequentPatterns.equals(frequentPatternsPf2),
            s"${frequentPatterns} ${frequentPatternsPf2}")
      }
   }
}
