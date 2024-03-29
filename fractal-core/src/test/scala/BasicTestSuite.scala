package br.ufmg.cs.systems.fractal

import br.ufmg.cs.systems.fractal.computation.SamplingEnumerator
import br.ufmg.cs.systems.fractal.pattern.PatternUtils
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Tag}

import scala.io.Source

class BasicTestSuite extends FunSuite with BeforeAndAfterAll with Logging {
   private val numPartitions: Int = 20
   private val appName: String = "fractal-test"
   private val logLevel: String = "warn"
   private val defaultConfs: Map[String,Any] = Map(
      "num_partitions" -> numPartitions
      ,"ws_internal" -> true
      ,"ws_external" -> true
   )

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
      val in = Source.fromFile("../data/test/motifs-test.gt")
      for (line <- in.getLines()) {
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
      val in = Source.fromFile("../data/test/pattern-matching-test.gt")
      for (line <- in.getLines()) {
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
      val in = Source.fromFile("../data/test/fsm-test.gt")
      for (line <- in.getLines()) {
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
      val in = Source.fromFile("../data/test/maximal-clique-test.gt")
      for (line <- in.getLines()) {
         val toks = line.trim.split(" ")
         val (graph, maxSize, numSubgraphs) = (toks(0), toks(1), toks(2))
         maximalCliqueGt = maximalCliqueGt + ((graph, maxSize.toInt) ->
            numSubgraphs.toLong)
      }
      in.close()
      maximalCliqueGt
   }

   /** set up spark context */
   override def beforeAll(): Unit = {
      master = s"local[${numPartitions}]"
      // spark conf and context
      val conf = new SparkConf()
         .setMaster(master)
         .setAppName(appName)

      sc = new SparkContext(conf)

      sc.setLogLevel(logLevel)
      fc = new FractalContext(sc, logLevel)

      def withDefaultConfs(graph: FractalGraph): FractalGraph = {
         var newGraph = graph
         defaultConfs.foreach(kv => newGraph = newGraph.set(kv._1,kv._2))
         newGraph
      }

      cubeGraph = withDefaultConfs(fc.textFile(
         "../data/cube",
         graphClass = "br.ufmg.cs.systems.fractal.graph.VELabeledMainGraph"))

      citeseerSingleLabelGraph = withDefaultConfs(fc.textFile(
         "../data/citeseer",
         graphClass = "br.ufmg.cs.systems.fractal.graph.UnlabeledMainGraph"))

      citeseerGraph = withDefaultConfs(fc.textFile(
         "../data/citeseer",
         graphClass = "br.ufmg.cs.systems.fractal.graph.VELabeledMainGraph"))
   }

   /** stop spark context */
   override def afterAll(): Unit = {
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
         val vsubgraphsFrac = cubeGraph.vfractoid.extend(1).explore(k)
         val numSubgraphs = vsubgraphsFrac.aggregationCount

         assert(numSubgraphs == numSubgraph(k))
      }
   }

   test("[cube,cliques]", Tag("cube.cliques")) {
      val numSubgraph = List(8, 12, 0)

      cubeGraph.set("num_partitions", numPartitions)

      for (k <- 0 to (numSubgraph.size - 1)) {
         val cliquesFrac = cubeGraph.cliquesPO(k + 1)

         val numCliques = cliquesFrac.aggregationCount
         assert(numCliques == numSubgraph(k))
      }
   }


   test("[cube,cliquesopt]", Tag("cube.cliquesopt")) {
      val numSubgraph = List(8, 12, 0)

      cubeGraph.set("num_partitions", numPartitions)

      for (k <- 0 to (numSubgraph.size - 1)) {
         val cliqueFrac = cubeGraph.cliquesCustomKClist(k + 1)
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
         val freqPatterns = cubeGraph.fsmPO(support, k).collectAsMap()

         assert(freqPatterns.size == numFreqPatterns(k - 1))
      }
   }


   test("[cube,gquerying]", Tag("cube.gquerying")) {
      // Expected output
      val numSubgraph = Map("triangles" -> 0, "squares" -> 2)

      cubeGraph.set("num_partitions", numPartitions)

      // triangles
      val trianglePattern = PatternUtils.fromFS("../data/test/triangle")
      val trianglesFrac = cubeGraph.patternQueryingPA(trianglePattern).
         explore(2)
      val numTriangles = trianglesFrac.aggregationCount
      assert(numTriangles == numSubgraph("triangles"))

      // squares
      val squarePattern = PatternUtils.fromFS("../data/test/square")
      val squaresFrac = cubeGraph.patternQueryingPA(squarePattern).
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
            val frac = chosenGraph.maximalCliquesCustomQuick(maxSize)
            frac.aggregationCount
         }

         assert(numMaximalCliques == numSubgraphs,
            s"SubgraphFirst numVertices=${maxSize}")

         // Pattern-first naive approach
         val numMaximalCliques2 = {
            val frac = chosenGraph.maximalCliquesPA(maxSize)
            frac.aggregationCount
         }

         assert(numMaximalCliques2 == numSubgraphs,
            s"PatternFirst numVertices=${maxSize}")
      }
   }

   test("[sampling.motifs]", Tag("sampling.motifs")) {
      citeseerSingleLabelGraph.set("num_partitions", numPartitions)

      for (k <- Array(3, 4)) {

         // k-motifs
         val motifsCounts = citeseerSingleLabelGraph.motifsPO(k)
            .collectAsMap()

         // k-motifs estimate
         val fraction = 0.5
         val motifsCountsEst = citeseerSingleLabelGraph
            .motifsSamplePO(k, fraction)
            .collectAsMap()

         for ((p, gt) <- motifsCounts) {
            val estimate = motifsCountsEst(p) / fraction
            val error = 100 * (gt - estimate).abs / gt.toDouble
            logInfo(s"pattern=${p}; ground-truth=${gt};" +
               s" fraction=${fraction}; estimate=${estimate}" +
               s"; error(%)=${error}")

            // TODO: find a better way to assert correctness for sampling
         }
      }
   }


   test("[sampling.gquerying.triangle]", Tag("sampling.gquerying.triangle")) {
      citeseerSingleLabelGraph.set("num_partitions", numPartitions)
      // ground-truth 1: 3-cliques
      val gt1 = citeseerSingleLabelGraph.cliquesPO(3).aggregationCount

      // ground-truth 2: triangle querying
      val trianglePattern = PatternUtils.fromFS("../data/test/triangle")
      val trianglesFrac = citeseerSingleLabelGraph
         .patternQueryingPA(trianglePattern).
         explore(2)
      val gt2 = trianglesFrac.aggregationCount

      // sanity check
      assert(gt1 == gt2)

      // 50% estimate
      val fraction = 0.5
      val senumClass = classOf[SamplingEnumerator[PatternInducedSubgraph]]
      val fractionKey = "sampling_fraction"
      val sample = citeseerSingleLabelGraph
         .set(fractionKey, fraction)
         .pfractoid(trianglePattern)
         .extend(3, senumClass)
      val estimate = sample.aggregationCount / fraction

      // error
      val error = 100 * (gt1 - estimate).abs / gt1.toDouble
      logInfo(
         s"ground-truth=${gt1}; fraction=${fraction}; estimate=${estimate}" +
            s"; error(%)=${error}")

      // TODO: find a better way to assert correctness for sampling
   }


   test("[sampling.gquerying.square]", Tag("sampling.gquerying.square")) {
      citeseerSingleLabelGraph.set("num_partitions", numPartitions)

      // ground-truth: square querying
      val squarePattern = PatternUtils.fromFS("../data/test/square")
      squarePattern.setVertexLabeled(false);
      val squares = citeseerSingleLabelGraph.patternQueryingPA(squarePattern)
         .explore(3)
      val gt = squares.aggregationCount

      // 10% estimate
      val fraction = 0.1
      val senumClass = classOf[SamplingEnumerator[PatternInducedSubgraph]]
      val fractionKey = "sampling_fraction"
      val sample = citeseerSingleLabelGraph
         .set(fractionKey, fraction)
         .pfractoid(squarePattern)
         .extend(4, senumClass)
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
               //.set("ws_internal", false)
               //.set("ws_external", false)
         } else if (graph == "citeseer") {
            citeseerGraph
               //.set("ws_internal", false)
               //.set("ws_external", false)
         } else {
            throw new RuntimeException(s"Invalid graph string ${graph}")
         }

         chosenGraph.set("num_partitions", numPartitions)

         val motifsMap1 = chosenGraph.motifsPO(numVertices).collectAsMap()
         val counts1 = motifsMap1.values.toArray
         java.util.Arrays.sort(counts1)
         assert(java.util.Arrays.equals(counts, counts1),
            s"numVertices=${numVertices} ${counts.sum} ${counts1.sum}"
         )

         val motifsMap2 = chosenGraph.motifsPA(numVertices).collectAsMap()
         assert(motifsMap2.equals(motifsMap1),
            s"${motifsMap1.values.sum} ${motifsMap2.values.sum} " +
               s"\n${
                  motifsMap1.filterNot(kv => motifsMap2.contains(kv._1))
               }\n${motifsMap1}\n${motifsMap2}")

         val motifsMap3 = chosenGraph.motifsPAMCVC(numVertices).collectAsMap()
         assert(motifsMap3.equals(motifsMap1),
            s"${motifsMap1.values.sum} ${motifsMap3.values.sum}" +
               s" ${motifsMap1} ${motifsMap3}")
      }
   }

   test("[citeseer.gquerying.gt]", Tag("citeseer.gquerying.gt")) {
      for (((_, query, numVertices), numSubgraphs) <- patternMatchingGt
         .filter(_._1._1 == "citeseer")) {

         val graph = citeseerSingleLabelGraph
            .set("num_partitions", numPartitions)

         val queryGraphPattern = PatternUtils.fromFS(
            s"../data/queries/${query}")
         val gqueryingFrac = graph
            .patternQueryingPA(queryGraphPattern)
            .explore(numVertices - 1)

         assert(gqueryingFrac.aggregationCount == numSubgraphs,
            s"query=${query} numVertices=${numVertices}")

         val queryPartialResults = graph
            .patternQueryingPAMCVC(queryGraphPattern)
         var totalNumSubgraphs = 0L
         for (queryPartialResult <- queryPartialResults) {
            totalNumSubgraphs += queryPartialResult.aggregationCount
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
            .fsmPO(minSupport, Int.MaxValue)
            .collectAsMap()
         assert(frequentPatterns.size == numPatterns)

         // pattern-first approach: pattern-matching on every possible pattern
         val frequentPatternsPf = chosenGraph
            .fsmPA(minSupport, Int.MaxValue)
            .collectAsMap()
         assert(frequentPatternsPf.size == numPatterns,
            s"${minSupport} ${frequentPatterns}")
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
            .fsmPAMCVC(minSupport, Int.MaxValue)
            .collectAsMap()
         assert(frequentPatternsPf2.size == numPatterns,
            s"${frequentPatterns} ${frequentPatternsPf2}")
         assert(frequentPatterns.equals(frequentPatternsPf2),
            s"${frequentPatterns} ${frequentPatternsPf2}")
      }
   }
}
