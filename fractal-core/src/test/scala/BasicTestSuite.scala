package br.ufmg.cs.systems.fractal

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Tag}

class BasicTestSuite extends FunSuite with BeforeAndAfterAll {
  private val numPartitions: Int = 8
  private val appName: String = "fractal-test"
  private val logLevel: String = "error"

  private var master: String = _
  private var sc: SparkContext = _
  private var fc: FractalContext = _
  private var fgraph: FractalGraph = _
  private var fgraphEdgeLabel: FractalGraph = _

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

  test ("[cube,fsm]", Tag("cube.fsm")) {
    import br.ufmg.cs.systems.fractal.gmlib.fsm.DomainSupport
    import br.ufmg.cs.systems.fractal.pattern.Pattern

    // Critical test
    // Test output for fsm with support 2 for Subgraphs with size 2 to 3
    val support = 2

    // Expected output
    val numFreqPatterns = List(3, 3+4, 3+4+7)

    for (k <- 0 to (numFreqPatterns.size - 1)) {
      val fsmRes = fgraph.fsm(support, k).
        set ("num_partitions", numPartitions)

      val freqPatterns = fsmRes.
        aggregationMap [Pattern,DomainSupport] ("frequent_patterns")

      assert(freqPatterns.size == numFreqPatterns(k))
    }
  }

  test ("[cube,gquerying]", Tag("cube.gquerying")) {
    // Expected output
    val numSubgraph = Map("triangles" -> 0, "squares" -> 2)

    // triangles
    val triangle = new FractalGraph("../data/triangle-test.graph", fgraph.fractalContext)
    val triangles = fgraph.gquerying(triangle).
      set ("num_partitions", numPartitions).
      explore(2)
    assert(triangles.numValidSubgraphs() == numSubgraph("triangles"))

    // squares
    val square = new FractalGraph("../data/square-test.graph", fgraph.fractalContext)
    val squares = fgraph.gquerying(square).
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
}
