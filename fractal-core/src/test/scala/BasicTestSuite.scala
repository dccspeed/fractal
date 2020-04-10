package br.ufmg.cs.systems.fractal

import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Tag}

class BasicTestSuite extends FunSuite with BeforeAndAfterAll with Logging {
  private val numPartitions: Int = 8
  private val appName: String = "fractal-test"
  private val logLevel: String = "info"

  private var master: String = _
  private var sc: SparkContext = _
  private var fc: FractalContext = _
  private var fgraph: FractalGraph = _
  private var fgraphEdgeLabel: FractalGraph = _
  private var citeseerGraph: FractalGraph = _

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
    
    citeseerGraph = fc.textFile ("../data/citeseer-single-label.graph")
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
  
  test ("[sampling.motifs.3]", Tag("sampling.motifs.3")) {
    import br.ufmg.cs.systems.fractal.pattern.Pattern
    import org.apache.hadoop.io.LongWritable

    // 3-motifs
    val motifs3 = citeseerGraph.motifs.explore(2)
    val motifsCounts3 = motifs3.aggregationMap [Pattern,LongWritable] ("motifs")

    // 3-motifs estimate
    val fraction = 0.5
    val motifs3est = citeseerGraph.svfractoid(fraction).
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
      assert (error <= 10)
    }
  }
  
  test ("[sampling.motifs.4]", Tag("sampling.motifs.4")) {
    import br.ufmg.cs.systems.fractal.pattern.Pattern
    import org.apache.hadoop.io.LongWritable

    // 4-motifs
    val motifs4 = citeseerGraph.motifs.explore(3)
    val motifsCounts4 = motifs4.aggregationMap [Pattern,LongWritable] ("motifs")

    // 4-motifs estimate
    val fraction = 0.5
    val motifs4est = citeseerGraph.svfractoid(fraction).
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
    val gt1 = citeseerGraph.cliques.explore(2).numValidSubgraphs()

    // ground-truth 2: triangle querying
    val triangle = new FractalGraph("../data/triangle-test.graph", fc)
    val triangles = citeseerGraph.gquerying(triangle).
      set("num_partitions", numPartitions).
      explore(2)
    val gt2 = triangles.numValidSubgraphs()

    // sanity check
    assert (gt1 == gt2)
    
    // 50% estimate
    val fraction = 0.5
    val sample = citeseerGraph.spfractoid(triangle.asPattern, fraction).
      expand(3).
      set("num_partitions", numPartitions)
    val estimate = sample.numValidSubgraphs() / fraction

    // error
    val error = 100 * (gt1 - estimate).abs / gt1.toDouble
    logInfo(s"ground-truth=${gt1}; fraction=${fraction}; estimate=${estimate}" +
      s"; error(%)=${error}")

    // TODO: find a better way to assert correctness for sampling
    assert (error <= 10)
  }

  test ("[sampling.gquerying.square]", Tag("sampling.gquerying.square")) {
    // ground-truth: square querying
    val square = new FractalGraph("../data/q2-square.graph", fc)
    val squares = citeseerGraph.gquerying(square).
      set("num_partitions", numPartitions).
      explore(3)
    val gt = squares.numValidSubgraphs()

    // 10% estimate
    val fraction = 0.1
    val sample = citeseerGraph.spfractoid(square.asPattern, fraction).
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
}
