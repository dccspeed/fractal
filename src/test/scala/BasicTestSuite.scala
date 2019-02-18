package br.ufmg.cs.systems.fractal

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Tag}

class BasicTestSuite extends FunSuite with BeforeAndAfterAll {
  private val numPartitions: Int = 2
  private val appName: String = "fractal-test"

  private var master: String = _
  private var sampleGraphPath: String = _
  private var sc: SparkContext = _
  private var arab: FractalContext = _
  private var arabGraph: FractalGraph = _

  /** set up spark context */
  override def beforeAll: Unit = {
    master = s"local[${numPartitions}]"
    // spark conf and context
    val conf = new SparkConf().
      setMaster(master).
      setAppName(appName)

    sc = new SparkContext(conf)
    arab = new FractalContext(sc, "warn")

    val loader = classOf[BasicTestSuite].getClassLoader

    sampleGraphPath = "data/cube.graph"
    arabGraph = arab.textFile (sampleGraphPath)

  }

  /** stop spark context */
  override def afterAll: Unit = {
    if (sc != null) {
      sc.stop()
      arab.stop()
    }
  }

  test ("[cube,motifs]", Tag("cube.motifs")) {
    // Test output for motifs for subgraph with size 0 to 3

    // Expected output
    val numSubgraph = List(8, 12, 24)

    for (k <- 0 to (numSubgraph.size - 1)) {
      val motifsRes = arabGraph.motifs.
        set ("num_partitions", numPartitions).
        exploreExp(k)
      val Subgraphs = motifsRes.subgraphs

      assert(Subgraphs.count() == numSubgraph(k))
    }

  }

  test ("[cube,cliques]", Tag("cube.cliques")) {
    // Test output for clique for Subgraphs with size 1 to 3
    // Expected output
    val numSubgraph = List(8, 12, 0)

    for (k <- 0 to (numSubgraph.size - 1)) {
      val cliqueRes = arabGraph.cliques.
        set ("num_partitions", numPartitions).
        exploreExp(k)

      val Subgraphs = cliqueRes.subgraphs

      assert(Subgraphs.count == numSubgraph(k))
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
      val fsmRes = arabGraph.fsm(support, k).
        set ("num_partitions", numPartitions)

      val freqPatterns = fsmRes.
        aggregation [Pattern,DomainSupport] ("frequent_patterns")

      assert(freqPatterns.size == numFreqPatterns(k))
    }

  }
}
