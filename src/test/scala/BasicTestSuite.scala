package io.arabesque

import io.arabesque.conf.{Configuration, SparkConfiguration}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Tag}

class BasicTestSuite extends FunSuite with BeforeAndAfterAll {
  private val numPartitions: Int = 2
  private val appName: String = "fractal-test"

  private var master: String = _
  private var sampleGraphPath: String = _
  private var sc: SparkContext = _
  private var arab: ArabesqueContext = _
  private var arabGraph: ArabesqueGraph = _

  /** set up spark context */
  override def beforeAll: Unit = {
    master = s"local[${numPartitions}]"
    // spark conf and context
    val conf = new SparkConf().
      setMaster(master).
      setAppName(appName)

    sc = new SparkContext(conf)
    arab = new ArabesqueContext(sc, "warn")

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
    // Test output for motifs for embedding with size 0 to 3

    // Expected output
    val numEmbedding = List(8, 12, 24)

    for (k <- 0 to (numEmbedding.size - 1)) {
      val motifsRes = arabGraph.motifs.
        set ("num_partitions", numPartitions).
        exploreExp(k)
      val embeddings = motifsRes.embeddings

      assert(embeddings.count() == numEmbedding(k))
    }

  }

  test ("[cube,cliques]", Tag("cube.cliques")) {
    // Test output for clique for embeddings with size 1 to 3
    // Expected output
    val numEmbedding = List(8, 12, 0)

    for (k <- 0 to (numEmbedding.size - 1)) {
      val cliqueRes = arabGraph.cliques.
        set ("num_partitions", numPartitions).
        exploreExp(k)

      val embeddings = cliqueRes.embeddings

      assert(embeddings.count == numEmbedding(k))
    }
  }


  test ("[cube,fsm]", Tag("cube.fsm")) {
    import io.arabesque.gmlib.fsm.DomainSupport
    import io.arabesque.pattern.Pattern

    // Critical test
    // Test output for fsm with support 2 for embeddings with size 2 to 3
    val support = 2

    // Expected output
    val numFreqPatterns = List(3, 3+4, 3+4+7)

    for (k <- 0 to (numFreqPatterns.size - 1)) {
      val fsmRes = arabGraph.fsm2(support, k).
        set ("num_partitions", numPartitions)

      val freqPatterns = fsmRes.
        aggregation [Pattern,DomainSupport] ("frequent_patterns")

      assert(freqPatterns.size == numFreqPatterns(k))
    }

  }
}
