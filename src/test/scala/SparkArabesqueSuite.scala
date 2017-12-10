package io.arabesque

import io.arabesque.computation._
import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding._
import io.arabesque.utils.ClosureParser

import org.apache.spark.{SparkConf, SparkContext}

import org.scalatest.{BeforeAndAfterAll, ConfigMap, FunSuite, Tag}

class SparkArabesqueSuite extends FunSuite with BeforeAndAfterAll {

  import SparkConfiguration._
  private val master = "local[2]"
  private val appName = "arabesque-spark"

  private var sampleGraphPath: String = _
  private var sc: SparkContext = _
  private var arab: ArabesqueContext = _
  private var arabGraph: ArabesqueGraph = _

  /** set up spark context */
  override def beforeAll(configMap: ConfigMap): Unit = {
    // spark conf and context
    var conf = new SparkConf().
      setMaster(configMap.getWithDefault[String]("spark.master", master)).
      setAppName(appName)

    configMap.iterator.filter(_._1 startsWith "spark.").foreach {
      case (key,value) =>
        conf = conf.set(key, value.toString)
        println(s"${this.getClass.getName}: setting config: ${key} -> ${value}")
    }

    sc = new SparkContext(conf)
    arab = new ArabesqueContext(sc, "warn")

    val graphPath = configMap.getWithDefault[String](
      "arabesque.graph", "sample.graph")
    val loader = classOf[SparkArabesqueSuite].getClassLoader
    val url = loader.getResource(graphPath)
    sampleGraphPath = url.getPath
    arabGraph = arab.textFile (sampleGraphPath)
  }

  /** stop spark context */
  override def afterAll(configMap: ConfigMap): Unit = {
    if (sc != null) {
      sc.stop()
      arab.stop()
    }
  }

  /** measuring elapsed time */
  def time[R](block: => R): R = {  
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println ("Elapsed time: " + ( (t1 - t0) * 10e-6 ) + " ms")
    result
  }

  /** expected results */

  val motifsOracle = Map(
    3 -> 24546
    )

  val cliquesOracle = Map(
    3 -> 1166
    )

  val cliquesPercOracle = Map(
    3 -> 234
    )

  val fsmOracle = Map(
    (100, 3) -> 31414
    )

  val trianglesOracle = scala.io.Source.fromFile (
    classOf[SparkArabesqueSuite].getClassLoader.
      getResource("sample-triangles.txt").getPath
    ).getLines.map (_ split "\\s+").
    map (a => (a(0).toInt, a(1).toInt)).toMap
  
  /** tests */

  test ("[motifs,filter]", Tag("motifs.filter")) { time {
    val motifsRes = arabGraph.motifs.explore(1)
    val filteredMotifsRes = motifsRes.filter (
      (e,c) => e.getVertices contains 3309
    )
    assert (filteredMotifsRes.embeddings.count == 35)
  }}

  test ("[triangles,filter]", Tag("triangles.filter")) { time {
    val trianglesRes = arabGraph.triangles.explore(2)
    val filteredTrianglesRes = trianglesRes.filter (
      (e,c) => e.getVertices contains 3309
    )
    assert (filteredTrianglesRes.embeddings.count == 53)
  }}
  
  test ("[triangles,expand,filter]",
      Tag("triangles.expand.filter")) { time {
    val trianglesRes = arabGraph.triangles.explore(2).expand.explore(1)
    val filteredTrianglesRes = trianglesRes.filter (
      (e,c) => e.getVertices contains 3309
    )
    assert (filteredTrianglesRes.embeddings.count == 9099)
  }}

  test ("[fsm,motifs,cliques,concurrent]",
      Tag("fsm.motifs.cliques.concurrent")) { time {
    import scala.concurrent._
    import scala.concurrent.duration.Duration
    import ExecutionContext.Implicits.global

    val motifsFuture = Future {
      val motifsRes = arabGraph.motifs(3).exploreAll()
      val motifsEmbeddings = motifsRes.embeddings
      assert (motifsEmbeddings.count == motifsOracle(3))
      assert (motifsEmbeddings.distinct.count == motifsOracle(3))
    }

    val fsmFuture = Future {
      val fsmRes = arabGraph.fsm(100, 3).exploreAll()
      val fsmEmbeddings = fsmRes.embeddings
      assert (fsmEmbeddings.count == fsmOracle((100, 3)))
      assert (fsmEmbeddings.distinct.count == fsmOracle((100, 3)))
    }

    val cliquesFromMotifsFuture = Future {
      val motifsRes = arabGraph.motifs.explore(1).cache
      val cliquesRes = motifsRes.cliques.explore(2)
      val trianglesRes = motifsRes.triangles
      assert (cliquesRes.embeddings.count <= cliquesOracle(3))
      assert (trianglesRes.embeddings.count == cliquesOracle(3))
      assert (arabGraph.cliques(3).embeddings.count == cliquesOracle(3))

      val backMotifsRes1 = cliquesRes.explore(-3)
      val backMotifsRes2 = trianglesRes.explore(-1)
      val backMotifsRes1Count = backMotifsRes1.embeddings.distinct.count
      val backMotifsRes2Count = backMotifsRes2.embeddings.distinct.count
      assert (backMotifsRes1Count == backMotifsRes2Count)
      assert (backMotifsRes2Count == motifsRes.embeddings.count)
    }

    Await.result(motifsFuture, Duration.Inf)
    Await.result(fsmFuture, Duration.Inf)
    Await.result(cliquesFromMotifsFuture, Duration.Inf)
  }}
  
  test ("[motifs,odag]", Tag("motifs.odag")) { time {
    val motifsRes = arabGraph.motifs (3).
      set ("comm_strategy", COMM_ODAG_SP)
    val embeddings = motifsRes.embeddings
    assert (embeddings.count == motifsOracle(3))
    assert (embeddings.distinct.count == motifsOracle(3))
  }}

  test ("[motifs,embedding]", Tag("motifs.embedding")) { time {
    val motifsRes = arabGraph.motifs.explore (2).
      set ("comm_strategy", COMM_EMBEDDING)
    val embeddings = motifsRes.embeddings
    assert (embeddings.count == motifsOracle(3))
    assert (embeddings.distinct.count == motifsOracle(3))
  }}

  test ("[motifs,gtag]", Tag("motifs.gtag")) { time {
    val motifsRes = arabGraph.motifs.
      set ("comm_strategy", COMM_GTAG).
      copy(mustSync = true).
      explore(2)
    val embeddings = motifsRes.embeddings
    assert (embeddings.count == motifsOracle(3))
    assert (embeddings.distinct.count == motifsOracle(3))
  }}

  test ("[fsm,odag]", Tag("fsm.odag")) { time {
    val fsmRes = arabGraph.fsm (100, 3).
      set ("comm_strategy", COMM_ODAG_SP)
    val embeddings = fsmRes.embeddings
    assert (embeddings.count == fsmOracle((100, 3)))
    assert (embeddings.distinct.count == fsmOracle((100, 3)))
  }}
  
  test ("[fsm,embedding]", Tag("fsm.embedding")) { time {
    val fsmRes = arabGraph.fsm (100, 3).
      set ("comm_strategy", COMM_EMBEDDING)
    val embeddings = fsmRes.embeddings
    assert (embeddings.count == fsmOracle((100, 3)))
    assert (embeddings.distinct.count == fsmOracle((100, 3)))
  }}

  test ("[fsm,gtag]", Tag("fsm.gtag")) { time {
    import io.arabesque.gmlib.fsm._
    import io.arabesque.pattern.Pattern
    var fsmRes = arabGraph.fsm (100).
      set ("comm_strategy", COMM_GTAG).
      cache
    val embeddings1 = fsmRes.embeddings
    fsmRes = fsmRes.explore(1, 2)
    val embeddings = fsmRes.embeddings.union(embeddings1)
    assert (embeddings.count == fsmOracle((100, 3)))
    assert (embeddings.distinct.count == fsmOracle((100, 3)))
  }}

  test ("[triangles,odag]", Tag("triangles.odag")) { time {
    import org.apache.hadoop.io.{IntWritable, LongWritable}

    val trianglesRes = arabGraph.allStepsTriangles.
      set ("comm_strategy", COMM_ODAG_SP)

    val triangleMemberships = trianglesRes.
      aggregation [IntWritable,LongWritable] ("membership")

    triangleMemberships.foreach { case (v,n) =>
      assert (trianglesOracle(v.get) == n.get)
    }
  }}

  test ("[triangles,embedding]", Tag("triangles.embedding")) { time {
    import org.apache.hadoop.io.{IntWritable, LongWritable}

    val trianglesRes = arabGraph.triangles.
      set ("comm_strategy", COMM_EMBEDDING)

    val triangleMemberships = trianglesRes.
      aggregation [IntWritable,LongWritable] ("membership")

    triangleMemberships.foreach { case (v,n) =>
      assert (trianglesOracle(v.get) == n.get)
    }
  }}

  test ("[cliques,odag]", Tag("cliques.odag")) { time {
    val cliquesRes = arabGraph.cliques (3).
      set ("comm_strategy", COMM_ODAG_SP)
    val embeddings = cliquesRes.embeddings
    assert (embeddings.count == cliquesOracle(3))
    assert (embeddings.distinct.count == cliquesOracle(3))
  }}

  test ("[cliques,embedding]", Tag("cliques.embedding")) { time {
    val cliquesRes = arabGraph.cliques (3).
      set ("comm_strategy", COMM_EMBEDDING)
    val embeddings = cliquesRes.embeddings
    assert (embeddings.count == cliquesOracle(3))
    assert (embeddings.distinct.count == cliquesOracle(3))
  }}

  test ("[cliques percolation]", Tag("cliques.percolation")) { time {
    import io.arabesque.utils.collection.{IntArrayList, UnionFindOps}
    import scala.collection.JavaConverters._
    import org.apache.hadoop.io._

    val maxsize = 3
    val cliquepercRes = arabGraph.cliquesPercolation (maxsize)
    val cliqueAdjacencies = cliquepercRes.
      aggregationStorage [IntArrayList,IntArrayList] ("membership")
    val cliqueAdjacenciesBc = sc.broadcast (cliqueAdjacencies)
    val cliques = cliquepercRes.
      aggregationRDD [IntArrayList,VertexInducedEmbedding] ("cliques")

    val communities = cliques.map { case (repr,e) =>
      val m = cliqueAdjacenciesBc.value
      val key = UnionFindOps.find [IntArrayList] (
        v => m.getValue(v),
        (k,v) => m.aggregateWithReusables (k, v),
        repr.value
      )
      (key, e.value)
    }.reduceByKey { (e1,e2) =>
      e2.getVertices.iterator.asScala.
        foreach (v => if (!(e1.getVertices contains v)) e1.addWord (v))
      e1
    }

    assert (communities.count == cliquesPercOracle(3))
  }}
}
