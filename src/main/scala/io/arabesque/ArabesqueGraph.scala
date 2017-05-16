package io.arabesque

import io.arabesque.utils.Logging

import java.util.UUID

import io.arabesque.computation._
import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding._
import io.arabesque.graph.MainGraph

import scala.reflect.{classTag, ClassTag}

/**
  * Creates an [[io.arabesque.ArabesqueGraph]] used for calling arabesque graph
  * algorithms
  *
  * @param path  a string indicating the path for input graph
  * @param local TODO
  * @param arab  an [[io.arabesque.ArabesqueContext]] instance
  */
class ArabesqueGraph(
    path: String,
    local: Boolean,
    arab: ArabesqueContext,
    logLevel: String) extends Logging {

  private val uuid: UUID = UUID.randomUUID

  private val mainGraph: MainGraph = {
    import Configuration._
    val config = new SparkConfiguration
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.setMainGraphClass (
      config.getClass (CONF_MAINGRAPH_CLASS, CONF_MAINGRAPH_CLASS_DEFAULT).
      asInstanceOf[Class[_ <: MainGraph]]
    )
    config.createGraph()
  }

  def tmpPath: String = s"${arab.tmpPath}/graph-${uuid}"

  def arabContext: ArabesqueContext = arab

  def this(path: String, arab: ArabesqueContext, logLevel: String) = {
    this (path, false, arab, logLevel)
  }

  private def resultHandler [E <: Embedding : ClassTag] (
      config: SparkConfiguration[E], stepByStep: Boolean = true)
    : ArabesqueResult[E] = {
    config.set ("log_level", logLevel)
    config.setMainGraph (mainGraph)
    new ArabesqueResult [E] (this, config).copy (stepByStep = stepByStep)
  }

  /**
   * Computes all the motifs of a given size
   * {{{
    * import io.arabesque.ArabesqueContext
    *
    * val input_graph = "ArabesqueDir/data/cube.graph"
    * val max_size = 3
    *
    * val arab = new ArabesqueContext(sc)
    *
    * val graph = arab.textFile(input_graph)
    * val res = graph.motif(4)
    *
    * res.embeddings.count
    * res.embeddings.collect
    * }}}
   * @param maxSize number of vertices of the target motifs
   *
   * @return an [[io.arabesque.ArabesqueResult]] carrying odags and embeddings
   */
  def motifs(maxSize: Int): ArabesqueResult[VertexInducedEmbedding] = {
    val config = new SparkConfiguration [VertexInducedEmbedding]
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("output_path", s"${tmpPath}/motifs-${config.getId}")
    config.set ("arabesque.motif.maxsize", maxSize)
    config.set ("computation", "io.arabesque.gmlib.motif.MotifComputation")
    resultHandler (config, false)
  }

  /**
   * Return a motif computation containing:
   * - Sum aggregation with key=Pattern and value=LongWritable
   */
  def motifs: ArabesqueResult[VertexInducedEmbedding] = {
    import org.apache.hadoop.io.LongWritable
    import io.arabesque.pattern.Pattern

    val AGG_MOTIFS = "motifs"
    vertexInducedComputation.withAggregationRegistered [Pattern,LongWritable] (
      AGG_MOTIFS)(
      (v1, v2) => {v1.set (v1.get + v2.get); v1})

  }

  /**
   * Computes all the frequent subgraphs for the given support
   *
    * {{{
    * import io.arabesque.ArabesqueContext
    *
    * val input_graph = "ArabesqueDir/data/cube.graph"
    * val max_size = 2
    * val support = 3
    *
    * val graph = arab.textFile(input_graph)
    * val res = graph.fsm(support, max_size)
    *
    * res.embeddings.count
    * res.embeddings.collect
    * }}}
   *
   * @param support frequency threshold
   * @param maxSize upper bound for embedding exploration
   *
   * @return an [[io.arabesque.ArabesqueResult]] carrying odags and embeddings
   */
  def fsm(support: Int, maxSize: Int): ArabesqueResult[EdgeInducedEmbedding] = {
    val config = new SparkConfiguration [EdgeInducedEmbedding]
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("output_path", s"${tmpPath}/fsm-${config.getId}")
    config.set ("arabesque.fsm.maxsize", maxSize)
    config.set ("arabesque.fsm.support", support)
    config.set ("computation", "io.arabesque.gmlib.fsm.FSMComputation")
    config.set ("master_computation",
      "io.arabesque.gmlib.fsm.FSMMasterComputation")
    resultHandler (config, false)
  }

  /**
   */
  def fsm(support: Int): ArabesqueResult[EdgeInducedEmbedding] = {
    import io.arabesque.gmlib.fsm._
    import io.arabesque.pattern.Pattern
    import io.arabesque.utils.SerializableWritable
    import java.lang.ThreadLocal

    val AGG_SUPPORT = "support"
    edgeInducedComputation { new EdgeProcessFunc {
      @transient lazy val domainSupport = new ThreadLocal [DomainSupport] {
        override def initialValue = new DomainSupport(support)
      }
      def apply (e: EdgeInducedEmbedding, c: Computation[EdgeInducedEmbedding])
        : Unit = {
        domainSupport.get.setFromEmbedding (e)
        c.map(AGG_SUPPORT, e.getPattern, domainSupport.get)
      }
    }}.
    withPatternAggregationFilter ((p,c) => c.readAggregation(AGG_SUPPORT).
      containsKey (p)).
    withMasterCompute { c =>
      if (c.readAggregation (AGG_SUPPORT).getNumberMappings <= 0 &&
        c.getStep > 0) {
        c.haltComputation()
      }
    }.
    withAggregationRegistered [Pattern,DomainSupport] (AGG_SUPPORT,
      new DomainSupportReducer(),
      endAggregationFunction = new DomainSupportEndAggregationFunction())
  }

  /**
   * Counts triangles
    * {{{
    *   import io.arabesque.ArabesqueContext
    *   val input_graph = "ArabesqueDir/data/cube.graph"
    *
    *   val graph = arab.textFile(input_graph)
    *   val res = graph.triangles()
    *
    *   // The cube graph has no triangle
    *   res.embeddings.count()
    *   res.embeddings.collect()
    * }}}
   *
   * @return an [[io.arabesque.ArabesqueResult]] carrying odags and embeddings
   */
  def allStepsTriangles: ArabesqueResult[VertexInducedEmbedding] = {
    val config = new SparkConfiguration [VertexInducedEmbedding]
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("output_path", s"${tmpPath}/triangles-${config.getId}")
    config.set ("computation",
      "io.arabesque.gmlib.triangles.CountingTrianglesComputation")
    resultHandler (config, false)
  }

  /**
   */
  def triangles: ArabesqueResult[VertexInducedEmbedding] = {
    import org.apache.hadoop.io.{IntWritable, LongWritable}
    import io.arabesque.utils.SerializableWritable
    import io.arabesque.aggregation.reductions.LongSumReduction
    import io.arabesque.utils.collection.IntArrayList

    val longUnitSer = new SerializableWritable (new LongWritable(1))
    val AGG_TRIANGLES = "membership"
    vertexInducedComputation { (e,c) =>
      if (e.getNumVertices == 3) {
        val vertices = e.getVertices
        val id = new IntWritable()
        var i = 0
        while (i < 3) {
          id.set (vertices.getUnchecked(i))
          c.map (AGG_TRIANGLES, id, longUnitSer.value)
          i += 1
        }
      }
    }.
    withFilter ((e,c) => e.getNumVertices < 3 ||
      (e.getNumVertices == 3 && e.getNumEdges == 3)).
    withAggregationRegistered [IntWritable,LongWritable] (AGG_TRIANGLES,
      new LongSumReduction)
  }

  /**
   * Computes graph cliques of a given size
   *
    * {{{
    *   import io.arabesque.ArabesqueContext
    *   val input_graph = "ArabesqueDir/data/cube.graph"
    *
    *   val graph = arab.textFile(input_graph)
    *   val res = graph.fsm()
    *
    *   res.embeddings.count()
    *   res.embeddings.collect()
    * }}}
    *
    *
   * @param maxSize target clique size
   *
   * @return an [[io.arabesque.ArabesqueResult]] carrying odags and embeddings
   */
  def cliques(maxSize: Int): ArabesqueResult[VertexInducedEmbedding] = {
    val config = new SparkConfiguration [VertexInducedEmbedding]
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("output_path", s"${tmpPath}/cliques-${config.getId}")
    config.set ("arabesque.clique.maxsize", maxSize)
    config.set ("computation", "io.arabesque.gmlib.clique.CliqueComputation")
    resultHandler (config, false)
  }

  /**
   */
  def cliques: ArabesqueResult[VertexInducedEmbedding] = {
    vertexInducedComputation.
      withFilter ((e,c) =>
          e.getNumEdgesAddedWithExpansion == e.getNumVertices - 1)
  }
 
  /**
   */
  def cliquesPercolation(maxSize: Int)
    : ArabesqueResult[VertexInducedEmbedding] = {
    val config = new SparkConfiguration [VertexInducedEmbedding]
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("output_path", s"${tmpPath}/cliques-${config.getId}")
    config.set ("arabesque.clique.maxsize", maxSize)
    config.set ("computation",
      "io.arabesque.gmlib.cliqueperc.CliquePercComputation")
    resultHandler (config, false)
  }

  /** api for custom computations **/

  def computation [E <: Embedding : ClassTag]: ArabesqueResult[E] = {
    val eClass = classTag[E].runtimeClass
    if (eClass == classOf[VertexInducedEmbedding]) {
      vertexInducedComputation.asInstanceOf[ArabesqueResult[E]]
    } else if (eClass == classOf[EdgeInducedEmbedding]) {
      edgeInducedComputation.asInstanceOf[ArabesqueResult[E]]
    } else {
      throw new RuntimeException (s"Unsupported embedding type ${eClass}")
    }
  }

  def emptyComputation [E <: Embedding : ClassTag]: ArabesqueResult[E] = {
    assert (computation.config.clearComputationContainer)
    computation
  }

  /**
   * Returns a new result with a configurable computation container.
   *
    * {{{
    *   import io.arabesque.ArabesqueContext
    *   val input_graph = "ArabesqueDir/data/cube.graph"
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
    *   res.embeddings.count()
    *   res.embeddings.collect()
    * }}}
    *
    * @param process function that is called for each embedding produced
    *
    * @return an [[io.arabesque.ArabesqueResult]] carrying odags and embeddings
   */
  def edgeInducedComputation(
      process: (EdgeInducedEmbedding,
                Computation[EdgeInducedEmbedding]) => Unit)
    : ArabesqueResult[EdgeInducedEmbedding] = {
    val computation: Computation[EdgeInducedEmbedding] =
      new EComputationContainer(processOpt = Some(process))
    val config = new SparkConfiguration[EdgeInducedEmbedding].
      withNewComputation (computation)
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("output_path", s"${tmpPath}/edge-computation-${config.getId}")
    customComputation [EdgeInducedEmbedding] (config)
  }

  def edgeInducedComputation: ArabesqueResult[EdgeInducedEmbedding] =
    edgeInducedComputation ((_,_) => {})

  /**
   * Returns a new result with a configurable computation container.
   *
    * {{{
    *   import io.arabesque.ArabesqueContext
    *   val input_graph = "ArabesqueDir/data/cube.graph"
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
    *   res.embeddings.count()
    *   res.embeddings.collect()
    * }}}
    *
    * @param process function that is called for each embedding produced
    *
    * @return an [[io.arabesque.ArabesqueResult]] carrying odags and embeddings
   */
  def vertexInducedComputation(
      process: (VertexInducedEmbedding,
                Computation[VertexInducedEmbedding]) => Unit)
    : ArabesqueResult[VertexInducedEmbedding] = {
    val computation: Computation[VertexInducedEmbedding] =
      new VComputationContainer(processOpt = Some(process))
    val config = new SparkConfiguration[VertexInducedEmbedding].
      withNewComputation (computation)
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("output_path", s"${tmpPath}/vertex-computation-${config.getId}")
    customComputation [VertexInducedEmbedding] (config)
  }

  def vertexInducedComputation: ArabesqueResult[VertexInducedEmbedding] =
    vertexInducedComputation ((_,_) => {})

  def customComputation [E <: Embedding: ClassTag] (
      config: SparkConfiguration[E]): ArabesqueResult[E] = {
    resultHandler [E] (config)
  }
}
