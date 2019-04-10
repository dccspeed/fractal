package br.ufmg.cs.systems.fractal

import br.ufmg.cs.systems.fractal.aggregation._
import br.ufmg.cs.systems.fractal.aggregation.reductions._
import br.ufmg.cs.systems.fractal.computation._
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.graph.{Vertex, Edge}
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util._
import com.koloboke.collect.IntCollection
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Writable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.Map
import scala.reflect.{ClassTag, classTag}

/**
 * Fractal workflow state.
 */
case class Fractoid [S <: Subgraph : ClassTag](
    fractalGraph: FractalGraph,
    private val mustSync: Boolean,
    private val scope: Int,
    step: Int,
    parentOpt: Option[Fractoid[S]],
    config: SparkConfiguration[S],
    private val aggFuncs: Map[String,(
    (S,Computation[S],_ <: Writable) => _ <: Writable,
    (S,Computation[S],_ <: Writable) => _ <: Writable)]) extends Logging {

  def this(arabGraph: FractalGraph, config: SparkConfiguration[S]) = {
    this(arabGraph, false, 0, 0, None, config, Map.empty)
  }

  def sparkContext: SparkContext = fractalGraph.fractalContext.sparkContext

  if (!config.isInitialized) {
    config.initialize(isMaster = true)
  }

  /**
   * Size of the chain of computations in this result
   */
  lazy val numComputations: Int = {
    var currOpt = Option(getComputationContainer[S])
    var nc = 0
    while (currOpt.isDefined) {
      nc += 1
      currOpt = currOpt.get.nextComputationOpt.
        asInstanceOf[Option[ComputationContainer[S]]]
    }
    nc
  }

  /**
   * Number of computations accumulated, including parents. This value starts
   * with zero, thus, depth=0 implies in numComputations=1
   */
  lazy val depth: Int = {
    @scala.annotation.tailrec
    def findDepthRec(r: Fractoid[S], accum: Int): Int = {
      if (!r.parentOpt.isDefined) {
        accum + r.numComputations
      } else {
        findDepthRec(r.parentOpt.get, accum + r.numComputations)
      }
    }
    findDepthRec(this, -1)
  }

  /**
   * Lazy evaluation for the results
   */
  private var masterEngineOpt: Option[SparkMasterEngine[S]] = None

  private def masterEngine: SparkMasterEngine[S] = synchronized {
    masterEngineOpt match {
      case None =>

        var _masterEngine = parentOpt match {
          case Some(parent) =>
            if (parent.masterEngine.next) {
              SparkMasterEngine [S] (sparkContext, config, parent.masterEngine)
            } else {
              masterEngineOpt = Some(parent.masterEngine)
              return parent.masterEngine
            }

          case None =>
            SparkMasterEngine [S] (sparkContext, config)
        }

        assert (_masterEngine.superstep == this.step,
          s"masterEngineNext=${_masterEngine.next}" +
          s" masterEngineStep=${_masterEngine.superstep} thisStep=${this.step}")

        logInfo (s"Computing ${this}. Engine: ${_masterEngine}")
        _masterEngine.next

        _masterEngine.finalizeComputation
        masterEngineOpt = Some(_masterEngine)
        _masterEngine

      case Some(_masterEngine) =>
        _masterEngine
    }
  }

  def compute(): Map[String,Long] = {
    masterEngine.aggAccums.map{case (k,v) => (k, v.value.longValue)}
  }

  /**
   * Output: Subgraphs
   */
  private var SubgraphsOpt: Option[RDD[ResultSubgraph[_]]] = None

  def subgraphs: RDD[ResultSubgraph[_]] = subgraphs((_, _) => true)

  def subgraphs(shouldOutput: (S,Computation[S]) => Boolean)
    : RDD[ResultSubgraph[_]] = {
    if (config.confs.contains(SparkConfiguration.COMPUTATION_CONTAINER)) {
      val thisWithOutput = withOutput(shouldOutput).set(
        "output_path",
        s"${config.getOutputPath}-${step}"
        )
      //thisWithOutput.config.setOutputPath(
      //  s"${thisWithOutput.config.getOutputPath}-${step}")

      logInfo (s"Output to get Subgraphs: ${this} ${thisWithOutput}")
      thisWithOutput.masterEngine.getSubgraphs
    } else {
      SubgraphsOpt match {
        case None if config.isOutputActive =>
          val _Subgraphs = masterEngine.getSubgraphs
          SubgraphsOpt = Some(_Subgraphs)
          _Subgraphs

        case Some(_Subgraphs) if config.isOutputActive =>
          _Subgraphs

        case _ =>
          masterEngineOpt = None
          SubgraphsOpt = None
          config.set ("output_active", true)
          subgraphs
      }
    }
  }

  def internalSubgraphs: RDD[S] = internalSubgraphs((_,_) => true)

  def internalSubgraphs(
      shouldOutput: (S,Computation[S]) => Boolean): RDD[S] = {
    if (config.confs.contains(SparkConfiguration.COMPUTATION_CONTAINER)) {
      val thisWithOutput = withOutput(shouldOutput).set(
        "output_path",
        s"${config.getOutputPath}-${step}"
        )
      logInfo (s"Output to get internalSubgraphs: ${this} ${thisWithOutput}")
      val configBc = thisWithOutput.masterEngine.configBc
      thisWithOutput.masterEngine.getSubgraphs.
        map (_.toInternalSubgraph [S] (configBc.value))
    } else {
      throw new RuntimeException(s"Not supported yet for internal Subgraphs")
    }
  }

  /**
   * Registered aggregations
   */
  def registeredAggregations: Array[String] = {
    config.getAggregationsMetadata.map (_._1).toArray
  }

  /**
   * Get aggregation mappings defined by the user or empty if it does not exist
   */
  def aggregationMap [K <: Writable, V <: Writable](name: String,
                                                    shouldAggregate: (S,Computation[S]) => Boolean): Map[K,V] = {
    withAggregation [K,V] (name, shouldAggregate).aggregationMap (name)
  }

  /**
   * Get aggregation mappings defined by the user or empty if it does not exist
   */
  def aggregationMap [K <: Writable, V <: Writable](name: String): Map[K,V] = {
    val aggValue = aggregationStorage [K,V] (name)
    if (aggValue == null) Map.empty[K,V]
    else aggValue.getMapping
  }

  /**
   * Get aggregation mappings defined by the user or empty if it does not exist
   */
  def aggregationStorage [K <: Writable, V <: Writable] (name: String)
    : AggregationStorage[K,V] = {
    masterEngine.getAggregatedValue [AggregationStorage[K,V]] (name)
  }

  /*
   * Get aggregations defined by the user as an RDD or empty if it does not
   * exist.
   */
  def aggregation [K <: Writable, V <: Writable](name: String,
                                                 shouldAggregate: (S,Computation[S]) => Boolean)
    : RDD[(SerializableWritable[K],SerializableWritable[V])] = {
    sparkContext.parallelize (aggregationMap [K,V] (name, shouldAggregate).toSeq.
    map {
      case (k,v) => (new SerializableWritable(k), new SerializableWritable(v))
    }, config.numPartitions)
  }

  /*
   * Get aggregations defined by the user as an RDD or empty if it does not
   * exist.
   */
  def aggregation [K <: Writable, V <: Writable](name: String)
    : RDD[(SerializableWritable[K],SerializableWritable[V])] = {
    sparkContext.parallelize (aggregationMap [K,V] (name).toSeq.map {
      case (k,v) => (new SerializableWritable(k), new SerializableWritable(v))
    }, config.numPartitions)
  }

  /**
   * Saves subgraphs as sequence files (HDFS):
   * key=org.apache.hadoop.io.NullWritable value=ResultSubgraph
   * Behavior:
   *  - If at this point no computation was performed we just configure
   *  the execution engine and force the computation(count action)
   *  - Otherwise we rename the Subgraphs path to *path* and clear the
   *  Subgraphs RDD variable, which will force the creation of a new RDD with
   *  the corrected path.
   *
   * @param path hdfs (hdfs://) or local (file://) path
   */
  def saveSubgraphsAsSequenceFile(path: String): Unit = SubgraphsOpt match {
    case None =>
      logInfo ("no subgraphs found, computing them ... ")
      config.setOutputPath (path)
      subgraphs.count

    case Some(_Subgraphs) =>
      logInfo (
        s"found results, renaming from ${config.getOutputPath} to ${path}")
      val fs = FileSystem.get(sparkContext.hadoopConfiguration)
      fs.rename (new Path(config.getOutputPath), new Path(path))
      if (config.getOutputPath != path) SubgraphsOpt = None
      config.setOutputPath (path)

  }

  /**
   * Saves the subgraphs as text
   *
   * @param path hdfs(hdfs://) or local(file://) path
   */
  def saveSubgraphsAsTextFile(path: String): Unit = {
    subgraphs.
      map (emb => emb.words.mkString(" ")).
      saveAsTextFile (path)
  }

  def explore(n: Int): Fractoid[S] = {
    var currResult = this
    var results: List[Fractoid[S]] = List(currResult)
    var numResults = 1
    while (currResult.parentOpt.isDefined) {
      currResult = currResult.parentOpt.get
      results = currResult :: results
      numResults += 1
    }

    var i = 0
    var j = numResults
    currResult = this
    while (i < n) {
      results.foreach { r =>
        currResult = currResult.handleNextResult(r)
          //.copy(step = j)
        j += 1
      }
      i += 1
    }

    currResult
  }

  /**
   * This function will handle to the user a new result with a new configuration
   * NOTE: The configuration will make changes to the current scope, which may
   * include several steps
   *
   * @param key id of the configuration
   * @param value value of the new configuration
   *
   * @return new result
   */
  def set(key: String, value: Any): Fractoid[S] = {

    def setRec(curr: Fractoid[S]): Fractoid[S] = {
      if (curr.scope == this.scope) {
        val parent = if (curr.parentOpt.isDefined) {
          setRec(curr.parentOpt.get)
        } else {
          null
        }
        curr.copy (config = curr.config.withNewConfig (key,value),
          parentOpt = Option(parent))
      } else {
        curr
      }
    }

    setRec(this)
  }

  /**
   * This function will handle to the user a new result without a configuration
   * NOTE: The configuration will make changes to the current scope, which may
   * include several steps
   *
   * @param key id of the configuration
   *
   * @return new result
   */
  def unset(key: String): Fractoid[S] = {

    def unsetRec(curr: Fractoid[S]): Fractoid[S] = {
      if (curr.scope == this.scope) {
        val parent = if (curr.parentOpt.isDefined) {
          unsetRec(curr.parentOpt.get)
        } else {
          null
        }
        curr.copy (config = curr.config.withoutConfig (key),
          parentOpt = Option(parent))
      } else {
        curr
      }
    }

    unsetRec(this)
  }

  /**
   * This function will handle to the user a new result with new configurations
   *
   * @param configMap new configurations as a map (configName,configValue)
   *
   * @return new result
   */
  def set(configMap: Map[String,Any]): Fractoid[S] = {
    this.copy (config = config.withNewConfig (configMap))
  }

  /**
   * Auxiliary function for handling computation containers that were not set in
   * this result
   */
  private def getComputationContainer [S <: Subgraph]
    : ComputationContainer[S] = {
    try {
      var container: Computation[S] = config.computationContainer[S]
      container.asInstanceOf[ComputationContainer[S]]
    } catch {
      case e: RuntimeException =>
        logWarning (s"No computation container was set." +
          s" Please start with 'edgeInducedComputation' or" +
          s" 'vertexInducedComputation' from fractalGraph." +
          s" Error message: ${e.getMessage}")
        return null
    }
  }

  /**
   * Handle the creation of a next result.
   */
  private def handleNextResult(result: Fractoid[S],
                               newConfig: SparkConfiguration[S] = config)
    : Fractoid[S] = if (result.mustSync) {
    logInfo (s"Adding sync barrier between ${this} and ${result}")
    result.copy(scope = this.scope + 1,
      step = this.step + 1, parentOpt = Some(this))
  } else {
    logInfo (s"Next result. Appending ${result} to ${this}")
    val nextContainer = result.getComputationContainer[S]
    withNextComputation (nextContainer, newConfig)
  }

  private def emptyComputation: Fractoid[S] = {
    emptyComputation(Primitive.None)
  }

  /**
   * Create an empty computation that inherits all this result's configurations
   * but the computation container itself
   */
  private def emptyComputation(p: Primitive): Fractoid[S] = {
    val ec = config.confs.get(SparkConfiguration.COMPUTATION_CONTAINER) match {
      case Some(cc: ComputationContainer[_]) =>
        this.set(
          SparkConfiguration.COMPUTATION_CONTAINER,
          cc.asLastComputation.clear().withPrimitive(p))
      case _ =>
        this
    }
    ec.unset(SparkConfiguration.MASTER_COMPUTATION_CONTAINER)
  }

  private def withFirstComputation: Fractoid[S] = {
    val computation = {
      val sclass = classTag[S].runtimeClass
      if (sclass == classOf[VertexInducedSubgraph]) {
        new VComputationContainer(processOpt = Option(null),
          primitiveOpt = Option(Primitive.E))
      } else if (sclass == classOf[EdgeInducedSubgraph]) {
        new EComputationContainer(processOpt = Option(null),
          primitiveOpt = Option(Primitive.E))
      } else if (sclass == classOf[PatternInducedSubgraph]) {
        val pattern = config.confs("pattern").asInstanceOf[Pattern]
        new VEComputationContainer(processOpt = Option(null),
          patternOpt = Option(pattern), primitiveOpt = Option(Primitive.E))
      } else {
        throw new RuntimeException(s"Unsupported subgraph type ${sclass}")
      }
    }

    this.copy(config = config.withNewComputation(
      computation.asInstanceOf[Computation[S]]))
  }

  /****** Fractal Scala API: High Level API ******/

  /**
   * Perform *n* expansion iterations
   *
   * @param n number of expansions
   * @return new result
   */
  def expand(n: Int): Fractoid[S] = {
    var curr = this
    logInfo(s"ExpandBefore ${curr}")
    for (i <- 0 until n) {

      // first computation, create a new computation
      if (getComputationContainer[S] == null) {
        curr = curr.withFirstComputation

      // computation exists, append to the current one
      } else {
        val expandComp = emptyComputation.
          withExpandCompute(null).
          copy(mustSync = false)
        curr = handleNextResult(expandComp)
      }
    }
    logInfo(s"ExpandAfter ${curr}")
    curr
  }

  /**
   * Perform a expansion step in a particular way, defined by
   * *getPossibleExtensions*
   *
   * @param getPossibleExtensions function that receives an subgraph and
   * returns zero or more Subgraphs (collection)
   * @return new result
   */
  def extend(getPossibleExtensions: (S,Computation[S]) => IntCollection)
    : Fractoid[S] = {

    val curr = if (getComputationContainer[S] == null) {
      this.withFirstComputation
    } else {
      this
    }

    val newConfig = curr.config.withNewComputation (
      curr.getComputationContainer[S].
      withNewFunctions (getPossibleExtensionsOpt = Option(getPossibleExtensions)))
    curr.copy (config = newConfig)
  }

  /**
   * Filter the existing subgraphs based on a function
   *
   * @param filter function that decides whether an subgraph should be kept or
   * discarded
   * @return new result
   */
  def filter(filter: (S,Computation[S]) => Boolean): Fractoid[S] = {
    ClosureCleaner.clean(filter)
    val filterComp = emptyComputation(Primitive.F).
      withExpandCompute((e,c) => c.bypass(e)).
      withFilter(filter)
    handleNextResult(filterComp)
  }

  /**
   * Filter the existing subgraphs based on a aggregation
   *
   * @param filter function that decides whether an subgraph should be kept or
   * discarded
   * @return new result
   */
  def filter [K <: Writable : ClassTag, V <: Writable : ClassTag](
      agg: String)(
      filter: (S,AggregationStorage[K,V]) => Boolean): Fractoid[S] = {

    val filterFunc = (e: S, c: Computation[S]) => {
      filter(e, c.readAggregation(agg))
    }

    val filterComp = emptyComputation.
      withExpandCompute((e,c) => Iterator(e)).
      withFilter(filterFunc).
      copy(mustSync = true)

    handleNextResult(filterComp)
  }

  /**
   * Register an aggregation and include the aggregation map to the existing
   * Subgraphs.
   *
   * @param name custom name of this aggregation --> this is used later for
   * retrieving the aggregation results
   * @param aggregationKey function that extracts the key from the subgraph
   * @param aggregationValue function that extracts the value from the subgraph
   * @param reductionFunction function used to reduce the values
   *
   * @return new result
   */
  def aggregate [K <: Writable : ClassTag, V <: Writable : ClassTag] (
      name: String,
      aggregationKey: (S,Computation[S],K) => K,
      aggregationValue: (S,Computation[S],V) => V,
      reductionFunction: (V,V) => V,
      endAggregationFunction: EndAggregationFunction[K,V] = null,
      isIncremental: Boolean = false
    ): Fractoid[S] = {
    withAggregationRegistered(
      name,
      aggregationKey = aggregationKey,
      aggregationValue = aggregationValue,
      reductionFunction = new ReductionFunctionContainer(reductionFunction),
      endAggregationFunction = endAggregationFunction,
      isIncremental = isIncremental).
    withAggregation [K,V] (name, (_,_) => true)
  }

  /**
   * Register an aggregation and include the aggregation map to the existing
   * Subgraphs.
   *
   * @param name custom name of this aggregation --> this is used later for
   * retrieving the aggregation results
   * @param reductionFunction function used to reduce the values
   *
   * @return new result
   */
  def aggregateAll [K <: Writable : ClassTag, V <: Writable : ClassTag] (
      name: String,
      func: (S,Computation[S]) => Iterator[(K,V)],
      reductionFunction: (V,V) => V): Fractoid[S] = {
    withAggregationRegisteredIterator [K,V] (
      name,
      reductionFunction = new ReductionFunctionContainer(reductionFunction)).
    withAggregationIterator(name, func)
  }

  /**
    * Graph reduction filter for vertices
    * @param vfilter
    * @tparam V
    * @return new fractoid with graph filtering
    */
  def vfilter [V] (vfilter: Vertex[V] => Boolean): Fractoid[S] = {
    val vpred = new VertexFilterFunc[V] {
      override def test(v: Vertex[V]): Boolean = vfilter(v)
    }

    if (getComputationContainer[S] == null) {
      withFirstComputation.
        withExpandCompute((e,c) => c.bypass(e)).
        set("vfilter", vpred)
    } else {
      val filterComp = emptyComputation.
        withExpandCompute((e,c) => c.bypass(e)).
        set("vfilter", vpred).
        copy(mustSync = true)
      handleNextResult(filterComp)
    }
  }

  /**
    * Graph reduction filter for edges
    * @param efilter
    * @tparam E
    * @return new fractoid with graph filtering
    */
  def efilter [E] (efilter: Edge[E] => Boolean): Fractoid[S] = {
    val epred = new EdgeFilterFunc[E] {
      override def test(e: Edge[E]): Boolean = efilter(e)
    }

    if (getComputationContainer[S] == null) {
      withFirstComputation.
        withExpandCompute((e, c) => c.bypass(e)).
        set("efilter", epred)
    } else {
      val filterComp = emptyComputation.
        withExpandCompute((e, c) => c.bypass(e)).
        set("efilter", epred).
        copy(mustSync = true)
      handleNextResult(filterComp)
    }
  }

  /****** Fractal Scala API: ComputationContainer ******/

  /**
   * Updates the process function of the underlying computation container.
   *
   * @param process process function to be applied to each subgraph produced
   *
   * @return new result
   */
  private def withProcess (process: (S,Computation[S]) => Unit): Fractoid[S] = {
    val newComp = getComputationContainer[S].withNewFunctions (
      processOpt = Option(process))
    val newConfig = config.withNewComputation (newComp)
    this.copy (config = newConfig)
  }

  /**
   * Append a body function to the process
   *
   * @param func function to be appended
   *
   * @return new result
   */
  private def withProcessInc (func: (S,Computation[S]) => Unit): Fractoid[S] = {
    // get the current process function
    val oldProcess = getComputationContainer[S].processOpt match {
      case Some(process) => process
      case None => (e: S, c: Computation[S]) => {}
    }

    // incremental process
    val process = (e: S, c: Computation[S]) => {
      oldProcess (e, c)
      func (e, c)
    }

    withProcess(process)
  }

  /**
   * Updates the filter function of the underlying computation container.
   *
   * @param filter filter function that determines whether Subgraphs must be
   * further processed or not.
   *
   * @return new result
   */
  private def withFilter (filter: (S,Computation[S]) => Boolean): Fractoid[S] = {
    val newConfig = config.withNewComputation (
      getComputationContainer[S].withNewFunctions (filterOpt = Option(filter)))
    this.copy (config = newConfig)
  }

  /**
   * Updates the word filter function of the underlying computation container.
   *
   * @param filter filter function that determines whether an extension must be
   * further processed or not.
   *
   * @return new result
   */
  private def withWordFilter (
      filter: WordFilterFunc[S]): Fractoid[S] = {

    val newConfig = config.withNewComputation (
      getComputationContainer[S].
      withNewFunctions (wordFilterOpt = Option(filter)))
    this.copy (config = newConfig)
  }

  /**
   * Updates the shouldOutput function of the underlying computation container.
   *
   * @param shouldOutput function that determines whether we should output the
   * subgraph or not
   */
  private def withOutput (shouldOutput: (S,Computation[S]) => Boolean)
    : Fractoid[S] = {

    withProcessInc (
      (e: S, c: Computation[S]) => {
        if (shouldOutput(e,c)) {
          c.output(e)
        }
      }
    )
  }

  /**
   * Include an aggregation map into the process function
   *
   * @param name aggregation name
   * @param func function that returns aggregation pairs
   *
   * @return new result
   */
  private def withAggregationIterator [K <: Writable, V <: Writable] (name: String,
      func: (S,Computation[S]) => Iterator[(K,V)]): Fractoid[S] = {

    withProcessInc (
      (e: S, c: Computation[S]) => {
        val iter = func(e, c)
        while (iter.hasNext()) {
          val kv = iter.next()
          c.map(name, kv._1, kv._2)
        }
      }
    )
  }

  /**
   * Include an aggregation map into the process function
   *
   * @param name aggregation name
   * @param shouldAggregate condition for aggregating an subgraph
   *
   * @return new result
   */
  private def withAggregation [K <: Writable, V <: Writable] (name: String,
      shouldAggregate: (S,Computation[S]) => Boolean): Fractoid[S] = {

    if (!aggFuncs.get(name).isDefined) {
      logWarning (s"Unknown aggregation ${name}." +
        s" Please register it first with *withAggregationRegistered*")
      return this
    }

    val (_aggregationKey, _aggregationValue) = aggFuncs(name)
    val (aggregationKey, aggregationValue) = (
      _aggregationKey.asInstanceOf[(S,Computation[S],K) => K],
      _aggregationValue.asInstanceOf[(S,Computation[S],V) => V]
      )

    withProcessInc (
      (e: S, c: Computation[S]) => {
        // TODO: remove shouldAggregate properly
        //if (shouldAggregate(e,c)) {
          val aggStorage = c.getAggregationStorage[K,V](name)
          val k = aggregationKey(e, c, aggStorage.reusableKey())
          val v = aggregationValue(e, c, aggStorage.reusableValue())
          aggStorage.aggregateWithReusables(k, v)
          //c.map (name, aggregationKey(e,c), aggregationValue(e,c))
        //}
      }
    )
  }

  /**
   * Updates the getPossibleExtensions function of the underlying computation
   * container.
   *
   * @param func that returns the possible extensions.
   *
   * @return new result
   */
  private def withGetPossibleExtensions (func: (S,Computation[S]) => IntCollection)
    : Fractoid[S] = {
    val newConfig = config.withNewComputation (
      getComputationContainer[S].withNewFunctions (
        getPossibleExtensionsOpt = Option(func)))
    this.copy (config = newConfig)
  }

  /**
   * Updates the init function of the underlying computation
   * container.
   *
   * @param init initialization function for the computation
   *
   * @return new result
   */
  private def withInit (init: (Computation[S]) => Unit): Fractoid[S] = {
    val newConfig = config.withNewComputation (
      getComputationContainer[S].withNewFunctions (initOpt = Option(init)))
    this.copy (config = newConfig)
  }

  /**
   * Updates the initAggregations function of the underlying computation
   * container.
   *
   * @param initAggregations function that initializes the aggregations for the
   * computation
   *
   * @return new result
   */
  private def withInitAggregations (initAggregations: (Computation[S]) => Unit)
    : Fractoid[S] = {
    val newConfig = config.withNewComputation (
      getComputationContainer[S].withNewFunctions (
        initAggregationsOpt = Option(initAggregations)))
    this.copy (config = newConfig)
  }

  /**
   * Adds a new aggregation to the computation
   *
   * @param name identifier of this new aggregation
   * @param reductionFunction the function that aggregates two values
   * @param endAggregationFunction the function that is applied at the end of
   * each local aggregation, in the workers
   * @param persistent whether this aggregation must be persisted in each
   * superstep or not
   * @param aggStorageClass custom aggregation storage implementation
   *
   * @return new result
   */
  private def withAggregationRegisteredIterator [
        K <: Writable : ClassTag, V <: Writable: ClassTag
      ] (
      name: String,
      reductionFunction: ReductionFunction[V],
      endAggregationFunction: EndAggregationFunction[K,V] = null,
      persistent: Boolean = false,
      aggStorageClass: Class[_ <: AggregationStorage[K,V]] =
        classOf[AggregationStorage[K,V]],
      isIncremental: Boolean = false)
    : Fractoid[S] = {

    // TODO: check whether this aggregation is already registered and act
    // properly

    // if the user specifies *Pattern* as the key, we must find the concrete
    // implementation within the Configuration before registering the
    // aggregation
    val _keyClass = implicitly[ClassTag[K]].runtimeClass
    val keyClass = if (_keyClass == classOf[Pattern]) {
      config.getPatternClass().asInstanceOf[Class[K]]
    } else {
      _keyClass.asInstanceOf[Class[K]]
    }
    val valueClass = implicitly[ClassTag[V]].runtimeClass.asInstanceOf[Class[V]]

    // get the old init aggregations function in order to compose it
    val oldInitAggregation = getComputationContainer[S].
    initAggregationsOpt match {
      case Some(initAggregations) => initAggregations
      case None => (c: Computation[S]) => {}
    }

    // construct an incremental init aggregations function
    val initAggregations = (c: Computation[S]) => {
      oldInitAggregation (c) // init aggregations so far
      c.getConfig().registerAggregation (name, aggStorageClass, keyClass,
        valueClass, persistent, reductionFunction, endAggregationFunction,
        isIncremental)
    }

    withInitAggregations (initAggregations)
  }

  /**
   * Adds a new aggregation to the computation
   *
   * @param name identifier of this new aggregation
   * @param reductionFunction the function that aggregates two values
   * @param endAggregationFunction the function that is applied at the end of
   * each local aggregation, in the workers
   * @param persistent whether this aggregation must be persisted in each
   * superstep or not
   * @param aggStorageClass custom aggregation storage implementation
   *
   * @return new result
   */
  private def withAggregationRegistered [
        K <: Writable : ClassTag, V <: Writable: ClassTag
      ] (
      name: String,
      reductionFunction: ReductionFunction[V],
      endAggregationFunction: EndAggregationFunction[K,V] = null,
      persistent: Boolean = false,
      aggStorageClass: Class[_ <: AggregationStorage[K,V]] =
        classOf[AggregationStorage[K,V]],
      aggregationKey: (S, Computation[S], K) => K =
        (e: S, c: Computation[S], k: K) => null.asInstanceOf[K],
      aggregationValue: (S, Computation[S], V) => V =
        (e: S, c: Computation[S], v: V) => null.asInstanceOf[V],
      isIncremental: Boolean = false)
    : Fractoid[S] = {

    // TODO: check whether this aggregation is already registered and act
    // properly

    // if the user specifies *Pattern* as the key, we must find the concrete
    // implementation within the Configuration before registering the
    // aggregation
    val _keyClass = implicitly[ClassTag[K]].runtimeClass
    val keyClass = if (_keyClass == classOf[Pattern]) {
      config.getPatternClass().asInstanceOf[Class[K]]
    } else {
      _keyClass.asInstanceOf[Class[K]]
    }
    val valueClass = implicitly[ClassTag[V]].runtimeClass.asInstanceOf[Class[V]]

    // get the old init aggregations function in order to compose it
    val oldInitAggregation = getComputationContainer[S].
    initAggregationsOpt match {
      case Some(initAggregations) => initAggregations
      case None => (c: Computation[S]) => {}
    }

    // construct an incremental init aggregations function
    val initAggregations = (c: Computation[S]) => {
      oldInitAggregation (c) // init aggregations so far
      c.getConfig().registerAggregation (name, aggStorageClass, keyClass,
        valueClass, persistent, reductionFunction, endAggregationFunction,
        isIncremental)
    }

    withInitAggregations (initAggregations).copy (
      aggFuncs = aggFuncs ++ Map(name -> (aggregationKey, aggregationValue))
    )
  }

  /**
   * Adds a new aggregation to the computation
   *
   * @param name identifier of this new aggregation
   * @param reductionFunction the function that aggregates two values
   *
   * @return new result
   */
  private def withAggregationRegistered [
      K <: Writable : ClassTag, V <: Writable : ClassTag
      ] (name: String)(
        aggregationKey: (S, Computation[S], K) => K,
        aggregationValue: (S, Computation[S], V) => V,
        reductionFunction: (V,V) => V
      ): Fractoid[S] = {
    withAggregationRegistered [K,V] (name,
      aggregationKey = aggregationKey,
      aggregationValue = aggregationValue,
      reductionFunction = new ReductionFunctionContainer [V] (reductionFunction)
      )
  }

  /**
   * Adds a new aggregation to the computation
   *
   * @param name identifier of this new aggregation
   * @param reductionFunction the function that aggregates two values
   * @param endAggregationFunction the function that is applied at the end of
   * each local aggregation, in the workers
   *
   * @return new result
   */
  private def withAggregationRegistered [
      K <: Writable : ClassTag, V <: Writable : ClassTag
      ] (name: String, reductionFunction: (V,V) => V,
      endAggregationFunction: (AggregationStorage[K,V]) => Unit)
      : Fractoid[S] = {
    withAggregationRegistered [K,V] (name,
      new ReductionFunctionContainer [V] (reductionFunction),
      new EndAggregationFunctionContainer [K,V] (endAggregationFunction))
  }

  /**
   * Specify a custom expand function to the computation
   *
   * @param expandCompute expand function
   *
   * @return new result
   */
  private def withExpandCompute (expandCompute: (S,Computation[S]) => Iterator[S])
    : Fractoid[S] = {
    val newConfig = if (expandCompute != null) {
      config.withNewComputation (
        getComputationContainer[S].withNewFunctions (
          expandComputeOpt = Option((e,c) => expandCompute(e,c).asJava)
        )
      )
    } else {
      config.withNewComputation (
        getComputationContainer[S].withNewFunctions (expandComputeOpt = None)
      )
    }
    this.copy (config = newConfig)
  }

  /**
   * Return a new result with the computation appended.
   */
  private def withNextComputation (nextComputation: Computation[S],
      newConfig: SparkConfiguration[S] = config)
    : Fractoid[S] = {
    logInfo (s"Appending ${nextComputation} to ${getComputationContainer[S]}")
    val _newConfig = newConfig.withNewComputation (
      getComputationContainer[S].withComputationAppended (nextComputation)
    )
    this.copy (config = _newConfig)
  }

  /****** fractal Scala API: MasterComputationContainer ******/

  /**
   * Updates the init function of the underlying master computation
   * container.
   *
   * @param init initialization function for the master computation
   *
   * @return new result
   */
  private def withMasterInit (init: (MasterComputation) => Unit): Fractoid[S] = {
    val newConfig = config.withNewMasterComputation (
      config.masterComputationContainer.withNewFunctions (
        initOpt = Option(init))
      )
    this.copy (config = newConfig)
  }

  /**
   * Updates the compute function of the underlying master computation
   * container.
   *
   * @param compute callback executed at the end of each superstep in the master
   *
   * @return new result
   */
  private def withMasterCompute (compute: (MasterComputation) => Unit)
    : Fractoid[S] = {
    val newConfig = config.withNewMasterComputation (
      config.masterComputationContainer.withNewFunctions (
        computeOpt = Option(compute)))
    this.copy (config = newConfig)
  }

  /**
   * Check whether the current subgraph parameter is compatible with another
   */
  private def extensibleFrom [EE: ClassTag]: Boolean = {
    classTag[EE].runtimeClass == classTag[S].runtimeClass
  }

  override def toString: String = {
    def computationToString: String = config.computationContainerOpt match {
      case Some(cc) =>
        cc.toString
      case None =>
        s"${config.getString(Configuration.CONF_COMPUTATION_CLASS,"")}"
    }

    s"Fractoid(" +
      //s"scope=${scope}, " +
      s"step=${step}," +
      s" depth=${depth}," +
      s" computation=${computationToString}" +
      //s" mustSync=${mustSync}, +
      //s" config=${config}," +
      //s" outputPath=${config.getOutputPath}," +
      //s" isOutputActive=${config.isOutputActive}" +
      s")"
  }

  def toDebugString: String = parentOpt match {
    case Some(parent) =>
      s"${parent.toDebugString}\n${toString}"
    case None =>
      toString
  }

}
