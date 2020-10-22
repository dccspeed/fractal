package br.ufmg.cs.systems.fractal

import java.io.Serializable
import java.util.concurrent.atomic.AtomicInteger

import br.ufmg.cs.systems.fractal.aggregation._
import br.ufmg.cs.systems.fractal.computation._
import br.ufmg.cs.systems.fractal.conf.SparkConfiguration
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util._
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.reflect.{ClassTag, classTag}

/**
 * Fractal workflow state.
 */
case class Fractoid [S <: Subgraph : ClassTag]
(
   private val fractalGraph: FractalGraph,
   step: Int,
   configBc: Broadcast[SparkConfiguration],
   computationContainer: ComputationContainer[S],
   pattern: Pattern
) extends Logging {

   def this(fractalGraph: FractalGraph, config: SparkConfiguration) = {
      this(fractalGraph, Fractoid.nextStepId,
         fractalGraph.fractalContext.sparkContext.broadcast(config),
         null, null)
   }

   def fractalContext: FractalContext = fractalGraph.fractalContext

   def sparkContext: SparkContext = fractalContext.sparkContext

   def config: SparkConfiguration = configBc.value

   if (!config.isInitialized) {
      config.initialize(isMaster = true)
   }

   /**
    * Size of the chain of computations in this result
    */
   lazy val numComputations: Int = {
      var currOpt = Option(computationContainer)
      var nc = 0
      while (currOpt.isDefined) {
         nc += 1
         currOpt = currOpt.get.nextComputationOpt.
            asInstanceOf[Option[ComputationContainer[S]]]
      }
      nc
   }

   private def subgraphAggregationCallback = new SubgraphCallback[S] {
      private var subgraphAggregation: SubgraphAggregation[S] = _
      override def apply(s: S, c: Computation[S]): Unit = {
         if (EventTimer.ENABLED) {
            EventTimer.workerInstance(c.getPartitionId).finishAndStart(
               EventTimer.ENUMERATION_FILTERING, EventTimer.AGGREGATION)
         }

         subgraphAggregation.aggregate(s)

         if (EventTimer.ENABLED) {
            EventTimer.workerInstance(c.getPartitionId).finishAndStart(
               EventTimer.AGGREGATION, EventTimer.ENUMERATION_FILTERING)
         }
      }

      override def init(c: Computation[S]): Unit = {
         subgraphAggregation = c.getSubgraphAggregation
      }
   }

   private def customSubgraphAggregationCallback
   (callback: (S, Computation[S], SubgraphCallback[S]) => Unit,
    _underlyingCallback: SubgraphCallback[S])
   : SubgraphCallback[S] = new SubgraphCallback[S] {
      private val underlyingCallback = _underlyingCallback
      override def apply(subgraph: S, computation: Computation[S]): Unit = {
         callback(subgraph, computation, underlyingCallback)
      }

      override def init(computation: Computation[S]): Unit = {
         underlyingCallback.init(computation)
      }
   }

   /**
    * Get an array with *all* primitives within this workflow. *All* meaning
    * primitives of this fractoid and parent (recursively)
    */
   def primitives: Array[Primitive] = {
      val thisPrimitives = if (computationContainer != null) {
         computationContainer.primitives()
      } else {
         Array[Primitive]()
      }

      thisPrimitives
   }

   private[fractal] def masterEngineImmutable: SparkMasterEngine[S] = {
      val _masterEngine = SparkMasterEngine[S](this)
      logInfo (s"Computing ${this}. Engine: ${_masterEngine}")
      _masterEngine
   }

   /**
    * Aggregates valid subgraphs into a single long
    * @param defaultValue initial value for this aggregation
    * @param value mapping function applied on each valid subgraph
    * @param reduce reduce function to aggregate the results
    * @return single final long
    */
   def aggregationLong
   (defaultValue: Long, value: S => Long, reduce: (Long,Long) => Long)
   : Long = {
      val callback = subgraphAggregationCallback
      withNextStepId
         .withInitAggregations(c => callback.init(c))
         .withProcess((s,c) => callback.apply(s,c))
         .masterEngineImmutable
         .longRDD(defaultValue, value, reduce)
         .reduce(reduce)
   }

   /**
    * Aggregates valid subgraphs into a single long with custom callback
    * @param defaultValue initial value for this aggregation
    * @param value mapping function applied on each valid subgraph
    * @param reduce reduce function to aggregate the results
    * @param callback custom user callback
    * @return single final long
    */
   def aggregationLongWithCallback
   (defaultValue: Long, value: S => Long, reduce: (Long,Long) => Long,
    callback: (S, Computation[S], SubgraphCallback[S]) => Unit)
   : Long = {
      val _underlyingCallback = subgraphAggregationCallback
      val providedCallback = customSubgraphAggregationCallback(
         callback, _underlyingCallback)
      withNextStepId
         .withInitAggregations(c => providedCallback.init(c))
         .withProcess((s,c) => providedCallback.apply(s,c))
         .masterEngineImmutable
         .longRDD(defaultValue, value, reduce)
         .reduce(reduce)
   }

   /**
    * Counts (listing) the number of valid subgraphs
    * @return number of valid subgraphs
    */
   def aggregationCount: Long = {
      aggregationLong(0L, _ => 1L, _ + _)
   }

   /**
    * Counts (listing) the number of valid subgraphs with custom callback
    * @param callback user callback
    * @return number of valid subgraphs
    */
   def aggregationCountWithCallback
   (callback: (S, Computation[S], SubgraphCallback[S]) => Unit)
   : Long = {
      aggregationLongWithCallback(0L, _ => 1L, _ + _, callback)
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Values in this function
    * are exclusively primitive longs.
    * @param key mapping function that extracts the key from a subgraph
    * @param defaultValue the default value (long)
    * @param value value mapping functions that extracts a value from a subgraph
    * @param reduce reduce function that aggregates values of the same key
    * @tparam K key parameter type
    * @return an RDD of key/value pairs (K,Long)
    */
   def aggregationObjLong[K <: Serializable : ClassTag]
   (key: S => K, defaultValue: Long, value: S => Long,
    reduce: (Long,Long) => Long)
   : RDD[(K,Long)] = {
      val callback = subgraphAggregationCallback
      val objLongRDD = withNextStepId
         .withInitAggregations(c => callback.init(c))
         .withProcess((s,c) => callback.apply(s,c))
         .masterEngineImmutable
         .objLongRDD[K](key, defaultValue, value, reduce)
         .foldByKey(defaultValue)(reduce)

      objLongRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Values in this function
    * are exclusively primitive longs. Keys in this function are exclusively
    * canonical patterns (built-in).
    * @param defaultValue the default value (long)
    * @param value value mapping functions that extracts a value from a subgraph
    * @param reduce reduce function that aggregates values of the same key
    * @return an RDD of key/value pairs (Pattern,Long)
    */
   def aggregationCanonicalPatternLong
   (key: S => Pattern, defaultValue: Long, value: S => Long,
    reduce: (Long,Long) => Long)
   : RDD[(Pattern,Long)] = {
      val callback = subgraphAggregationCallback
      val objLongRDD = withNextStepId
         .withInitAggregations(c => callback.init(c))
         .withProcess((s,c) => callback.apply(s,c))
         .masterEngineImmutable
         .objLongRDD[Pattern](key, defaultValue, value, reduce)

         .foldByKey(defaultValue)(reduce)
         .map(kv => {kv._1.turnCanonical(); kv})

         .foldByKey(defaultValue)(reduce)

      objLongRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Values in this function
    * are exclusively primitive longs. Keys in this function are exclusively
    * canonical patterns (built-in). This function uses a custom user
    * callback that has access to the underlying callback
    * @param defaultValue the default value (long)
    * @param value value mapping functions that extracts a value from a subgraph
    * @param reduce reduce function that aggregates values of the same key
    * @param callback custom user callback
    * @return an RDD of key/value pairs (Pattern,Long)
    */
   def aggregationCanonicalPatternLongWithCallback
   (key: S => Pattern, defaultValue: Long, value: S => Long,
    reduce: (Long,Long) => Long,
    callback: (S, Computation[S], SubgraphCallback[S]) => Unit)
   : RDD[(Pattern,Long)] = {
      val _underlyingCallback = subgraphAggregationCallback
      val providedCallback = customSubgraphAggregationCallback(
         callback, _underlyingCallback)
      val objLongRDD = withNextStepId
         .withInitAggregations(c => providedCallback.init(c))
         .withProcess((s,c) => providedCallback.apply(s,c))
         .masterEngineImmutable
         .objLongRDD[Pattern](key, defaultValue, value, reduce)
         .foldByKey(defaultValue)(reduce)
         .map(kv => {kv._1.turnCanonical(); kv})
         .foldByKey(defaultValue)(reduce)

      objLongRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Keys and values in this
    * function are objects.
    * @param key mapping function that extracts the key from a subgraph
    * @param value value mapping functions that extracts a value from a subgraph
    * @param aggregate reduce function that aggregates the value of the
    *                  second parameter value into the first parameter value
    * @tparam K key parameter type
    * @tparam V value parameter type
    * @return an RDD of key/value pairs (K,V)
    */
   def aggregationObjObj
   [K <: Serializable : ClassTag, V <: Serializable : ClassTag]
   (key: S => K, value: S => V, aggregate: (V,V) => Unit)
   : RDD[(K,V)] = {
      val callback = subgraphAggregationCallback
      val objObjRDD = withNextStepId
         .withInitAggregations(c => callback.init(c))
         .withProcess((s,c) => callback.apply(s,c))
         .masterEngineImmutable
         .objObjRDD[K,V](key, value, aggregate)
         .reduceByKey{case (v1,v2) => aggregate(v1, v2); v1}

      objObjRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Keys and values in this
    * function are objects. This function uses a custom callback for calling
    * the underlying callback, in case the user want to aggregate a subgraph
    * several times.
    * @param key mapping function that extracts the key from a subgraph
    * @param value value mapping functions that extracts a value from a subgraph
    * @param aggregate reduce function that aggregates the value of the
    *                  second parameter value into the first parameter value
    * @param callback user callback
    * @tparam K key parameter type
    * @tparam V value parameter type
    * @return an RDD of key/value pairs (K,V)
    */
   def aggregationObjObjWithCallback
   [K <: Serializable : ClassTag, V <: Serializable : ClassTag]
   (key: S => K, value: S => V, aggregate: (V,V) => Unit,
    callback: (S, Computation[S], SubgraphCallback[S]) => Unit)
   : RDD[(K,V)] = {
      val _underlyingCallback = subgraphAggregationCallback
      val providedCallback = customSubgraphAggregationCallback(callback,
         _underlyingCallback)
      val objObjRDD = withNextStepId
         .withInitAggregations(c => providedCallback.init(c))
         .withProcess((s,c) => providedCallback.apply(s,c))
         .masterEngineImmutable
         .objObjRDD[K,V](key, value, aggregate)
         .reduceByKey{case (v1,v2) => aggregate(v1, v2); v1}

      objObjRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Values in this
    * function are objects. Keys in this function are canonical patterns
    * (built-in)
    * @param value value mapping functions that extracts a value from a subgraph
    * @param aggregate reduce function that aggregates the value of the
    *                  second parameter value into the first parameter value
    * @tparam V value parameter type
    * @return an RDD of key/value pairs (Pattern,V)
    */
   def aggregationCanonicalPatternObj
   [V <: Serializable : ClassTag]
   (key: S => Pattern, value: S => V, aggregate: (V,V) => Unit)
   : RDD[(Pattern,V)] = {
      val callback = subgraphAggregationCallback
      val objObjRDD = withNextStepId
         .withInitAggregations(c => callback.init(c))
         .withProcess((s,c) => callback.apply(s,c))
         .masterEngineImmutable
         .objObjRDD[Pattern,V](key, value, aggregate)
         .reduceByKey{case (v1,v2) => aggregate(v1, v2); v1}
         .map(kv => {kv._1.turnCanonical(); kv})
         .reduceByKey{case (v1,v2) => aggregate(v1, v2); v1}

      objObjRDD
   }

   def explore(n: Int): Fractoid[S] = {
      var currResult = this
      var i = 0
      while (i < n) {
         currResult = currResult.handleNextResult(this)
         i += 1
      }

      currResult
   }

   /**
    * Copies this fractoid while setting a new unique step id (incremental
    * per JVM instance)
    * @return fractoid with unique step id
    */
   private def withNextStepId: Fractoid[S] = {
      this.copy(step = Fractoid.nextStepId)
   }

   /**
    * Handle the creation of a next result.
    */
   private def handleNextResult(result: Fractoid[S])
   : Fractoid[S] = {
      logInfo (s"HandleNextResultAppend ${result} to  ${this}")
      val nextContainer = result.computationContainer
      withNextComputation(nextContainer)
   }

   /**
    * Create an empty computation that inherits all this result's configurations
    * but the computation container itself
    */
   private def emptyComputation(p: Primitive): Fractoid[S] = {
      if (computationContainer != null) {
         this.copy(computationContainer = computationContainer
            .asLastComputation.clear().withPrimitive(p))
      } else {
         this
      }
   }

   private def withFirstComputation: Fractoid[S] = {
      //this.copy(config = config.withNewComputation(
      //   Fractoid.createFirstComputation(pattern)))
      this.copy(computationContainer = Fractoid.createFirstComputation(pattern))
   }

   /****** Fractal Scala API: High Level API ******/

   /**
    * Adds a callback to this workflow
    * @param callback function to be applied to each valid subgraph
    * @return the new fractoid
    */
   def process(callback: (S,Computation[S]) => Unit): Fractoid[S] = {
      withProcess(callback)
   }

   /**
    * Perform *n* expansion iterations
    *
    * @param n number of expansions
    * @return new result
    */
   def expand(n: Int): Fractoid[S] = {
      logInfo(s"Expand fractoid=${this} n=${n}")
      // base step, no effect
      if (n == 0) return this

      var stepResult: Fractoid[S] = null

      // first computation, create a new computation
      if (computationContainer == null) {
         stepResult = withFirstComputation
         logInfo(s"ExpandNewComputation(n=${n}): before=${this} after=${stepResult}")
      } else {
         val expandComp = emptyComputation(Primitive.E).
            withShouldBypass(false)
         stepResult = handleNextResult(expandComp)
         logInfo(s"ExpandAppendComputation(n=${n}): before=${this} after=${stepResult}")
      }

      // recursive call
      stepResult.expand(n - 1)
   }

   /**
    * Filter the existing subgraphs based on a function
    *
    * @param filter function that decides whether an subgraph should be kept or
    * discarded
    * @return new result
    */
   def filter(filter: (S,Computation[S]) => Boolean): Fractoid[S] = {
      //ClosureCleaner.clean(filter)
      val filterComp = emptyComputation(Primitive.F).
         withShouldBypass(true).
         withFilter(filter)
      val result = handleNextResult(filterComp)
      logInfo (s"Filter before: ${this} after: ${result}")
      result
   }

   /****** Fractal Scala API: ComputationContainer ******/

   /**
    * Updates the process function of the underlying computation container.
    *
    * @param process process function to be applied to each subgraph produced
    *
    * @return new result
    */
   private def withProcess (process: (S,Computation[S]) => Unit)
   : Fractoid[S] = {
      val newComp = computationContainer.withNewFunctions (
         processOpt = Option(process))
      val result = this.copy(computationContainer = newComp)
      logInfo (s"WithProcess before: ${this} after: ${result}")
      logInfo (s"WithProcessComp before: ${computationContainer} after: ${newComp}")
      result
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
      val newComp = computationContainer
         .withNewFunctions(filterOpt = Option(filter))
      this.copy(computationContainer = newComp)
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
      val initComp = computationContainer.withNewFunctions (
            initAggregationsOpt = Option(initAggregations))
      val initRes = this.copy(computationContainer = initComp)
      logInfo(s"WithInitAggregations before=${this} after=${initRes}")
      initRes
   }

   private def withShouldBypass (bypass: Boolean): Fractoid[S] = {
      val newComp = computationContainer.withNewFunctions(
            shouldBypassOpt = Option(bypass))
      this.copy(computationContainer = newComp)
   }

   /**
    * Return a new result with the computation appended.
    */
   private def withNextComputation (nextComputation: Computation[S])
   : Fractoid[S] = {
      logInfo (s"Appending ${nextComputation} to ${computationContainer}")
      val newComp = computationContainer
         .withComputationAppended(nextComputation)
      val result = this.copy(computationContainer = newComp)
      logInfo (s"Result after appending: ${result}")
      result
   }

   override def toString: String = {
      s"Fractoid(" +
         s"step=${step}," +
         s" computation=${computationContainer}" +
         s")"
   }
}

object Fractoid {
   private val nextStepIdAtomic = new AtomicInteger(0)

   private def nextStepId = nextStepIdAtomic.getAndIncrement()

   private def createFirstComputation[S <: Subgraph : ClassTag]
   (pattern: Pattern = null): ComputationContainer[S] = {
      val computation = {
         val sclass = classTag[S].runtimeClass
         if (sclass == classOf[VertexInducedSubgraph]) {
            new VComputationContainer(processOpt = Option(null),
               primitive = Primitive.E)
         } else if (sclass == classOf[EdgeInducedSubgraph]) {
            new EComputationContainer(processOpt = Option(null),
               primitive = Primitive.E)
         } else if (sclass == classOf[PatternInducedSubgraph]) {
            new VEComputationContainer(processOpt = Option(null),
               patternOpt = Option(pattern),
               primitive = Primitive.E)
         } else {
            throw new RuntimeException(s"Unsupported subgraph type ${sclass}")
         }
      }

      computation.asInstanceOf[ComputationContainer[S]]
   }
}
