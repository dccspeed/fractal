package br.ufmg.cs.systems.fractal

import java.io.Serializable
import java.util.concurrent.atomic.AtomicInteger

import br.ufmg.cs.systems.fractal.aggregation._
import br.ufmg.cs.systems.fractal.callback.{EdgeInducedVertexInducedSubgraphConverter, PatternInducedEdgeInducedSubgraphConverter, PatternInducedVertexInducedSubgraphConverter, SubgraphCallback, SubgraphConverter, VertexInducedPatternducedSubgraphConverter}
import br.ufmg.cs.systems.fractal.computation._
import br.ufmg.cs.systems.fractal.conf.SparkConfiguration
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util._
import org.apache.spark.SparkContext
import org.apache.spark.api.python.{PythonRDD, SerDeUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.reflect.{ClassTag, classTag}

/**
 * Fractal workflow state.
 */
case class Fractoid[S <: Subgraph : ClassTag]
(
   private val fractalGraph: FractalGraph,
   step: Int,
   configBc: Broadcast[SparkConfiguration],
   computationContainer: ComputationContainer[S],
   pattern: Pattern,
   parent: Fractoid[_ <: Subgraph]
) extends Logging {

   def this(fractalGraph: FractalGraph, config: SparkConfiguration) = {
      this(fractalGraph, Fractoid.nextStepId,
         fractalGraph.fractalContext.sparkContext.broadcast(config),
         null, null, null)
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
         subgraphAggregation.aggregate_AGGREGATION_PRIMITIVE(s)
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
      logInfo(s"Computing ${this}. Engine: ${_masterEngine}")
      _masterEngine
   }

   /**
    * Aggregates valid subgraphs into a single long
    *
    * @param _defaultValue initial value for this aggregation
    * @param _value        mapping function applied on each valid subgraph
    * @param _reduce       reduce function to aggregate the results
    * @return single final long
    */
   def aggregationLong
   (_defaultValue: Long, _value: S => Long, _reduce: (Long, Long) => Long)
   : Long = {

      val longSubgraphAggregation = new LongSubgraphAggregation[S] {
         override def reduce(v1: Long, v2: Long): Long = _reduce(v1, v2)

         override def aggregate_AGGREGATION_PRIMITIVE(subgraph: S): Unit = map(_value(subgraph))

         override def defaultValue(): Long = _defaultValue
      }

      val callback = subgraphAggregationCallback
      withNextStepId
         .withInitAggregations(c => callback.init(c))
         .withProcess((s, c) => callback.apply(s, c))
         .masterEngineImmutable
         .longRDD(longSubgraphAggregation)
         .reduce(_reduce)
   }

   /**
    * Aggregates valid subgraphs into a single long with custom callback
    *
    * @param _defaultValue initial value for this aggregation
    * @param _value        mapping function applied on each valid subgraph
    * @param _reduce       reduce function to aggregate the results
    * @param callback      custom user callback
    * @return single final long
    */
   def aggregationLongWithCallback
   (_defaultValue: Long, _value: S => Long, _reduce: (Long, Long) => Long,
    callback: (S, Computation[S], SubgraphCallback[S]) => Unit)
   : Long = {
      val _underlyingCallback = subgraphAggregationCallback
      val providedCallback = customSubgraphAggregationCallback(
         callback, _underlyingCallback)

      val longSubgraphAggregation = new LongSubgraphAggregation[S] {
         override def reduce(v1: Long, v2: Long): Long = _reduce(v1, v2)

         override def aggregate_AGGREGATION_PRIMITIVE(subgraph: S): Unit = map(_value(subgraph))

         override def defaultValue(): Long = _defaultValue
      }

      withNextStepId
         .withInitAggregations(c => providedCallback.init(c))
         .withProcess((s, c) => providedCallback.apply(s, c))
         .masterEngineImmutable
         .longRDD(longSubgraphAggregation)
         .reduce(_reduce)
   }

   /**
    * Counts (listing) the number of valid subgraphs
    *
    * @return number of valid subgraphs
    */
   def aggregationCount: Long = {
      aggregationLong(0L, _ => 1L, _ + _)
   }

   /**
    * Counts (listing) the number of valid subgraphs with custom callback
    *
    * @param callback user callback
    * @return number of valid subgraphs
    */
   def aggregationCountWithCallback
   (callback: (S, Computation[S], SubgraphCallback[S]) => Unit)
   : Long = {
      aggregationLongWithCallback(0L, _ => 1L, _ + _, callback)
   }

   /**
    * Aggregates valid subgraphs into a single long
    *
    * @param longSubgraphAggregation custom aggregation
    * @return single final long
    */
   def aggregationLong
   (longSubgraphAggregation: LongSubgraphAggregation[S])
   : RDD[Long] = {
      val callback = subgraphAggregationCallback
      withNextStepId
         .withInitAggregations(c => callback.init(c))
         .withProcess((s, c) => callback.apply(s, c))
         .masterEngineImmutable
         .longRDD(longSubgraphAggregation)
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Values in this function
    * are exclusively primitive longs.
    *
    * @param _key          mapping function that extracts the key from a
    *                      subgraph
    * @param _defaultValue the default value (long)
    * @param _value        value mapping functions that extracts a value from
    *                      a subgraph
    * @param _reduce       reduce function that aggregates values of the same
    *                      key
    * @tparam K key parameter type
    * @return an RDD of key/value pairs (K,Long)
    */
   def aggregationObjLong[K <: Serializable : ClassTag]
   (_key: S => K, _defaultValue: Long, _value: S => Long,
    _reduce: (Long, Long) => Long)
   : RDD[(K, Long)] = {

      val objLongSubgraphAggregation = new ObjLongSubgraphAggregation[S, K] {
         override def reduce(v1: Long, v2: Long): Long = _reduce(v1, v2)

         override def aggregate_AGGREGATION_PRIMITIVE(subgraph: S): Unit = {
            map(_key(subgraph), _value(subgraph))
         }

         override def defaultValue(): Long = _defaultValue
      }

      val objLongRDD = aggregationObjLong[K](objLongSubgraphAggregation)
         .foldByKey(_defaultValue)(_reduce)

      objLongRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Values in this function
    * are exclusively primitive longs.
    *
    * @param objLongSubgraphAggregation custom aggregation
    * @tparam K key type
    * @return
    */
   def aggregationObjLong[K <: Serializable : ClassTag]
   (objLongSubgraphAggregation: ObjLongSubgraphAggregation[S, K])
   : RDD[(K, Long)] = {
      val callback = subgraphAggregationCallback
      val objLongRDD = withNextStepId
         .withInitAggregations(c => callback.init(c))
         .withProcess((s, c) => callback.apply(s, c))
         .masterEngineImmutable
         .objLongRDD[K](objLongSubgraphAggregation)

      objLongRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Values in this function
    * are exclusively primitive longs. Keys in this function are exclusively
    * canonical patterns (built-in).
    *
    * @param _defaultValue the default value (long)
    * @param _value        value mapping functions that extracts a value from
    *                      a subgraph
    * @param _reduce       reduce function that aggregates values of the same
    *                      key
    * @return an RDD of key/value pairs (Pattern,Long)
    */
   def aggregationCanonicalPatternLong
   (_key: S => Pattern, _defaultValue: Long, _value: S => Long,
    _reduce: (Long, Long) => Long)
   : RDD[(Pattern, Long)] = {
      val objLongSubgraphAggregation = new
            ObjLongSubgraphAggregation[S, Pattern] {
         override def reduce(v1: Long, v2: Long): Long = _reduce(v1, v2)

         override def aggregate_AGGREGATION_PRIMITIVE(subgraph: S): Unit = {
            map(_key(subgraph), _value(subgraph))
         }

         override def defaultValue(): Long = _defaultValue
      }

      val objLongRDD = aggregationCanonicalPatternLong(
         objLongSubgraphAggregation)
         .foldByKey(_defaultValue)(_reduce)
         .map(kv => {
            kv._1.turnCanonical(); kv
         })
         .foldByKey(_defaultValue)(_reduce)

      objLongRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Values in this function
    * are exclusively primitive longs. Keys in this function are exclusively
    * canonical patterns (built-in).
    *
    * @param objLongSubgraphAggregation custom aggregation
    * @return an RDD of key/value pairs (Pattern,Long)
    */
   def aggregationCanonicalPatternLong
   (objLongSubgraphAggregation: ObjLongSubgraphAggregation[S, Pattern])
   : RDD[(Pattern, Long)] = {
      val callback = subgraphAggregationCallback
      val objLongRDD = withNextStepId
         .withInitAggregations(c => callback.init(c))
         .withProcess((s, c) => callback.apply(s, c))
         .masterEngineImmutable
         .objLongRDD[Pattern](objLongSubgraphAggregation)
      objLongRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Values in this function
    * are exclusively primitive longs. Keys in this function are exclusively
    * canonical patterns (built-in). This function uses a custom user
    * callback that has access to the underlying callback
    *
    * @param _defaultValue the default value (long)
    * @param _value        value mapping functions that extracts a value from
    *                      a subgraph
    * @param _reduce       reduce function that aggregates values of the same
    *                      key
    * @param _callback     custom user callback
    * @return an RDD of key/value pairs (Pattern,Long)
    */
   def aggregationCanonicalPatternLongWithCallback
   (_key: S => Pattern, _defaultValue: Long, _value: S => Long,
    _reduce: (Long, Long) => Long,
    _callback: (S, Computation[S], SubgraphCallback[S]) => Unit)
   : RDD[(Pattern, Long)] = {

      val objLongSubgraphAggregation = new ObjLongSubgraphAggregation[S,
         Pattern] {

         override def reduce(v1: Long, v2: Long): Long = _reduce(v1, v2)

         override def aggregate_AGGREGATION_PRIMITIVE(subgraph: S): Unit = {
            map(_key(subgraph), _value(subgraph))
         }

         override def defaultValue(): Long = _defaultValue
      }

      val objLongRDD = aggregationCanonicalPatternLongWithCallback(
         objLongSubgraphAggregation, _callback)
         .foldByKey(_defaultValue)(_reduce)
         .map(kv => {
            kv._1.turnCanonical(); kv
         })
         .foldByKey(_defaultValue)(_reduce)

      objLongRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Values in this function
    * are exclusively primitive longs. Keys in this function are exclusively
    * canonical patterns (built-in). This function uses a custom user
    * callback that has access to the underlying callback
    *
    * @param objLongSubgraphAggregation custom aggregation
    * @param _callback                  custom user callback
    * @return an RDD of key/value pairs (Pattern,Long)
    */
   def aggregationCanonicalPatternLongWithCallback
   (objLongSubgraphAggregation: ObjLongSubgraphAggregation[S, Pattern],
    _callback: (S, Computation[S], SubgraphCallback[S]) => Unit)
   : RDD[(Pattern, Long)] = {
      val _underlyingCallback = subgraphAggregationCallback
      val providedCallback = customSubgraphAggregationCallback(
         _callback, _underlyingCallback)
      val objLongRDD = withNextStepId
         .withInitAggregations(c => providedCallback.init(c))
         .withProcess((s, c) => providedCallback.apply(s, c))
         .masterEngineImmutable
         .objLongRDD[Pattern](objLongSubgraphAggregation)

      objLongRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Keys and values in this
    * function are objects.
    *
    * @param _key    mapping function that extracts the key from a subgraph
    * @param _value  value mapping functions that extracts a value from a
    *                subgraph
    * @param _reduce reduce function that aggregates the value of the
    *                second parameter value into the first parameter value
    * @tparam K key parameter type
    * @tparam V value parameter type
    * @return an RDD of key/value pairs (K,V)
    */
   def aggregationObjObj
   [K <: Serializable : ClassTag, V <: Serializable : ClassTag]
   (_key: S => K, _value: S => V, _reduce: (V, V) => Unit)
   : RDD[(K, V)] = {
      val objObjSubgraphAggregation = new ObjObjSubgraphAggregation[S, K, V] {
         override def reduce(existingValue: V, otherValue: V): Unit = {
            _reduce(existingValue, otherValue)
         }

         override def aggregate_AGGREGATION_PRIMITIVE(subgraph: S): Unit = {
            map(_key(subgraph), _value(subgraph))
         }
      }

      val objObjRDD = aggregationObjObj[K, V](objObjSubgraphAggregation)
         .reduceByKey { case (v1, v2) => _reduce(v1, v2); v1 }

      objObjRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Keys and values in this
    * function are objects.
    *
    * @param objObjSubgraphAggregation custom aggregation
    * @tparam K key parameter type
    * @tparam V value parameter type
    * @return an RDD of key/value pairs (K,V)
    */
   def aggregationObjObj
   [K <: Serializable : ClassTag, V <: Serializable : ClassTag]
   (objObjSubgraphAggregation: ObjObjSubgraphAggregation[S, K, V])
   : RDD[(K, V)] = {
      val callback = subgraphAggregationCallback
      val objObjRDD = withNextStepId
         .withInitAggregations(c => callback.init(c))
         .withProcess((s, c) => callback.apply(s, c))
         .masterEngineImmutable
         .objObjRDD[K, V](objObjSubgraphAggregation)

      objObjRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Keys and values in this
    * function are objects. This function uses a custom callback for calling
    * the underlying callback, in case the user want to aggregate a subgraph
    * several times.
    *
    * @param _key     mapping function that extracts the key from a subgraph
    * @param _value   value mapping functions that extracts a value from a
    *                 subgraph
    * @param _reduce  reduce function that aggregates the value of the
    *                 second parameter value into the first parameter value
    * @param callback user callback
    * @tparam K key parameter type
    * @tparam V value parameter type
    * @return an RDD of key/value pairs (K,V)
    */
   def aggregationObjObjWithCallback
   [K <: Serializable : ClassTag, V <: Serializable : ClassTag]
   (_key: S => K, _value: S => V, _reduce: (V, V) => Unit,
    callback: (S, Computation[S], SubgraphCallback[S]) => Unit)
   : RDD[(K, V)] = {

      val objObjSubgraphAggregation = new ObjObjSubgraphAggregation[S, K, V] {
         override def reduce(existingValue: V, otherValue: V): Unit = {
            _reduce(existingValue, otherValue)
         }

         override def aggregate_AGGREGATION_PRIMITIVE(subgraph: S): Unit = {
            map(_key(subgraph), _value(subgraph))
         }
      }

      val objObjRDD = aggregationObjObjWithCallback[K, V](
         objObjSubgraphAggregation, callback)
         .reduceByKey { case (v1, v2) => _reduce(v1, v2); v1 }

      objObjRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Keys and values in this
    * function are objects. This function uses a custom callback for calling
    * the underlying callback, in case the user want to aggregate a subgraph
    * several times.
    *
    * @param objObjSubgraphAggregation custom aggregation
    * @param callback                  user callback
    * @tparam K key parameter type
    * @tparam V value parameter type
    * @return an RDD of key/value pairs (K,V)
    */
   def aggregationObjObjWithCallback
   [K <: Serializable : ClassTag, V <: Serializable : ClassTag]
   (objObjSubgraphAggregation: ObjObjSubgraphAggregation[S, K, V],
    callback: (S, Computation[S], SubgraphCallback[S]) => Unit)
   : RDD[(K, V)] = {
      val _underlyingCallback = subgraphAggregationCallback
      val providedCallback = customSubgraphAggregationCallback(callback,
         _underlyingCallback)

      val objObjRDD = withNextStepId
         .withInitAggregations(c => providedCallback.init(c))
         .withProcess((s, c) => providedCallback.apply(s, c))
         .masterEngineImmutable
         .objObjRDD[K, V](objObjSubgraphAggregation)

      objObjRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Values in this
    * function are objects. Keys in this function are canonical patterns
    * (built-in)
    *
    * @param value     value mapping functions that extracts a value from a
    *                  subgraph
    * @param aggregate reduce function that aggregates the value of the
    *                  second parameter value into the first parameter value
    * @tparam V value parameter type
    * @return an RDD of key/value pairs (Pattern,V)
    */
   def aggregationCanonicalPatternObj
   [V <: Serializable : ClassTag]
   (key: S => Pattern, value: S => V, aggregate: (V, V) => Unit)
   : RDD[(Pattern, V)] = {
      val objObjRDD = aggregationObjObj[Pattern, V](key, value, aggregate)
         .map(kv => {
            kv._1.turnCanonical(); kv
         })
         .reduceByKey { case (v1, v2) => aggregate(v1, v2); v1 }

      objObjRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Keys and values in this
    * function are longs.
    *
    * @param _key    mapping function that extracts the key from a subgraph
    * @param _value  value mapping functions that extracts a value from a
    *                subgraph
    * @param _reduce reduce function that aggregates the value of the
    *                second parameter value into the first parameter value
    * @return an RDD of key/value pairs (K,V)
    */
   def aggregationLongLong
   (_key: S => Long, _defaultValue: Long, _value: S => Long,
    _reduce: (Long, Long) => Long)
   : RDD[(Long, Long)] = {

      val longLongSubgraphAggregation = new LongLongSubgraphAggregation[S] {
         override def aggregate_AGGREGATION_PRIMITIVE(subgraph: S): Unit = {
            map(_key(subgraph), _value(subgraph))
         }

         override def reduce(v1: Long, v2: Long): Long = _reduce(v1, v2)

         override def defaultValue(): Long = _defaultValue
      }

      val longLongRDD = aggregationLongLong(longLongSubgraphAggregation)
         .reduceByKey(_reduce)

     longLongRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Keys and values in this
    * function are longs.
    *
    * @param longLongSubgraphAggregation custom aggregation
    * @return an RDD of key/value pairs (K,V)
    */
   def aggregationLongLong
   (longLongSubgraphAggregation: LongLongSubgraphAggregation[S])
   : RDD[(Long, Long)] = {
      val callback = subgraphAggregationCallback
      val longLongRDD = withNextStepId
         .withInitAggregations(c => callback.init(c))
         .withProcess((s, c) => callback.apply(s, c))
         .masterEngineImmutable
         .longLongRDD(longLongSubgraphAggregation)

      longLongRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Keys in this function
    * are longs and values are objects.
    *
    * @param _key    key function
    * @param _value  value function
    * @param _reduce reduce function (inplace on the first parameter)
    * @return an RDD of key/value pairs (K,V)
    */
   def aggregationLongObj
   [V <: Serializable : ClassTag]
   (_key: S => Long, _value: S => V, _reduce: (V, V) => Unit)
   : RDD[(Long, V)] = {
      val longObjSubgraphAggregation = new LongObjSubgraphAggregation[S, V] {
         override def reduce(v1: V, v2: V): Unit = {
            _reduce(v1, v2)
         }

         override def aggregate_AGGREGATION_PRIMITIVE(subgraph: S): Unit = {
            map(_key(subgraph), _value(subgraph))
         }
      }

      val longObjRDD = aggregationLongObj[V](longObjSubgraphAggregation)
         .reduceByKey { case (v1, v2) => _reduce(v1, v2); v1 }

      longObjRDD
   }

   /**
    * Aggregates valid subgraphs by mapping each valid subgraph to a
    * key/value pair and reducing the values by key. Keys in this function
    * are longs and values are objects.
    *
    * @param longObjSubgraphAggregation custom aggregation
    * @return an RDD of key/value pairs (K,V)
    */
   def aggregationLongObj
   [V <: Serializable : ClassTag]
   (longObjSubgraphAggregation: LongObjSubgraphAggregation[S, V])
   : RDD[(Long, V)] = {
      val callback = subgraphAggregationCallback
      val longObjRDD = withNextStepId
         .withInitAggregations(c => callback.init(c))
         .withProcess((s, c) => callback.apply(s, c))
         .masterEngineImmutable
         .longObjRDD[V](longObjSubgraphAggregation)

      longObjRDD
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
    *
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
      logInfo(s"HandleNextResultAppend ${result} to  ${this}")
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

   /**
    * Called for the first expand call in this fractoid (bootstrap)
    * @return new fractoid
    */
   private def withFirstComputation: Fractoid[S] = {
      this.copy(computationContainer = Fractoid.createFirstComputation(pattern))
   }

   /** **** Fractal Scala API: High Level API ******/

   /**
    * Adds a callback to this workflow
    *
    * @param callback function to be applied to each valid subgraph
    * @return the new fractoid
    */
   def process(callback: (S, Computation[S]) => Unit): Fractoid[S] = {
      withProcess(callback)
   }

   /**
    * Perform *n* expansion iterations
    *
    * @param n number of expansions
    * @return new result
    */
   def expand(n: Int): Fractoid[S] = {
      expand(n, null)
   }

   /**
    * Perform *n* expansion iterations using custom subgraph enumerator
    *
    * @param n number of expansions
    * @param senumClass subgraph enumerator class
    * @return new result
    */
   def expand(n: Int, senumClass: Class[_ <: SubgraphEnumerator[S]])
   : Fractoid[S] = {
      logInfo(s"Expand fractoid=${this} n=${n}")
      // base step, no effect
      if (n == 0) return this

      var stepResult: Fractoid[S] = null

      // first computation, create a new computation
      if (computationContainer == null) {
         stepResult = withFirstComputation
            .withSubgraphEnumeratorClass(senumClass)
         logInfo(
            s"ExpandNewComputation(n=${n}): before=${this}" +
               s" after=${stepResult} senumClass=${senumClass}")
      } else {
         val expandComp = emptyComputation(Primitive.E)
            .withSubgraphEnumeratorClass(senumClass)
         stepResult = handleNextResult(expandComp)
         logInfo(
            s"ExpandAppendComputation(n=${n}): before=${this} " +
               s"after=${stepResult} senumClass=${senumClass}")
      }

      // recursive call
      stepResult.expand(n - 1, senumClass)
   }

   /**
    * Filter the existing subgraphs based on a function
    *
    * @param filter function that decides whether an subgraph should be kept or
    *               discarded
    * @return new result
    */
   def filter(filter: (S, Computation[S]) => Boolean): Fractoid[S] = {
      //ClosureCleaner.clean(filter)
      val senumClass = classOf[BypassSubgraphEnumerator[S]]
      val filterComp = emptyComputation(Primitive.F)
         .withSubgraphEnumeratorClass(senumClass)
         .withFilter(filter)
      val result = handleNextResult(filterComp)
      logInfo(s"Filter before: ${this} after: ${result}")
      result
   }

   /**
    * Switch to edge-induced fractoid using built-in converter
    * @return Edge-induced fractoid
    */
   def efractoid: Fractoid[EdgeInducedSubgraph] = {
      val converter = Fractoid.builtInConverterSelector[S,EdgeInducedSubgraph]
      efractoid(converter)
   }

   /**
    * Switch to edge-induced fractoid using custom converter
    * @param convertionFunc function mapping to edge-induced subgraphs
    * @return Edg-induced fractoid
    */
   def efractoid(convertionFunc: (S,Computation[S],EdgeInducedSubgraph,Computation[EdgeInducedSubgraph]) => Unit)
   : Fractoid[EdgeInducedSubgraph] = {

      val converter = new SubgraphConverter[S,EdgeInducedSubgraph] {
         private var nextEngine: SparkFromScratchEngine[EdgeInducedSubgraph] = _
         private var nextSubgraph: EdgeInducedSubgraph = _
         private var nextComputation: Computation[EdgeInducedSubgraph] = _

         override def convert(subgraphIn: S,
                              computationIn: Computation[S],
                              subgraphOut: EdgeInducedSubgraph,
                              computationOut: Computation[EdgeInducedSubgraph])
         : Unit = {
            convertionFunc.apply(subgraphIn, computationIn,
               subgraphOut, computationOut)
         }

         override def apply(subgraph: S,
                            computation: Computation[S]): Unit = {
            convert(subgraph, computation, nextSubgraph, nextComputation)
            // next engine compute
            nextEngine.initialWorkCompute()
         }

         override def init(computation: Computation[S]): Unit = {
            nextEngine = computation.getExecutionEngine.getNextEngine
               .asInstanceOf[SparkFromScratchEngine[EdgeInducedSubgraph]]
            nextComputation = nextEngine.computation
            nextSubgraph = nextEngine.computation.getSubgraphEnumerator
               .getSubgraph
         }
      }

      efractoid(converter)
   }

   /**
    * Switch to edge-induced fractoid using provided converter
    * @param converter subgraph converter mapping to edge-induced subgraph
    * @return edge-induced fractoid
    */
   private def efractoid(converter: SubgraphConverter[S,EdgeInducedSubgraph])
   : Fractoid[EdgeInducedSubgraph] = {
      val thisWithConverter = withNextStepId
         .withInitAggregations(c => converter.init(c))
         .withProcess((s, c) => converter.apply(s, c))

      val efrac = fractalGraph.efractoid.withNextStepId
         .copy(parent = thisWithConverter)

      efrac
   }

   /**
    * Switch to vertex-induced fractoid using built-in converter
    * @return Vertex-induced fractoid
    */
   def vfractoid: Fractoid[VertexInducedSubgraph] = {
      val converter = Fractoid.builtInConverterSelector[S,VertexInducedSubgraph]
      vfractoid(converter)
   }

   /**
    * Switch to vertex-induced fractoid using custom converter
    * @param convertionFunc function mapping to vertex-induced subgraphs
    * @return Vertex-induced fractoid
    */
   def vfractoid
   (convertionFunc
    : (S,Computation[S],VertexInducedSubgraph, Computation[VertexInducedSubgraph]) => Unit)
   : Fractoid[VertexInducedSubgraph] = {

      val converter = new SubgraphConverter[S,VertexInducedSubgraph] {
         private var nextEngine: SparkFromScratchEngine[VertexInducedSubgraph] = _
         private var nextSubgraph: VertexInducedSubgraph = _
         private var nextComputation: Computation[VertexInducedSubgraph] = _

         override def convert(subgraphIn: S,
                              computationIn: Computation[S],
                              subgraphOut: VertexInducedSubgraph,
                              computationOut: Computation[VertexInducedSubgraph])
         : Unit = {
            convertionFunc.apply(subgraphIn, computationIn,
               subgraphOut, computationOut)
         }

         override def apply(subgraph: S,
                            computation: Computation[S]): Unit = {
            convert(subgraph, computation, nextSubgraph, nextComputation)
            // next engine compute
            nextEngine.initialWorkCompute()
         }

         override def init(computation: Computation[S]): Unit = {
            nextEngine = computation.getExecutionEngine.getNextEngine
               .asInstanceOf[SparkFromScratchEngine[VertexInducedSubgraph]]
            nextComputation = nextEngine.computation
            nextSubgraph = nextEngine.computation.getSubgraphEnumerator
               .getSubgraph
         }
      }

      vfractoid(converter)
   }

   /**
    * Switch to vertex-induced fractoid using provided converter
    * @param converter subgraph converter mapping to vertex-induced subgraph
    * @return vertex-induced fractoid
    */
   private def vfractoid(converter: SubgraphConverter[S,VertexInducedSubgraph])
   : Fractoid[VertexInducedSubgraph] = {
      val thisWithConverter = withNextStepId
         .withInitAggregations(c => converter.init(c))
         .withProcess((s, c) => converter.apply(s, c))

      val vfrac = fractalGraph.vfractoid.withNextStepId
         .copy(parent = thisWithConverter)

      vfrac
   }

   /**
    * Switch to pattern-induced fractoid using built-in converter
    * @return pattern-induced fractoid
    */
   def pfractoid(pattern: Pattern): Fractoid[PatternInducedSubgraph] = {
      val converter = Fractoid.builtInConverterSelector[S,PatternInducedSubgraph]
      pfractoid(pattern, converter)
   }

   /**
    * Switch to pattern-induced fractoid using custom converter
    * @param convertionFunc function mapping to pattern-induced subgraphs
    * @return pattern-induced fractoid
    */
   def pfractoid
   (pattern: Pattern, convertionFunc
    : (S,Computation[S],PatternInducedSubgraph, Computation[PatternInducedSubgraph]) =>  Unit)
   : Fractoid[PatternInducedSubgraph] = {

      val converter = new SubgraphConverter[S,PatternInducedSubgraph] {
         private var nextEngine: SparkFromScratchEngine[PatternInducedSubgraph] = _
         private var nextSubgraph: PatternInducedSubgraph = _
         private var nextComputation: Computation[PatternInducedSubgraph] = _

         override def convert(subgraphIn: S,
                              computationIn: Computation[S],
                              subgraphOut: PatternInducedSubgraph,
                              computationOut: Computation[PatternInducedSubgraph])
         : Unit = {
            convertionFunc.apply(subgraphIn, computationIn,
               subgraphOut, computationOut)
         }

         override def apply(subgraph: S,
                            computation: Computation[S]): Unit = {
            convert(subgraph, computation, nextSubgraph, nextComputation)
            // next engine compute
            nextEngine.initialWorkCompute()
         }

         override def init(computation: Computation[S]): Unit = {
            nextEngine = computation.getExecutionEngine.getNextEngine
               .asInstanceOf[SparkFromScratchEngine[PatternInducedSubgraph]]
            nextComputation = nextEngine.computation
            nextSubgraph = nextEngine.computation.getSubgraphEnumerator
               .getSubgraph
         }
      }

      pfractoid(pattern, converter)
   }

   /**
    * Switch to pattern-induced fractoid using provided converter
    * @param converter subgraph converter mapping to pattern-induced subgraph
    * @return pattern-induced fractoid
    */
   private def pfractoid(pattern: Pattern,
                         converter: SubgraphConverter[S,PatternInducedSubgraph])
   : Fractoid[PatternInducedSubgraph] = {
      val thisWithConverter = withNextStepId
         .withInitAggregations(c => converter.init(c))
         .withProcess((s, c) => converter.apply(s, c))

      val pfrac = fractalGraph
         .pfractoid(pattern)
         .withNextStepId
         .copy(parent = thisWithConverter)

      pfrac
   }

   /** **** Fractal Scala API: ComputationContainer ******/

   /**
    * Updates the process function of the underlying computation container.
    *
    * @param process process function to be applied to each subgraph produced
    * @return new result
    */
   private def withProcess(process: (S, Computation[S]) => Unit)
   : Fractoid[S] = {
      val newComp = computationContainer.withNewFunctions(
         processOpt = Option(process))
      val result = this.copy(computationContainer = newComp)
      logInfo(s"WithProcess before: ${this} after: ${result}")
      logInfo(
         s"WithProcessComp before: ${computationContainer} after: ${newComp}")
      result
   }

   /**
    * Updates the filter function of the underlying computation container.
    *
    * @param filter filter function that determines whether Subgraphs must be
    *               further processed or not.
    * @return new result
    */
   private def withFilter(filter: (S, Computation[S]) => Boolean)
   : Fractoid[S] = {
      val newComp = computationContainer
         .withNewFunctions(filterOpt = Option(filter))
      this.copy(computationContainer = newComp)
   }

   /**
    * Updates the initAggregations function of the underlying computation
    * container.
    *
    * @param initAggregations function that initializes the aggregations for the
    *                         computation
    * @return new result
    */
   private def withInitAggregations(initAggregations: (Computation[S]) => Unit)
   : Fractoid[S] = {
      val initComp = computationContainer.withNewFunctions(
         initAggregationsOpt = Option(initAggregations))
      val initRes = this.copy(computationContainer = initComp)
      logInfo(s"WithInitAggregations before=${this} after=${initRes}")
      initRes
   }

   private def withSubgraphEnumeratorClass
   (senumClass: Class[_ <: SubgraphEnumerator[S]]): Fractoid[S] = {
      if (senumClass == null) return this.copy()
      val newComp = computationContainer.withNewFunctions(
         subgraphEnumeratorClassOpt = Option(senumClass)
      )
      this.copy(computationContainer = newComp)
   }

   /**
    * Return a new result with the computation appended.
    */
   private def withNextComputation(nextComputation: Computation[S])
   : Fractoid[S] = {
      logInfo(s"Appending ${nextComputation} to ${computationContainer}")
      val newComp = computationContainer
         .withComputationAppended(nextComputation)
      val result = this.copy(computationContainer = newComp)
      logInfo(s"Result after appending: ${result}")
      result
   }

   override def toString: String = {
      val className = classTag[S].runtimeClass.getSimpleName
      s"Fractoid${className}(" +
         s"step=${step}" +
         s",primitives=${primitives.mkString("-")}" +
         s",parent=${parent}" +
         s")"
   }
}

object Fractoid {
   private val nextStepIdAtomic = new AtomicInteger(0)

   private def nextStepId = nextStepIdAtomic.getAndIncrement()

   private def createFirstComputation[S <: Subgraph : ClassTag]
   (pattern: Pattern = null): ComputationContainer[S] = {
      val computation = {
         val sclass = classTag[S].runtimeClass.asInstanceOf[Class[S]]
         if (classOf[VertexInducedSubgraph].isAssignableFrom(sclass)) {
            new VComputationContainer(processOpt = Option(null),
               primitive = Primitive.E,
               subgraphClassOpt = Option(sclass))
         } else if (sclass == classOf[EdgeInducedSubgraph]) {
            new EComputationContainer(processOpt = Option(null),
               primitive = Primitive.E)
         } else if (sclass == classOf[PatternInducedSubgraph]) {
            new PComputationContainer(processOpt = Option(null),
               patternOpt = Option(pattern),
               primitive = Primitive.E)
         } else {
            throw new RuntimeException(s"Unsupported subgraph type ${sclass}")
         }
      }

      computation.asInstanceOf[ComputationContainer[S]]
   }

   private def builtInConverterSelector
   [IN <: Subgraph : ClassTag, OUT <: Subgraph : ClassTag]
   : SubgraphConverter[IN, OUT] = {
      val inRuntimeClass = classTag[IN].runtimeClass
      val outRuntimeClass = classTag[OUT].runtimeClass

      if (inRuntimeClass.isAssignableFrom(classOf[PatternInducedSubgraph])
          && outRuntimeClass.isAssignableFrom(classOf[EdgeInducedSubgraph])) {
         new PatternInducedEdgeInducedSubgraphConverter()
            .asInstanceOf[SubgraphConverter[IN,OUT]]
      }

      else if (inRuntimeClass.isAssignableFrom(classOf[PatternInducedSubgraph])
         && outRuntimeClass.isAssignableFrom(classOf[VertexInducedSubgraph])) {
         new PatternInducedVertexInducedSubgraphConverter()
            .asInstanceOf[SubgraphConverter[IN, OUT]]
      }

      else if (inRuntimeClass.isAssignableFrom(classOf[EdgeInducedSubgraph])
         && outRuntimeClass.isAssignableFrom(classOf[VertexInducedSubgraph])) {
         new EdgeInducedVertexInducedSubgraphConverter()
            .asInstanceOf[SubgraphConverter[IN, OUT]]
      }

      else if (classOf[VertexInducedSubgraph].isAssignableFrom(inRuntimeClass)
         && outRuntimeClass.isAssignableFrom(classOf[PatternInducedSubgraph])) {
         new VertexInducedPatternducedSubgraphConverter()
            .asInstanceOf[SubgraphConverter[IN, OUT]]
      }

      else {
         throw new RuntimeException(s"Built-in converter between " +
            s"${inRuntimeClass} and ${outRuntimeClass} not known.")
      }
   }
}
