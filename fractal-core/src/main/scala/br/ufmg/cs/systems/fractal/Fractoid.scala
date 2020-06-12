package br.ufmg.cs.systems.fractal

import java.io.Serializable
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import br.ufmg.cs.systems.fractal.aggregation._
import br.ufmg.cs.systems.fractal.aggregation.reductions._
import br.ufmg.cs.systems.fractal.computation._
import br.ufmg.cs.systems.fractal.conf.{Configuration, SparkConfiguration}
import br.ufmg.cs.systems.fractal.gmlib.fsm.MinImageSupport
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.graph.{Edge, Vertex}
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util._
import com.koloboke.collect.IntCollection
import com.koloboke.collect.map.ObjObjMap
import com.koloboke.collect.map.hash.HashObjObjMaps
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
case class Fractoid [S <: Subgraph : ClassTag]
(
   fractalGraph: FractalGraph,
   private val mustSync: Boolean,
   private val scope: Int,
   step: Int,
   parentOpt: Option[Fractoid[S]],
   config: SparkConfiguration[S],
   aggCallbacks
   : Map[String,SubgraphCallback[S]]
) extends Logging {

   def this(arabGraph: FractalGraph, config: SparkConfiguration[S]) = {
      this(arabGraph, false, 0, 0, None, config,
         Map.empty[String,SubgraphCallback[S]])
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

   private def subgraphAggregationCallback = new SubgraphCallback[S] {
      private var subgraphAggregation: SubgraphAggregation[S] = _
      override def apply(s: S, c: Computation[S]): Unit = {
         subgraphAggregation.aggregate(s)
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
      val thisPrimitives = if (getComputationContainer[S] != null) {
         getComputationContainer[S].primitives()
      } else {
         Array[Primitive]()
      }

      parentOpt match {
         case Some(p) =>
            p.primitives ++ thisPrimitives
         case None =>
            thisPrimitives
      }
   }

   /**
    * Lazy evaluation for the results
    */
   private var masterEngineOpt: Option[SparkMasterEngine[S]] = None

   private def masterEngineImmutable: SparkMasterEngine[S] = {
      val _masterEngine = parentOpt match {
         case Some(parent) =>
            SparkMasterEngine [S] (sparkContext, step, config,
               parent.masterEngineImmutable)

         case None =>
            SparkMasterEngine [S] (sparkContext, step, config, null)
      }

      logInfo (s"Computing ${this}. Engine: ${_masterEngine}")
      _masterEngine.config.set("primitives_workflow", primitives)
      _masterEngine
   }

   private def masterEngine: SparkMasterEngine[S] = synchronized {
      masterEngineOpt match {
         case None =>

            var _masterEngine = parentOpt match {
               case Some(parent) =>
                  if (parent.masterEngine.next) {
                     SparkMasterEngine [S] (sparkContext, step, config, parent
                        .masterEngine)
                  } else {
                     masterEngineOpt = Some(parent.masterEngine)
                     return parent.masterEngine
                  }

               case None =>
                  SparkMasterEngine [S] (sparkContext, step, config, null)
            }

            assert (_masterEngine.step == this.step,
               s"masterEngineNext=${_masterEngine.next}" +
                  s" masterEngineStep=${_masterEngine.step} thisStep=${this.step}")

            logInfo (s"Computing ${this}. Engine: ${_masterEngine}")
            _masterEngine.config.set("primitives_workflow", primitives)
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

   def compute(callback: (S,Computation[S]) => Unit): Map[String,Long] = {
      withProcessInc(callback).masterEngine.aggAccums
         .map{case (k,v) => (k, v.value.longValue)}
   }

   def numValidSubgraphs(): Long = {
      compute()(SparkFromScratchMasterEngine.VALID_SUBGRAPHS)
   }

   /**
    * Output: subgraphs
    */
   private var subgraphsOpt: Option[RDD[ResultSubgraph[_]]] = None

   def subgraphs: RDD[ResultSubgraph[_]] = subgraphs((_, _) => true)

   def subgraphs(shouldOutput: (S,Computation[S]) => Boolean)
   : RDD[ResultSubgraph[_]] = {
      if (config.confs.contains(SparkConfiguration.COMPUTATION_CONTAINER)) {
         var thisWithOutput = withOutput(shouldOutput)
         logInfo (s"Before setting path: ${thisWithOutput}")
         thisWithOutput = thisWithOutput.set(
            "output_path",
            s"${config.getOutputPath}-${step}"
         )
         //thisWithOutput.config.setOutputPath(
         //  s"${thisWithOutput.config.getOutputPath}-${step}")

         logInfo (s"Output to get Subgraphs: ${this} ${thisWithOutput}")
         thisWithOutput.masterEngine.getSubgraphs
      } else {
         subgraphsOpt match {
            case None if config.isOutputActive =>
               val _Subgraphs = masterEngine.getSubgraphs
               subgraphsOpt = Some(_Subgraphs)
               _Subgraphs

            case Some(_Subgraphs) if config.isOutputActive =>
               _Subgraphs

            case _ =>
               masterEngineOpt = None
               subgraphsOpt = None
               config.set ("output_active", true)
               subgraphs
         }
      }
   }

   @Deprecated
   def internalSubgraphs: RDD[S] = internalSubgraphs((_,_) => true)

   @Deprecated
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
   @Deprecated
   def registeredAggregations: Array[String] = {
      config.getAggregationsMetadata.map (_._1).toArray
   }

   @Deprecated
   def aggregationMapWithCallback
   [K <: Writable : ClassTag, V <: Writable : ClassTag]
   (name: String, callback: SubgraphCallback[S], reductionFunc: (V,V) => V)
   : Map[K,V] = {
      aggregationStorageWithCallback[K,V](name, callback, reductionFunc)
         .getMapping
   }

   @Deprecated
   def aggregationStorageWithCallback
   [K <: Writable : ClassTag, V <: Writable : ClassTag]
   (name: String, callback: SubgraphCallback[S], reductionFunc: (V,V) => V)
   : AggregationStorage[K,V] = {
      val aggStorageClass = classOf[AggregationStorage[K,V]]
      val persistent = false
      val endAggregationFunction = null
      val isIncremental = false
      val reductionFunction = new ReductionFunctionContainer[V](reductionFunc)

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
         callback.init(c)
      }

      val aggRes = withInitAggregations (initAggregations)
         .withProcessInc((s,c) => callback.apply(s,c))

      aggRes.masterEngine.getAggregatedValue[AggregationStorage[K,V]](name)
   }

   @Deprecated
   def aggregationMap2
   [K <: Writable : ClassTag, V <: Writable : ClassTag]
   (
      keyFunc: (S, Computation[S], K) => K,
      valueFunc: (S, Computation[S], V) => V,
      reductionFunc: (V,V) => V)
   : Map[K,V] = {
      aggregationStorage2[K,V](keyFunc, valueFunc, reductionFunc).getMapping
   }

   @Deprecated
   def aggregationStorage2
   [K <: Writable : ClassTag, V <: Writable : ClassTag]
   (
      keyFunc: (S, Computation[S], K) => K,
      valueFunc: (S, Computation[S], V) => V,
      reductionFunc: (V,V) => V)
   : AggregationStorage[K,V] = {
      val name = UUID.randomUUID().toString
      val callback = new SubgraphCallback[S] {
         private var aggregationStorage: AggregationStorage[K,V] = _
         private var reusableKey: K = _
         private var reusableValue: V = _

         override def apply(s: S, c: Computation[S]): Unit = {
            aggregationStorage.aggregateWithReusables(
               keyFunc(s, c, reusableKey),
               valueFunc(s, c, reusableValue)
            )
         }

         override def init(computation: Computation[S]): Unit = {
            val engine = computation.getExecutionEngine
            if (engine != null) {
               aggregationStorage = engine.getAggregationStorage(name)
               reusableKey = aggregationStorage.reusableKey()
               reusableValue = aggregationStorage.reusableValue()
            }
         }
      }

      aggregationStorageWithCallback(name, callback, reductionFunc)
   }

   /**
    * Get aggregation mappings defined by the user or empty if it does not exist
    */
   @Deprecated
   def aggregationMap [K <: Writable, V <: Writable](name: String): Map[K,V] = {
      val aggValue = aggregationStorage[K,V](name)
      if (aggValue == null) Map.empty[K,V]
      else aggValue.getMapping
   }

   /**
    * Get aggregation mappings defined by the user or empty if it does not exist
    */
   @Deprecated
   def aggregationStorage [K <: Writable, V <: Writable] (name: String)
   : AggregationStorage[K,V] = {
      withAggregation(name)
         .masterEngine.getAggregatedValue[AggregationStorage[K,V]](name)
   }

   /*
    * Get aggregations defined by the user as an RDD or empty if it does not
    * exist.
    */
   @Deprecated
   def aggregationRDD [K <: Writable, V <: Writable](name: String)
   : RDD[(SerializableWritable[K],SerializableWritable[V])] = {
      sparkContext.parallelize (aggregationMap[K,V](name).toSeq.
         map {
            case (k,v) => (new SerializableWritable(k), new SerializableWritable(v))
         }, config.numPartitions)
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
         .withProcessInc((s,c) => callback.apply(s,c))
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
         .withProcessInc((s,c) => providedCallback.apply(s,c))
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
         .withProcessInc((s,c) => callback.apply(s,c))
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
         .withProcessInc((s,c) => callback.apply(s,c))
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
         .withProcessInc((s,c) => providedCallback.apply(s,c))
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
         .withInitAggregations(c => callback
            .init(c))
         .withProcessInc((s,c) => callback.apply(s,c))
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
         .withProcessInc((s,c) => providedCallback.apply(s,c))
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
         .withProcessInc((s,c) => callback.apply(s,c))
         .masterEngineImmutable
         .objObjRDD[Pattern,V](key, value, aggregate)
         .reduceByKey{case (v1,v2) => aggregate(v1, v2); v1}
         .map(kv => {kv._1.turnCanonical(); kv})
         .reduceByKey{case (v1,v2) => aggregate(v1, v2); v1}

      objObjRDD
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
   @Deprecated
   def saveSubgraphsAsSequenceFile(path: String): Unit = subgraphsOpt match {
      case None =>
         logInfo ("no subgraphs found, computing them ... ")
         config.setOutputPath (path)
         subgraphs.count

      case Some(_Subgraphs) =>
         logInfo (
            s"found results, renaming from ${config.getOutputPath} to ${path}")
         val fs = FileSystem.get(sparkContext.hadoopConfiguration)
         fs.rename (new Path(config.getOutputPath), new Path(path))
         if (config.getOutputPath != path) subgraphsOpt = None
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
               s" Please start with 'vfractoid' or" +
               s" 'efractoid' or 'pfractoid' from fractalGraph." +
               s" Exception message: ${e.getMessage}")
            return null
      }
   }

   /**
    * Copies this fractoid while setting a new unique step id (incremental
    * per JVM instance)
    * @return fractoid with unique step id
    */
   private def withNextStepId: Fractoid[S] = {
      this.copy(step = Fractoid.nextStepId.getAndIncrement())
   }

   /**
    * Handle the creation of a next result.
    */
   private def handleNextResult(result: Fractoid[S],
                                newConfig: SparkConfiguration[S] = config)
   : Fractoid[S] = if (result.mustSync) {
      logInfo (s"HandleNextResultSync between ${this} and ${result}")
      result.copy(scope = this.scope + 1,
         step = this.step + 1, parentOpt = Some(this))
   } else {
      logInfo (s"HandleNextResultAppend ${result} to  ${this}")
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
    * Adds a callback to this workflow
    * @param callback function to be applied to each valid subgraph
    * @return the new fractoid
    */
   def process(callback: (S,Computation[S]) => Unit): Fractoid[S] = {
      withProcessInc(callback)
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
      if (getComputationContainer[S] == null) {
         stepResult = withFirstComputation
         logInfo(s"ExpandNewComputation(n=${n}): before=${this} after=${stepResult}")
      } else {
         val expandComp = emptyComputation(Primitive.E).
            withShouldBypass(false).
            withExpandCompute(null).
            copy(mustSync = false)
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
         //withExpandCompute((e,c) => c.bypass(e)).
         withShouldBypass(true).
         withFilter(filter)
      val result = handleNextResult(filterComp)
      logInfo (s"Filter before: ${this} after: ${result}")
      result
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

      var filterComp = emptyComputation(Primitive.F).
         //withExpandCompute((e,c) => c.bypass(e)).
         withShouldBypass(true)

      logInfo (s"FilterCompBefore ${filterComp}")

      filterComp = filterComp.
         withFilter(filterFunc).
         copy(mustSync = true)

      logInfo (s"FilterCompAfter ${filterComp}")

      val result = handleNextResult(filterComp)
      logInfo (s"FilterAgg before: ${this} after: ${result}")
      result
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
   def aggregate [K <: Writable : ClassTag, V <: Writable : ClassTag]
   (name: String,
    aggregationKey: (S,Computation[S],K) => K,
    aggregationValue: (S,Computation[S],V) => V,
    reductionFunction: (V,V) => V,
    endAggregationFunction: EndAggregationFunction[K,V] = null,
    isIncremental: Boolean = false
   ): Fractoid[S] = {
      val aggResult = withAggregationRegistered(
         name,
         aggregationKey = aggregationKey,
         aggregationValue = aggregationValue,
         reductionFunction = new ReductionFunctionContainer(reductionFunction),
         endAggregationFunction = endAggregationFunction,
         isIncremental = isIncremental)
      logInfo(s"Aggregate before=${this} after=${aggResult}")
      aggResult
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
   @Deprecated
   def vfilter [V] (vfilter: Vertex[V] => Boolean): Fractoid[S] = {
      val vpred = new VertexFilterFunc[V] {
         override def test(v: Vertex[V]): Boolean = vfilter(v)
      }

      if (getComputationContainer[S] == null) {
         withFirstComputation.
            //withExpandCompute((e,c) => c.bypass(e)).
            withShouldBypass(true).
            set("vfilter", vpred)
      } else {
         val filterComp = emptyComputation.
            //withExpandCompute((e,c) => c.bypass(e)).
            withShouldBypass(true).
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
   @Deprecated
   def efilter [E] (efilter: Edge[E] => Boolean): Fractoid[S] = {
      val epred = new EdgeFilterFunc[E] {
         override def test(e: Edge[E]): Boolean = efilter(e)
      }

      if (getComputationContainer[S] == null) {
         withFirstComputation.
            //withExpandCompute((e, c) => c.bypass(e)).
            withShouldBypass(true).
            set("efilter", epred)
      } else {
         val filterComp = emptyComputation.
            //withExpandCompute((e, c) => c.bypass(e)).
            withShouldBypass(true).
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
      val result = this.copy (config = newConfig)
      logInfo (s"WithProcess before: ${this} after: ${result}")
      logInfo (s"WithProcessComp before: ${getComputationContainer[S]} after: ${newComp}")
      result
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
    * @param name aggregation name
    * @return new result
    */
   @Deprecated
   private def withAggregation [K <: Writable, V <: Writable]
   (name: String): Fractoid[S] = {

      if (!aggCallbacks.get(name).isDefined) {
         logWarning (s"Unknown aggregation ${name}." +
            s" Please register it first with *withAggregationRegistered*")
         return this
      }

      val callback = aggCallbacks(name)
      withProcessInc((s,c) => callback.apply(s,c))
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
      val initComp = getComputationContainer[S].withNewFunctions (
            initAggregationsOpt = Option(initAggregations))
      val newConfig = config.withNewComputation(initComp)
      val initRes = this.copy (config = newConfig)
      logInfo(s"WithInitAggregations before=${this} after=${initRes}")
      initRes
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

      val callback = new SubgraphCallback[S] {
         private var aggregationStorage: AggregationStorage[K,V] = _
         private var reusableKey: K = _
         private var reusableValue: V = _

         override def apply(s: S, c: Computation[S]): Unit = {
            aggregationStorage.aggregateWithReusables(
               aggregationKey(s, c, reusableKey),
               aggregationValue(s, c, reusableValue)
            )
         }

         override def init(computation: Computation[S]): Unit = {
            val engine = computation.getExecutionEngine
            if (engine != null) {
               aggregationStorage = engine.getAggregationStorage(name)
               reusableKey = aggregationStorage.reusableKey()
               reusableValue = aggregationStorage.reusableValue()
            }
         }
      }

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
         callback.init(c)
      }

      val aggRes = withInitAggregations (initAggregations).copy (
         aggCallbacks = aggCallbacks ++ Map(name -> callback)
      )

      logInfo(s"WithAggregationRegistered before=${this} after=${aggRes}")

      aggRes
   }

   /**
    * Specify a custom expand function to the computation
    *
    * @param expandCompute expand function
    *
    * @return new result
    */
   private def withExpandCompute (expandCompute: (S,Computation[S]) => SubgraphEnumerator[S])
   : Fractoid[S] = {
      val newConfig = if (expandCompute != null) {
         config.withNewComputation (
            getComputationContainer[S].withNewFunctions (
               expandComputeOpt = Option((e,c) => expandCompute(e,c))
            )
         )
      } else {
         config.withNewComputation (
            getComputationContainer[S].withNewFunctions (expandComputeOpt = None)
         )
      }
      this.copy (config = newConfig)
   }

   private def withShouldBypass (bypass: Boolean): Fractoid[S] = {
      val newConfig = config.withNewComputation(
         getComputationContainer[S].withNewFunctions(
            shouldBypassOpt = Option(bypass), expandComputeOpt = Option(null))
      )
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
      val result = this.copy (config = _newConfig)
      logInfo (s"Result after appending: ${result}")
      result
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

object Fractoid {
   private val nextStepId = new AtomicInteger(0)
}
