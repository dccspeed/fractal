package br.ufmg.cs.systems.fractal.conf

import java.io._
import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Predicate

import br.ufmg.cs.systems.fractal.aggregation._
import br.ufmg.cs.systems.fractal.computation._
import br.ufmg.cs.systems.fractal.conf.Configuration._
import br.ufmg.cs.systems.fractal.graph._
import br.ufmg.cs.systems.fractal.optimization.OptimizationSetDescriptor
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray
import br.ufmg.cs.systems.fractal.util.{JVMProfiler, Logging, SerializableConfiguration}
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.io.Writable
import org.apache.spark.SparkConf

import scala.collection.mutable.Map

/**
 * Configurations are passed along in this mapping
 */
case class SparkConfiguration(confs: Map[String,Any])
   extends Configuration with Logging {

   // from scratch computation must have incremental aggregations
   fixAssignments

   // master hostname
   if (!confs.contains(CONF_MASTER_HOSTNAME)) try {
      confs.update(CONF_MASTER_HOSTNAME,
         java.net.InetAddress.getLocalHost().getHostAddress())
   } catch {
      case e: java.net.UnknownHostException =>
         throw e
   }

   def this() {
      this (Map(CONF_MASTER_HOSTNAME -> InetAddress.getLocalHost.getHostAddress))
   }

   /**
    * Sets a configuration (mutable)
    */
   def set(key: String, value: Any): Unit = {
      confs.update (key, value)
      fixAssignments
   }

   /**
    * We assume the number of requested executor cores as an alternative number
    * of partitions. However, by the time we call this function, the config
    * *num_partitions* should be already set by the user, or by the execution
    * master engine which has SparkContext.defaultParallelism as default
    */
   def numPartitions: Int = getInteger("num_partitions",
      getInteger("num_workers", 1) *
         getInteger("num_compute_threads", Runtime.getRuntime.availableProcessors))

   /**
    * Update assign internal names to user defined properties
    */
   private def fixAssignments = {
      def updateIfExists(key: String, config: String) = confs.remove (key) match {
         case Some(value) => confs.update (config, value)
         case None =>
      }

      // log level
      updateIfExists ("log_level", CONF_LOG_LEVEL)

      // info period
      updateIfExists ("info_period", INFO_PERIOD)

      // computation classes
      updateIfExists ("master_computation", CONF_MASTER_COMPUTATION_CLASS)
      updateIfExists ("computation", CONF_COMPUTATION_CLASS)

      // communication strategy
      updateIfExists ("comm_strategy", CONF_COMM_STRATEGY)

      // gtag
      updateIfExists ("gtag_batch_low", CONF_WS_EXTERNAL_BATCHSIZE_LOW)
      updateIfExists ("gtag_batch_high", CONF_WS_EXTERNAL_BATCHSIZE_HIGH)

      // work stealing
      updateIfExists ("ws_internal", CONF_WS_INTERNAL)
      updateIfExists ("ws_external", CONF_WS_EXTERNAL)

      // enumerator class
      updateIfExists ("subgraph_enumerator", CONF_ENUMERATOR_CLASS)

      // input
      updateIfExists ("input_graph_class", CONF_MAINGRAPH_CLASS)
      updateIfExists ("input_graph_path", CONF_MAINGRAPH_PATH)
      updateIfExists ("input_graph_local", CONF_MAINGRAPH_LOCAL)
      updateIfExists ("edge_labelled", CONF_MAINGRAPH_EDGE_LABELLED)

      // output
      updateIfExists ("output_active", CONF_OUTPUT_ACTIVE)
      updateIfExists ("output_path", CONF_OUTPUT_PATH)
      updateIfExists ("output_format", CONF_OUTPUT_FORMAT)

      // aggregation
      updateIfExists ("incremental_aggregation", CONF_INCREMENTAL_AGGREGATION)

      // multigraph
      updateIfExists ("multigraph", CONF_MAINGRAPH_MULTIGRAPH)

      // jvm profiler cmd
      updateIfExists ("jvmprof_cmd", CONF_JVMPROF_CMD)
   }

   var tagApplied = false

   def initializeWithTag(isMaster: Boolean): Unit = synchronized {
      initialize(isMaster)
      if (!tagApplied) {

         val startTag = System.currentTimeMillis

         val ret = (confs.get("vtag"), confs.get("etag")) match {
            case (Some(vtag : AtomicBitSetArray), Some(etag : AtomicBitSetArray)) =>
               tagApplied = true
               getMainGraph[MainGraph[_,_]].filter(vtag, etag)

            case other =>
               0
         }

         val elapsedTag = System.currentTimeMillis - startTag

         if (ret > 0) {
            logInfo (s"GraphTagging took ${elapsedTag} return=${ret}")
         }

         val startFilter = System.currentTimeMillis

         //if (!confs.contains("vfilter")) {
         getMainGraph[MainGraph[_,_]].undoVertexFilter()
         //}

         //if (!confs.contains("efilter")) {
         getMainGraph[MainGraph[_,_]].undoEdgeFilter()
         //}

         def filterVertices[V,E](graph: MainGraph[V,E],
                                 vpred: Predicate[_]): Int = {
            graph.filterVertices(vpred.asInstanceOf[Predicate[Vertex[V]]])
         }

         val removedVertices = confs.get("vfilter") match {
            case Some(vpred: Predicate[_]) =>
               tagApplied = true
               filterVertices(getMainGraph[MainGraph[_,_]], vpred)

            case other =>
               0
         }

         def filterEdges[V,E](graph: MainGraph[V,E],
                              epred: Predicate[_]): Int = {
            graph.filterEdges(epred.asInstanceOf[Predicate[Edge[E]]])
         }

         val removedEdges = confs.get("efilter") match {
            case Some(epred: Predicate[_]) =>
               tagApplied = true
               filterEdges(getMainGraph[MainGraph[_,_]], epred)

            case other =>
               0
         }

         val elapsedFilter = System.currentTimeMillis - startFilter
         //System.gc()

         if (removedVertices + removedEdges > 0) {
            logInfo (s"GraphFiltering took ${elapsedFilter} ms" +
               s" removedVertices=${removedVertices} removedEdges=${removedEdges}")
         }

         if (ret + removedVertices + removedEdges > 0) {
            getMainGraph[MainGraph[_,_]].afterGraphUpdate()
         }
      }
   }

   /**
    * Garantees that fractal configuration is properly set
    *
    * TODO: generalize the initialization in the superclass Configuration
    */
   override def initialize(isMaster: Boolean = false): Unit = synchronized {
      logDebug(s"Initializing config, id=${id} config=${this}" +
         s" mainGraph=${getMainGraph()} isMainGraphRead=${isMainGraphRead()}" +
         s" isMaster=${isMaster}")

      if (!isInitialized) {
         initializeInstance(!isMaster)
      }

      if (getMainGraph == null || !isMainGraphRead()) {
         val graph = getOrCreateMainGraph()
         setMainGraph(graph)
         logInfo(s"Graph created, configId=${id} graph=${graph}")
      }

      if (!isMaster && !isMainGraphRead()) {
         setGraph()
         val optimizationSet = getOptimizationSet()
         optimizationSet.applyAfterGraphLoad()
         logInfo(s"Graph read, configId=${id} graph=${getMainGraph()}" +
            s" optimizationSet= ${optimizationSet}")
      }
   }

   /**
    * Called whether no fractal configuration is set in the running jvm
    */
   private def initializeInstance(shouldSetGraph: Boolean = true): Unit = {

      fixAssignments

      // periodic information about execution
      infoPeriod = getLong(INFO_PERIOD, INFO_PERIOD_DEFAULT)

      // common configs
      setMainGraphClass (
         getClass (CONF_MAINGRAPH_CLASS, CONF_MAINGRAPH_CLASS_DEFAULT).
            asInstanceOf[Class[_ <: MainGraph[_,_]]]
      )

      setIsGraphEdgeLabelled (getBoolean (CONF_MAINGRAPH_EDGE_LABELLED,
         CONF_MAINGRAPH_EDGE_LABELLED_DEFAULT))

      setMasterComputationClass (
         getClass (CONF_MASTER_COMPUTATION_CLASS,
            CONF_MASTER_COMPUTATION_CLASS_DEFAULT).
            asInstanceOf[Class[_ <: MasterComputation]]
      )

      setComputationClass (
         getClass (CONF_COMPUTATION_CLASS, CONF_COMPUTATION_CLASS_DEFAULT).
            asInstanceOf[Class[_ <: Computation[_]]]
      )

      setPatternClass (
         getClass (CONF_PATTERN_CLASS, CONF_PATTERN_CLASS_DEFAULT).
            asInstanceOf[Class[_ <: Pattern]]
      )

      setSubgraphEnumClass(
         getClass (CONF_ENUMERATOR_CLASS, CONF_ENUMERATOR_CLASS_DEFAULT).
            asInstanceOf[Class[_ <: SubgraphEnumerator[_]]]
      )

      isGraphMulti = getBoolean(CONF_MAINGRAPH_MULTIGRAPH,
         CONF_MAINGRAPH_MULTIGRAPH_DEFAULT)

      // optimizations
      setOptimizationSetDescriptorClass (
         getClass (CONF_OPTIMIZATIONSETDESCRIPTOR_CLASS,
            CONF_OPTIMIZATIONSETDESCRIPTOR_CLASS_DEFAULT).
            asInstanceOf[Class[_ <: OptimizationSetDescriptor]]
      )

      val optimizationSet = getOptimizationSet()
      logInfo (s"Active optimizations (applyStartup): ${optimizationSet}")
      optimizationSet.applyStartup()

      setAggregationsMetadata (new java.util.HashMap())

      setOutputPath (getString(CONF_OUTPUT_PATH, CONF_OUTPUT_PATH_DEFAULT))

      JVMProfiler.instance().init(this);

      initialized = true
   }

   private def setGraph(): Boolean = {
      var graphRead = false

      // in case of the mainGraph is empty (no vertices and no edges), we try to
      // read it
      getMainGraph[MainGraph[_,_]].synchronized {
         if (!isMainGraphRead) {
            logInfo ("MainGraph is empty, gonna try reading it")
            readMainGraph()
            graphRead = true
         }
      }

      graphRead
   }

  def getValue(key: String, defaultValue: Any): Any = confs.get(key) match {
    case Some(value) => value
    case None => defaultValue
  }

  override def getObject[T](key: String, defaultValue: T): T = {
    getValue(key, defaultValue).asInstanceOf[T]
  }

  override def getInteger(key: String, defaultValue: Integer) =
    getValue(key, defaultValue).asInstanceOf[Int]
  
  override def getLong(key: String, defaultValue: java.lang.Long) =
    getValue(key, defaultValue).asInstanceOf[Long]
  
  override def getDouble(key: String, defaultValue: java.lang.Double) =
    getValue(key, defaultValue).asInstanceOf[Double]
  
  override def getFloat(key: String, defaultValue: java.lang.Float) =
    getValue(key, defaultValue).asInstanceOf[Float]

  override def getString(key: String, defaultValue: String) =
    getValue(key, defaultValue).asInstanceOf[String]
  
  override def getBoolean(key: String, defaultValue: java.lang.Boolean) = {
    val value = getValue(key, defaultValue)
    if (value.isInstanceOf[java.lang.Boolean]) {
      value.asInstanceOf[java.lang.Boolean]
    } else if (value.isInstanceOf[String]) {
      new java.lang.Boolean(value.asInstanceOf[String])
    } else {
      throw new RuntimeException(s"Invalid boolean for (${key}, ${value})")
    }
  }

   override def toString: String = {
      s"SparkConfiguration(${confs})"
   }
}

object SparkConfiguration extends Logging {
   // re-enumerates from scratch every superstep (aggregation based)
   val COMM_FROM_SCRATCH = "scratch"

   // auxiliary functions
   def serialize[T](obj: T): Array[Byte] = {
      val baos = new ByteArrayOutputStream
      val oos = new ObjectOutputStream(baos)
      oos.writeObject(obj)
      oos.close
      baos.toByteArray
   }

   def deserialize[T](bytes: Array[Byte]): T = {
      val bais = new ByteArrayInputStream(bytes)
      deserialize(bais)
   }

   def deserialize[T](is: InputStream): T = {
      val ois = new ObjectInputStream(is)
      ois.readObject().asInstanceOf[T]
   }

   def clone[T](obj: T): T = {
      deserialize(serialize(obj))
   }
}
