package br.ufmg.cs.systems.fractal.conf

import br.ufmg.cs.systems.fractal.computation._
import br.ufmg.cs.systems.fractal.conf.Configuration._
import br.ufmg.cs.systems.fractal.graph._
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.util.Logging

import java.net.InetAddress
import scala.collection.mutable.Map

/**
 * Configurations are passed along in this mapping
 */
case class SparkConfiguration(confs: Map[String, Any])
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
      this(Map(CONF_MASTER_HOSTNAME -> InetAddress.getLocalHost.getHostAddress))
   }

   /**
    * Sets a configuration (mutable)
    */
   def set(key: String, value: Any): Unit = {
      confs.update(key, value)
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
         getInteger("num_compute_threads",
            Runtime.getRuntime.availableProcessors))

   /**
    * Update assign internal names to user defined properties
    */
   private def fixAssignments = {
      def updateIfExists(key: String, config: String) = confs
         .remove(key) match {
         case Some(value) => confs.update(config, value)
         case None =>
      }

      // collect thread stats
      updateIfExists("collect_thread_stats", CONF_COLLECT_THREAD_STATS)

      // thread stats key
      updateIfExists("thread_stats_key", CONF_THREAD_STATS_KEY)

      // log level
      updateIfExists("log_level", CONF_LOG_LEVEL)

      // info period
      updateIfExists("info_period", INFO_PERIOD)

      // start time
      updateIfExists("start_time", CONF_START_TIME_MS)

      // time limit
      updateIfExists("time_limit", CONF_TIME_LIMIT_MS)

      // computation classes
      updateIfExists("computation", CONF_COMPUTATION_CLASS)

      // work stealing
      updateIfExists("ws_internal", CONF_WS_INTERNAL)
      updateIfExists("ws_external", CONF_WS_EXTERNAL)
      updateIfExists("ws_external_batchsize", CONF_WS_EXTERNAL_BATCHSIZE)

      // input
      updateIfExists("input_graph_class", CONF_MAINGRAPH_CLASS)
      updateIfExists("input_graph_path", CONF_MAINGRAPH_PATH)
      updateIfExists("edge_labeled", CONF_MAINGRAPH_EDGE_LABELED)
      updateIfExists("vertex_labeled", CONF_MAINGRAPH_VERTEX_LABELED)

      // multigraph
      updateIfExists("multigraph", CONF_MAINGRAPH_MULTIGRAPH)

      // edge filtering predicate
      updateIfExists("edge_filtering_predicate",
         CONF_MAINGRAPH_EDGE_FILTERING_PREDICATE)

      // vertex filtering predicate
      updateIfExists("vertex_filtering_predicate",
         CONF_MAINGRAPH_VERTEX_FILTERING_PREDICATE)
   }

   def initializeWithTag(isMaster: Boolean): Unit = synchronized {
      initialize(isMaster)
   }

   /**
    * Garantees that fractal configuration is properly set
    *
    * TODO: generalize the initialization in the superclass Configuration
    */
   override def initialize(isMaster: Boolean = false): Unit = synchronized {
      logDebug(s"Initializing config=${this}" +
         s" mainGraph=${getMainGraph()} isMainGraphRead=${isMainGraphRead()}" +
         s" isMaster=${isMaster}")

      if (!isInitialized) {
         initializeInstance(!isMaster)
      }

      if (getMainGraph == null || !isMainGraphRead()) {
         val graph = getOrCreateMainGraph()
         setMainGraph(graph)
         logInfo(s"Graph created graph=${graph}")
      }

      if (!isMaster && !isMainGraphRead()) {
         //setGraph()
         mainGraph.synchronized {
            if (!isMainGraphRead) {
               logInfo("MainGraph is empty, gonna try reading it")
               readMainGraph()
            }
         }
      }
   }

   /**
    * Called whether no fractal configuration is set in the running jvm
    */
   private def initializeInstance(shouldSetGraph: Boolean = true): Unit = {
      fixAssignments

      // periodic information about execution
      infoPeriod = getLong(INFO_PERIOD, INFO_PERIOD_DEFAULT_MS)
      startTime = getLong(CONF_START_TIME_MS, CONF_START_TIME_MS_DEFAULT)
      timeLimit = getLong(CONF_TIME_LIMIT_MS, CONF_TIME_LIMIT_MS_DEFAULT)

      // common configs
      setMainGraphClass(
         getClass(CONF_MAINGRAPH_CLASS, CONF_MAINGRAPH_CLASS_DEFAULT).
            asInstanceOf[Class[_ <: MainGraph]]
      )

      setIsGraphEdgeLabeled(getBoolean(CONF_MAINGRAPH_EDGE_LABELED,
         CONF_MAINGRAPH_EDGE_LABELED_DEFAULT))

      setIsGraphVertexLabeled(getBoolean(CONF_MAINGRAPH_VERTEX_LABELED,
         CONF_MAINGRAPH_VERTEX_LABELED_DEFAULT))

      setComputationClass(
         getClass(CONF_COMPUTATION_CLASS, CONF_COMPUTATION_CLASS_DEFAULT).
            asInstanceOf[Class[_ <: Computation[_]]]
      )

      setPatternClass(
         getClass(CONF_PATTERN_CLASS, CONF_PATTERN_CLASS_DEFAULT).
            asInstanceOf[Class[_ <: Pattern]]
      )

      isGraphMulti = getBoolean(CONF_MAINGRAPH_MULTIGRAPH,
         CONF_MAINGRAPH_MULTIGRAPH_DEFAULT)

      setEdgeFilteringPredicate(
         getValue(CONF_MAINGRAPH_EDGE_FILTERING_PREDICATE, null)
            .asInstanceOf[EdgeFilteringPredicate]
      )

      setVertexFilteringPredicate(
         getValue(CONF_MAINGRAPH_VERTEX_FILTERING_PREDICATE, null)
            .asInstanceOf[VertexFilteringPredicate]
      )

      initialized = true
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