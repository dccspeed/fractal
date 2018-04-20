package io.arabesque.computation

import java.io.OutputStreamWriter

import io.arabesque.aggregation.{AggregationStorage, AggregationStorageFactory}
import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding._
import io.arabesque.utils.{Logging, SerializableWritable}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{Writer => SeqWriter}
import org.apache.hadoop.io.{LongWritable, NullWritable, SequenceFile, Writable}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.{LongAccumulator, SizeEstimator}

import scala.collection.JavaConverters._
import scala.collection.mutable.Map

trait SparkEngine [E <: Embedding] 
    extends CommonExecutionEngine[E] with Serializable with Logging {

  var computed = false

  // superstep arguments
  val partitionId: Int
  val superstep: Int
  val accums: Map[String,LongAccumulator]
  val previousAggregationsBc: Broadcast[_]
  def configurationId: Int

  def configuration: SparkConfiguration[E] = {
    Configuration.get(configurationId).asInstanceOf[SparkConfiguration[E]]
  }

  def flush: Iterator[(_,_)]

  setLogLevel (configuration.getLogLevel)

  // computation implements the user algorithm
  var computation: Computation[E] = _

  // aggregation storages
  var aggregationStorageFactory: AggregationStorageFactory = _

  lazy val aggregationStorages
    : Map[String,AggregationStorage[_ <: Writable, _ <: Writable]] = Map.empty

  lazy val aggregationStorageSplits
    : Map[String,Array[(Int,Array[Byte])]] = Map.empty

  /**
   * We assume the number of requested executor cores as the default number of
   * partitions
   */
  def getNumberPartitions: Int = configuration.numPartitions

  // accumulators
  var numEmbeddingsProcessed: Long = _
  var numEmbeddingsGenerated: Long = _
  var numEmbeddingsOutput: Long = _

  def init(): Unit = {
    computation = configuration.createComputation[E]
    var currComp = computation
    while (currComp != null) {
      currComp.setExecutionEngine(this)
      currComp.init(configuration)
      currComp.initAggregations(configuration)
      currComp = currComp.nextComputation()
    }
    computation.setDepth(0)
    
    aggregationStorageFactory = new AggregationStorageFactory(configuration)

    configuration.setEmbeddingClass (computation.getEmbeddingClass())
    numEmbeddingsProcessed = 0
    numEmbeddingsGenerated = 0
    numEmbeddingsOutput = 0
    configuration.getAggregationsMetadata.asScala.keys.foreach { name =>
      val agg = aggregationStorageFactory.createAggregationStorage (name)
      aggregationStorages.update (name, agg)
    }
  }

  override def finalize() = {
    computation = null
    aggregationStorageFactory = null
    if (outputStreamOpt.isDefined) outputStreamOpt.get.close
    if (embeddingWriterOpt.isDefined) embeddingWriterOpt.get.close
  }

  /**
   * Any Spark accumulator used for stats accounting is flushed here
   */
  def flushStatsAccumulators: Unit = {
    accums(SparkMasterEngine.AGG_EMBEDDINGS_PROCESSED).
      add(numEmbeddingsProcessed)
    accums(SparkMasterEngine.AGG_EMBEDDINGS_GENERATED).
      add(numEmbeddingsGenerated)
    accums(SparkMasterEngine.AGG_EMBEDDINGS_OUTPUT).
      add(numEmbeddingsOutput)
    accums.foreach { case (name, accum) =>
      logInfo (s"Accumulator[${superstep}][${partitionId}][${name}]:" +
        s" ${accum.value}")
    }
  }

  def getStatsAccumulators: String = {
    accums.map { case (name, accum) =>
      s"${name}:${accum.value}"
    }.mkString(",")
  }

  /**
   * Flush a serialized aggregation by name
   *
   * @param name name of the aggregation storage
   * @return An iterator of key (destination partition) and serialized
   * aggregation that belongs to key partition
   */
  def flushAggregationsByName[K <: Writable, V <: Writable](
      name: String): Iterator[(Int,Array[Byte])] = {
    aggregationStorageSplits(name).iterator.filter(_ != null)
  }

  /**
   * Aggregates to a single, local storage and then splits again based on the
   * provided keys
   *
   * @param name name of the aggregation
   * @param aggStorage the actual storage that must be aggregated and splitted
   */
  def aggregateAndSplitFinalAggregation[K <: Writable, V <: Writable](
      name: String, aggStorage: AggregationStorage[K,V]): Unit = {
   
    // we lose the unserialized version of this aggregation
    aggregationStorages.remove(name)
    
    // the following function does the final local aggregation
    // e.g. for motifs, turns quick patterns into canonical ones
    def aggregate[K <: Writable, V <: Writable](agg1: AggregationStorage[K,V],
        agg2: AggregationStorage[_,_]) = {
      agg1.finalLocalAggregate (agg2.asInstanceOf[AggregationStorage[K,V]])
    }
    
    val start = System.currentTimeMillis
    logInfo(s"LocalAggregationStorage name=${name}" +
      s" step=${superstep} partitionId=${partitionId}" +
      s" aggStorage=${aggStorage}")

    aggregate (
      configuration.getOrCreateFinalAggStorage(name),
      aggStorage)

    // get a single local (same worker) version of this aggregation
    // this is a shared reference between every engine in this worker
    val (_finalAggStorage, barrier) = configuration.
        maybeReclaimFinalAggStorage(superstep, name)

    val finalAggStorage = _finalAggStorage.asInstanceOf[AggregationStorage[K,V]]

    // wait for the last engine aggregate its content to finalAggStorage
    finalAggStorage.synchronized {
      while (barrier.get != 0) {
        finalAggStorage.wait()
      }
    }

    // at this point the finalAggStorage is complete
    // the following is a sanity notification for all engines still waiting for
    // final aggregation completion
    finalAggStorage.synchronized {
      finalAggStorage.notifyAll()
    }

    val elapsed = System.currentTimeMillis - start

    // setup splits
    val numPartitions = getNumberPartitions()
    val aggStorageSplits = new Array[AggregationStorage[K,V]](numPartitions)
    var i = 0
    while (i < aggStorageSplits.length) {
      aggStorageSplits(i) = aggregationStorageFactory.
          createAggregationStorage (name).
          asInstanceOf[AggregationStorage[K,V]]
      i += 1
    }

    // a key consumer in the single finalAggStorage will allow us to split the
    // aggregated keys in parallel, i.e., each engine will split some portion of
    // the keys.
    val keysConsumer = finalAggStorage.getKeysConsumer()
    var key = keysConsumer.poll()
    var numKeysConsumed = 0
    while (key != null) {
      val value = finalAggStorage.getValue(key)
      var split = key.hashCode() % numPartitions
      if (split < 0) split += numPartitions
      aggStorageSplits(split).transferKeyFrom(key, finalAggStorage)
      numKeysConsumed += 1
      key = keysConsumer.poll()
    }

    val splitMapStr = aggStorageSplits.map(_.getNumberMappings()).mkString(",")
    logInfo (s"FinalAggregationSplit step=${superstep}" +
      s" partitionId=${partitionId}" +
      s" aggregationName=${name}" +
      s" keysConsumerSize=${keysConsumer.size()}" +
      s" aggStorageSplit=${splitMapStr}")

    // setup serialized splits
    val serializedSplits = new Array[(Int,Array[Byte])](numPartitions)

    // serialize the splits between the engines
    i = 0
    while (i < serializedSplits.length) {
      if (aggStorageSplits(i).getNumberMappings() > 0) {
        serializedSplits(i) = (i,
          SparkConfiguration.serialize(aggStorageSplits(i)))
      }
      aggStorageSplits(i) = null
      i += 1
    }
    
    // update the serialized version of those splits
    // not that at this point we already lose the unserilized version
    aggregationStorageSplits.update(name, serializedSplits)
  }

  /**
   * Returns the current value of an aggregation installed in this execution
   * engine.
   *
   * @param name name of the aggregation
   * @return the aggregated value or null if no aggregation was found
   */
  override def getAggregatedValue[A <: Writable](name: String): A = {
    previousAggregationsBc.value.asInstanceOf[Map[String,A]].get(name) match {
      case Some(aggStorage) => aggStorage
      case None =>
        logWarning (s"Previous aggregation storage $name not found")
        null.asInstanceOf[A]
    }
  }

  /**
   * Maps (key,value) to the respective local aggregator
   *
   * @param name identifies the aggregator
   * @param key key to account for
   * @param value value to be accounted for key in that aggregator
   * 
   */
  override def map[K <: Writable, V <: Writable](name: String,
      key: K, value: V) = {
    val aggStorage = getAggregationStorage[K,V] (name)
    aggStorage.aggregateWithReusables (key, value)
  }

  /**
   * Retrieves or creates the local aggregator for the specified name.
   * Obs. the name must match to the aggregator's metadata configured in
   * *initAggregations* (Computation)
   *
   * @param name aggregator's name
   * @return an aggregation storage with the specified name
   */
  override def getAggregationStorage[K <: Writable, V <: Writable](name: String)
      : AggregationStorage[K,V] = {
    try {
      aggregationStorages(name).asInstanceOf[AggregationStorage[K,V]]
    } catch {
      case e: java.util.NoSuchElementException =>
        val agg = aggregationStorageFactory.createAggregationStorage (name)
        aggregationStorages.update (name, agg)
        agg.asInstanceOf[AggregationStorage[K,V]]
    }
  }

  override def aggregate(name: String, value: LongWritable) = {
    aggregate(name, value.get)
  }
  
  override def aggregate(name: String, value: Long) = {
    if (accums.contains(name)) {
      accums(name).add(value)
    }
  }

  // output
  @transient val outputFunc = {
    import SparkConfiguration.{OUTPUT_PLAIN_TEXT, OUTPUT_SEQUENCE_FILE}
    configuration.getOutputFormat match {
      case OUTPUT_PLAIN_TEXT if configuration.isOutputActive =>
        (e: Embedding) => {
          outputPlainText(e)
          numEmbeddingsOutput += 1
        }

      case OUTPUT_SEQUENCE_FILE if configuration.isOutputActive =>
        (e: Embedding) => {
          outputSequenceFile(e)
          numEmbeddingsOutput += 1
        }

      case _ => (e: Embedding) => {}
    }
  }

  @transient var embeddingWriterOpt: Option[SeqWriter] = None

  @transient var outputStreamOpt: Option[OutputStreamWriter] = None

  @transient lazy val outputPath: Path = new Path(configuration.getOutputPath)

  /**
   * Output the embedding using te configured output function 'outputFunc'
   *
   * @param embedding embedding to output
   */
  override def output(embedding: Embedding) = outputFunc(embedding)

  /**
   * Output embedding to a sequence file
   */
  private def outputSequenceFile(embedding: Embedding) = {
    if (embeddingWriterOpt.isDefined) {
      val embeddingWriter = embeddingWriterOpt.get
      val resEmbedding = ResultEmbedding (embedding, configuration)
      embeddingWriter.append (NullWritable.get, resEmbedding)
      numEmbeddingsOutput += 1
    } else {
      // we must decide at runtime the concrete Writable to be used
      val resEmbeddingClass = if (embedding.isInstanceOf[EdgeInducedEmbedding])
        classOf[EEmbedding]
      else if (embedding.isInstanceOf[VertexInducedEmbedding])
        classOf[VEmbedding]
      else
        classOf[ResultEmbedding[_]] // not allowed, will crash

      // instantiate the embedding writer (sequence file)
      val superstepPath = new Path(outputPath, s"${getSuperstep}")
      val partitionPath = new Path(superstepPath, s"${partitionId}")

      logInfo (s"Output stream (sequence-file) created: " +
        s" step=${getSuperstep} partitionId=${partitionId}" +
        s" partitionPath=${partitionPath}")

      val embeddingWriter = SequenceFile.createWriter(configuration.hadoopConf,
        SeqWriter.file(partitionPath),
        SeqWriter.keyClass(classOf[NullWritable]),
        SeqWriter.valueClass(resEmbeddingClass))

      embeddingWriterOpt = Some(embeddingWriter)
      
      val resEmbedding = ResultEmbedding (embedding, configuration)
      embeddingWriter.append (NullWritable.get, resEmbedding)
      numEmbeddingsOutput += 1
    }
  }

  /**
   * Output embedding to a plain text
   */
  private def outputPlainText(embedding: Embedding) = {
    if (outputStreamOpt.isDefined) {
      val outputStream = outputStreamOpt.get
      outputStream.write(embedding.toOutputString)
      outputStream.write("\n")
    } else {
      val fs = FileSystem.get(configuration.hadoopConf)
      val superstepPath = new Path(outputPath, s"${getSuperstep}")
      val partitionPath = new Path(superstepPath, s"${partitionId}")

      logInfo (s"Output stream (text-file) created: " +
        s" step=${getSuperstep} partitionId=${partitionId}" +
        s" partitionPath=${partitionPath}")

      val outputStream = new OutputStreamWriter(fs.create(partitionPath))
      outputStreamOpt = Some(outputStream)
      outputStream.write(embedding.toOutputString)
      outputStream.write("\n")
    }
  }
  
  // other functions
  override def getPartitionId() = partitionId

  override def getSuperstep() = superstep

  def getConfig: SparkConfiguration[E] = configuration
}

