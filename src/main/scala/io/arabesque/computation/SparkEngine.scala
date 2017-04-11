package io.arabesque.computation

import java.io.OutputStreamWriter

import io.arabesque.aggregation.{AggregationStorage, AggregationStorageFactory}
import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding._
import io.arabesque.utils.Logging

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, SequenceFile, Writable}
import org.apache.hadoop.io.SequenceFile.{Writer => SeqWriter}

import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.Map
import scala.reflect.ClassTag

trait SparkEngine [E <: Embedding] 
    extends CommonExecutionEngine[E] with Serializable with Logging {

  var computed = false

  // superstep arguments
  val partitionId: Int
  val superstep: Int
  val accums: Map[String,Accumulator[_]]
  val previousAggregationsBc: Broadcast[_]

  setLogLevel (configuration.getLogLevel)

  // configuration has input parameters, computation knows how to ensure
  // arabesque's computational model
  @transient lazy val configuration: SparkConfiguration[E] = {
    val configuration = Configuration.get [SparkConfiguration[E]]
    configuration
  }

  // computation implements the user algorithm
  @transient lazy val computation: Computation[E] = {
    val computation = configuration.createComputation [E]
    computation.setUnderlyingExecutionEngine (this)
    computation.init()
    computation.initAggregations()
    computation
  }

  // aggregation storages
  @transient lazy val aggregationStorageFactory = new AggregationStorageFactory
  lazy val aggregationStorages
    : Map[String,AggregationStorage[_ <: Writable, _ <: Writable]] = Map.empty

  /**
   * We assume the number of requested executor cores as the default number of
   * partitions
   */
  def getNumberPartitions: Int = configuration.numPartitions

  // accumulators
  var numEmbeddingsProcessed: Long = 0
  var numEmbeddingsGenerated: Long = 0
  var numEmbeddingsOutput: Long = 0

  def init(): Unit = {}

  /**
   * Any Spark accumulator used for stats accounting is flushed here
   */
  def flushStatsAccumulators: Unit = {
    // accumulates an aggregator in the corresponding spark accumulator
    def accumulate[T : ClassTag](it: T, accum: Accumulator[_]) = {
      accum.asInstanceOf[Accumulator[T]] += it
    }
    logInfo (s"Embeddings processed: ${numEmbeddingsProcessed}")
    accumulate (numEmbeddingsProcessed,
      accums(ODAGMasterEngine.AGG_EMBEDDINGS_PROCESSED))
    logInfo (s"Embeddings generated: ${numEmbeddingsGenerated}")
    accumulate (numEmbeddingsGenerated,
      accums(ODAGMasterEngine.AGG_EMBEDDINGS_GENERATED))
    logInfo (s"Embeddings output: ${numEmbeddingsOutput}")
    accumulate (numEmbeddingsOutput,
      accums(ODAGMasterEngine.AGG_EMBEDDINGS_OUTPUT))
  }

  /**
   * Flushes a given aggregation.
   *
   * @param name name of the aggregation
   * @return iterator of aggregation storages
   * TODO: split aggregations before flush them and review the return type
   */
  def flushAggregationsByName(name: String) = {
    // the following function does the final local aggregation
    // e.g. for motifs, turns quick patterns into canonical ones
    def aggregate[K <: Writable, V <: Writable](agg1: AggregationStorage[K,V], agg2: AggregationStorage[_,_]) = {
      agg1.finalLocalAggregate (agg2.asInstanceOf[AggregationStorage[K,V]])
      agg1
    }
    val aggStorage = getAggregationStorage(name)
    val finalAggStorage = aggregate (
      aggregationStorageFactory.createAggregationStorage (name),
      aggStorage)
    Iterator(finalAggStorage)
  }

  /**
   * Returns the current value of an aggregation installed in this execution
   * engine.
   *
   * @param name name of the aggregation
   * @return the aggregated value or null if no aggregation was found
   */
  override def getAggregatedValue[A <: Writable](name: String): A =
    previousAggregationsBc.value.asInstanceOf[Map[String,A]].get(name) match {
      case Some(aggStorage) => aggStorage
      case None =>
        logWarning (s"Previous aggregation storage $name not found")
        null.asInstanceOf[A]
    }

  /**
   * Maps (key,value) to the respective local aggregator
   *
   * @param name identifies the aggregator
   * @param key key to account for
   * @param value value to be accounted for key in that aggregator
   * 
   */
  override def map[K <: Writable, V <: Writable](name: String, key: K, value: V) = {
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
      : AggregationStorage[K,V] = aggregationStorages.get(name) match {
    case Some(aggregationStorage : AggregationStorage[K,V] @unchecked) => aggregationStorage
    case None =>
      val aggregationStorage = aggregationStorageFactory.createAggregationStorage (name)
      aggregationStorages.update (name, aggregationStorage)
      aggregationStorage.asInstanceOf[AggregationStorage[K,V]]
    case Some(aggregationStorage) =>
      val e = new RuntimeException (s"Unexpected type for aggregation ${aggregationStorage}")
      logError (s"Wrong type of aggregation storage: ${e.getMessage}")
      throw e
  }
  
  override def aggregate(name: String, value: LongWritable) = accums.get (name) match {
    case Some(accum) =>
      accum.asInstanceOf[Accumulator[Long]] += value.get
    case None => 
      logWarning (s"Aggregator/Accumulator $name not found")
  }

  // output
  @transient val outputFunc = {
    import SparkConfiguration.{OUTPUT_PLAIN_TEXT, OUTPUT_SEQUENCE_FILE}
    configuration.getOutputFormat match {
      case OUTPUT_PLAIN_TEXT if configuration.isOutputActive => (e: Embedding) => {
        outputPlainText(e)
        numEmbeddingsOutput += 1
      }

      case OUTPUT_SEQUENCE_FILE if configuration.isOutputActive => (e: Embedding) => {
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
  private def outputSequenceFile(embedding: Embedding) = embeddingWriterOpt match {
    case Some(embeddingWriter) =>
      val resEmbedding = ResultEmbedding (embedding)
      embeddingWriter.append (NullWritable.get, resEmbedding)
      numEmbeddingsOutput += 1

    case None =>
      // we must decide at runtime the concrete Writable to be used
      val resEmbeddingClass = if (embedding.isInstanceOf[EdgeInducedEmbedding])
        classOf[EEmbedding]
      else if (embedding.isInstanceOf[VertexInducedEmbedding])
        classOf[VEmbedding]
      else
        classOf[ResultEmbedding[_]] // not allowed, will crash and should not happen

      // instantiate the embedding writer (sequence file)
      val superstepPath = new Path(outputPath, s"${getSuperstep}")
      val partitionPath = new Path(superstepPath, s"${partitionId}")
      val embeddingWriter = SequenceFile.createWriter(configuration.hadoopConf,
        SeqWriter.file(partitionPath),
        SeqWriter.keyClass(classOf[NullWritable]),
        SeqWriter.valueClass(resEmbeddingClass))

      embeddingWriterOpt = Some(embeddingWriter)
      
      val resEmbedding = ResultEmbedding (embedding)
      embeddingWriter.append (NullWritable.get, resEmbedding)
      numEmbeddingsOutput += 1
  }

  /**
   * Output embedding to a plain text
   */
  private def outputPlainText(embedding: Embedding) = outputStreamOpt match {
    case Some(outputStream) =>
      outputStream.write(embedding.toOutputString)
      outputStream.write("\n")

    case None =>
      logInfo (s"[partitionId=${getPartitionId}] Creating output stream")
      val fs = FileSystem.get(configuration.hadoopConf)
      val superstepPath = new Path(outputPath, s"${getSuperstep}")
      val partitionPath = new Path(superstepPath, s"${partitionId}")
      val outputStream = new OutputStreamWriter(fs.create(partitionPath))
      outputStreamOpt = Some(outputStream)
      outputStream.write(embedding.toOutputString)
      outputStream.write("\n")
  }
  
  // other functions
  override def getPartitionId() = partitionId

  override def getSuperstep() = superstep

}
