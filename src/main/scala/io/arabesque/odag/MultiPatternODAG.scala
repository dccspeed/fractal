package io.arabesque.odag

import io.arabesque.embedding.Embedding
import io.arabesque.computation.Computation
import io.arabesque.odag.domain.StorageReader
import io.arabesque.odag.domain.{DomainStorage, DomainStorageReadOnly}
import io.arabesque.pattern.Pattern

import io.arabesque.conf.{Configuration, SparkConfiguration}

import java.io._

import scala.collection.JavaConversions._

class MultiPatternODAG extends BasicODAG {

  @transient var configuration: Configuration[_ <: Embedding] = _
  var patterns: Set[Pattern] = Set.empty
  @transient lazy val reusablePattern: Pattern = configuration.createPattern

  def this(numDomains: Int) = {
    this()
    storage = new DomainStorage(numDomains)
    serializeAsReadOnly = false
  }

  def this(readOnly: Boolean) = {
    this()
    serializeAsReadOnly = false
    storage = createDomainStorage (readOnly)
  }

  override def init(config: Configuration[_ <: Embedding]): MultiPatternODAG = {
    configuration = config
    this
  }

  def aggregationFilter(computation: Computation[_]): Boolean = {
    patterns = patterns.filter ( pattern =>
      computation.aggregationFilter (pattern)
      )
    !patterns.isEmpty
  }

  override def addEmbedding(embedding: Embedding): Unit = {
    reusablePattern.setEmbedding (embedding)
    addEmbedding (embedding, reusablePattern)
  }

  def addEmbedding(embedding: Embedding, pattern: Pattern): Unit = {
    if (!(patterns contains pattern))
      patterns += pattern.copy

    storage.addEmbedding (embedding)
  }

  override def getPattern: Pattern = ???
   
  override def aggregate(other: BasicODAG): Unit = other match {
    case mpOdag: MultiPatternODAG =>
      // do quick pattern union
      patterns = this.patterns union mpOdag.patterns
      // do domain aggregation (like single pattern)
      storage.aggregate(mpOdag.storage)
    case _ =>
      throw new RuntimeException (s"Must aggregate odags of the same type")
  }

  override def getReader(
      configuration: Configuration[Embedding],
      computation: Computation[Embedding],
      numPartitions: Int,
      numBlocks: Int,
      maxBlockSize: Int): StorageReader = {
    storage.getReader(patterns.toArray, configuration, computation,
      numPartitions, numBlocks, maxBlockSize)
  }

  override def readExternal(objInput: ObjectInput): Unit = {
    serializeAsReadOnly = objInput.readBoolean
    storage = createDomainStorage(serializeAsReadOnly)
    readFields (objInput)
  }

  override def writeExternal(objOuptut: ObjectOutput): Unit = {
    objOuptut.writeBoolean(serializeAsReadOnly)
    write (objOuptut)
  }
  
  override def readFields(dataInput: DataInput): Unit = {
    this.clear
    init(Configuration.get(dataInput.readInt))
    val numPatterns = dataInput.readInt
    for (i <- 0 until numPatterns) {
      val pattern = configuration.createPattern
      pattern.readFields (dataInput)
      patterns += pattern
    }
    storage.readFields(dataInput)
  }

  override def write(dataOutput: DataOutput): Unit = {
    dataOutput.writeInt (configuration.getId)
    dataOutput.writeInt (patterns.size)
    patterns.foreach (pattern => pattern.write(dataOutput))
    storage.write(dataOutput)
  }

}
