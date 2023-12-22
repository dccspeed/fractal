package br.ufmg.cs.systems.fractal.pattern

import br.ufmg.cs.systems.fractal.FractalContext
import br.ufmg.cs.systems.fractal.util.{Logging, ReflectionSerializationUtils}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.util.Base64
import scala.jdk.CollectionConverters._
object PatternUtilsRDD extends Logging {

   def singleVertexRDD(sc: SparkContext, vertexLabel: Int): RDD[Pattern] = {
      val pattern = PatternUtils.singleVertexPattern(vertexLabel)
      sc.parallelize(Seq(pattern), 3 * sc.defaultParallelism)
   }

   def singleEdgeRDD(sc: SparkContext, vertexLabel: Int): RDD[Pattern] = {
      val pattern = PatternUtils.singleEdgePattern()
      sc.parallelize(Seq(pattern), 3 * sc.defaultParallelism)
   }

   def extendByVertexRDD(patterns: RDD[Pattern], vertexLabel: Int)
   : RDD[Pattern] = {
      val sc = patterns.sparkContext
      patterns
         .flatMap(p => {
            PatternUtils.extendByVertex(p, vertexLabel).asScala
         })
         .distinct(3 * sc.defaultParallelism)
   }

   def extendByEdgeInternalRDD(patterns: RDD[Pattern], edgeLabel: Int)
   : RDD[Pattern] = {
      val sc = patterns.sparkContext
      patterns
         .flatMap(p =>{
            PatternUtils.extendByEdgeInternal(p, edgeLabel).asScala
         })
         .distinct(3 * sc.defaultParallelism)
   }

   def extendByEdgeExternalRDD(patterns: RDD[Pattern], vertexLabel: Int,
                               edgeLabel: Int): RDD[Pattern] = {
      val sc = patterns.sparkContext
      patterns
         .flatMap(p =>{
            PatternUtils.extendByEdgeExternal(p, vertexLabel, edgeLabel).asScala
         })
         .distinct(3 * sc.defaultParallelism)
   }

   def extendByEdgeRDD(patterns: RDD[Pattern], vertexLabel: Int)
   : RDD[Pattern] = {
      val sc = patterns.sparkContext
      patterns
         .flatMap(p =>{
            PatternUtils.extendByEdge(p, vertexLabel).asScala
         })
         .distinct(3 * sc.defaultParallelism)
   }

   def extendByEdgeRDD(patterns: RDD[Pattern], vertexLabel: Int, edgeLabel: Int)
   : RDD[Pattern] = {
      val sc = patterns.sparkContext
      patterns
         .flatMap(p =>{
            PatternUtils.extendByEdge(p, vertexLabel, edgeLabel).asScala
         })
         .distinct(3 * sc.defaultParallelism)
   }

   def getOrGenerateVertexPatternsRDD(sc: SparkContext, numVertices: Int)
   : RDD[Pattern] = {
      val path = s"${System.getenv("FRACTAL_HOME")}/data/pattern-sets" +
         s"/vpatterns-${numVertices}"
      getOrGenerateVertexPatternsRDD(sc, numVertices, path)
   }

   def getOrGenerateVertexPatternsRDD(sc: SparkContext, numVertices: Int,
                                      path: String): RDD[Pattern] = {
      val fs = org.apache.hadoop.fs.FileSystem.get(
         new org.apache.hadoop.conf.Configuration())
      val hpath = new org.apache.hadoop.fs.Path(path)
      if (fs.exists(hpath)) {
         logInfo(s"FoundVertexPatterns" +
            s" numVertices=${numVertices} path=${path}")
         readPatternsRDD(sc, path)
      } else {
            logInfo(s"NotFoundVertexPatterns" +
               s" numVertices=${numVertices} path=${path}")
            val patternsRDD = vertexPatternsRDD(sc, numVertices)
            writePatternsRDD(patternsRDD, path)
            patternsRDD
      }
   }

   def vertexPatternsRDD(sc: SparkContext, numVertices: Int): RDD[Pattern] = {
      if (numVertices <= 0) return sc.emptyRDD

      var patterns = singleVertexRDD(sc, 1)
      var remainingVertices = numVertices - 1
      while (remainingVertices > 0) {
         patterns = extendByVertexRDD(patterns, 1)
         remainingVertices -= 1
      }

      patterns
   }

   def edgePatternsRDD(sc: SparkContext, numEdges: Int): RDD[Pattern] = {
      if (numEdges <= 0) return sc.emptyRDD

      var patterns = singleEdgeRDD(sc, 1)
      var remainingEdges = numEdges - 1
      while (remainingEdges > 0) {
         patterns = extendByEdgeRDD(patterns, 1)
         remainingEdges -= 1
      }

      patterns
   }

   def writePatternsRDD(patternsRDD: RDD[Pattern], path: String): Unit = {
      val base64EncodedPatternsRDD = patternsRDD
         .mapPartitions(patternsIter => {
            val encoder = Base64.getEncoder
            patternsIter.map(p => {
               val bytes = ReflectionSerializationUtils.serialize(p)
               encoder.encodeToString(bytes)
            })
         })

      base64EncodedPatternsRDD.saveAsTextFile(path, classOf[BZip2Codec])
   }

   def readPatternsRDD(sc: SparkContext, path: String): RDD[Pattern] = {
      val patternsRDD = sc.textFile(s"${path}/*.bz2")
         .mapPartitions(stringIter => {
            val decoder = Base64.getDecoder
            stringIter.map(str => {
               val bytes = decoder.decode(str)
               ReflectionSerializationUtils.deserialize[Pattern](bytes)
            })
         })

      patternsRDD
   }

   def localIterator(patternsRDD: RDD[Pattern]): PatternIteratorRDD = {
      new PatternIteratorRDD(patternsRDD)
   }
}

class PatternIteratorRDD(patternsRDD: RDD[Pattern])
   extends Iterator[Pattern] {

   // initial caching for local iterator
   patternsRDD.cache()
   val numPatterns = patternsRDD.count()
   private val localIterator = patternsRDD.toLocalIterator

   override def hasNext: Boolean = {
      val iterHasNext = localIterator.hasNext
      if (!iterHasNext) patternsRDD.unpersist()
      iterHasNext
   }

   override def next(): Pattern = localIterator.next()
}
