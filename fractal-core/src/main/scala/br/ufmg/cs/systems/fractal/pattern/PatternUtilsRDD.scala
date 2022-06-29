package br.ufmg.cs.systems.fractal.pattern

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

object PatternUtilsRDD {

   def singleVertexRDD(sc: SparkContext, vertexLabel: Int): RDD[Pattern] = {
      val pattern = PatternUtils.singleVertexPattern(vertexLabel)
      sc.parallelize(Array(pattern), 3 * sc.defaultParallelism)
   }

   def singleEdgeRDD(sc: SparkContext, vertexLabel: Int): RDD[Pattern] = {
      val pattern = PatternUtils.singleEdgePattern()
      sc.parallelize(Array(pattern), 3 * sc.defaultParallelism)
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
   //private val localIterator = patternsRDD.collect().iterator

   override def hasNext: Boolean = {
      val iterHasNext = localIterator.hasNext
      if (!iterHasNext) patternsRDD.unpersist()
      iterHasNext
   }

   override def next(): Pattern = localIterator.next()
}
