package br.ufmg.cs.systems.fractal.pattern

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

object PatternUtilsRDD {

   def singleVertexRDD(sc: SparkContext, vertexLabel: Int): RDD[Pattern] = {
      val pattern = PatternUtils.singleVertexPattern(vertexLabel)
      sc.parallelize(Array(pattern), 3 * sc.defaultParallelism)
   }

   def extendByVertexRDD(patterns: RDD[Pattern], vertexLabel: Int)
   : RDD[Pattern] = {
      val sc = patterns.sparkContext
      patterns
         .flatMap(p =>{
            PatternUtils.extendByVertex(p, vertexLabel).asScala
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
}
