package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

// TODO: refactor
object UseCaseGraphletKernel extends Logging {
   private val graphDatabasePath = "data/mutagenicity.sc"
   private val graphIndexesMapPath = "data/mutagenicity.idx"
   private val graphLabelsPath = "data/mutagenicity.labels"

   def main(args: Array[String]): Unit = {
      // environment setup
      val conf = new SparkConf().setAppName(this.getClass.getCanonicalName)
      val sc = new SparkContext(conf)
      val fc = new FractalContext(sc)
      val fgraph = fc.textFile(graphDatabasePath)
         .set("edge_labeled", false)
         .set("vertex_labeled", true)

      // read graph idx
      val vertexToGraphIdx = sc.textFile(graphIndexesMapPath)
         .map(_.toInt).collect()
      val vertexToGraphIdxBc = sc.broadcast(vertexToGraphIdx)

      // read graph labels
      val graphIdxToGraphLabel = sc.textFile(graphLabelsPath)
            .map(_.toInt).collect()
      val graphIdxToGraphLabelBc = sc.broadcast(graphIdxToGraphLabel)

      logApp(s"Number of graphs = ${vertexToGraphIdx.length}")

      // mapping subgraphs to key/value for aggregation
      val subgraphToPatternGraphIdx: VertexInducedSubgraph => (Pattern, Int) =
         subgraph => {
            val pattern = subgraph.quickPattern()
            val graphIdx = vertexToGraphIdxBc.value(subgraph.getVertices.get(0))
            (pattern, graphIdx)
         }

      // fractoid representing 5-vertex induced subgraphs
      val motifs = fgraph.vfractoid.expand(5)

      val motifsCountingPerGraph = motifs
         .aggregationObjLong (subgraphToPatternGraphIdx,0L, _ => 1L, _ + _)
         .map(quickPatternGraphIdxToCount => {
            quickPatternGraphIdxToCount._1._1.turnCanonical()
            quickPatternGraphIdxToCount
         })
         .reduceByKey(_ + _)
         .cache()

      val patternToIdx = motifsCountingPerGraph.map(_._1._1)
         .distinct()
         .collect()
         .zipWithIndex
         .toMap

      val numDistinctPatterns = patternToIdx.size
      logApp(s"Number of distinct patterns = ${numDistinctPatterns}")

      val patternToIdxBc = sc.broadcast(patternToIdx)

      val graphIdxToNumSubgraphs = motifsCountingPerGraph
         .map(kv => (kv._1._2, kv._2))
         .reduceByKey(_ + _)
         .collectAsMap()

      val graphIdxToNumSubgraphsBc = sc.broadcast(graphIdxToNumSubgraphs)

      val graphIdxVector = motifsCountingPerGraph
         .map(kv => {
            val ((pattern,graphIdx), count)= kv
            val patternIdx = patternToIdxBc.value(pattern)
            val totalCount = graphIdxToNumSubgraphsBc.value(graphIdx)
            val frequency =  count / totalCount.toDouble
            (graphIdx, (patternIdx, frequency))
         })
         .groupByKey()
         .map(kv => {
            val (graphIdx, patternFrequency) = kv
            val sortedPatternFrequency = patternFrequency.toArray.sortBy(_._1)
            val idxs = new Array[Int](sortedPatternFrequency.size)
            val values = new Array[Double](sortedPatternFrequency.size)
            var i = 0
            val iter = sortedPatternFrequency.iterator
            while (iter.hasNext) {
               val (pattern, frequency) = iter.next()
               idxs(i) = pattern
               values(i) = frequency
               i += 1
            }
            val vec = Vectors.sparse(numDistinctPatterns, idxs, values)
            (graphIdx, vec)
         })

      val data = graphIdxVector.map(kv => {
         val (graphIdx, vector) = kv
         LabeledPoint(graphIdxToGraphLabelBc.value(graphIdx - 1), vector)
      })

      // Split data into training (60%) and test (40%).
      val splits = data.randomSplit(Array(0.6, 0.4))
      val training = splits(0).cache()
      val test = splits(1)

      // Run training algorithm to build the model
      val numIterations = 100
      val model = SVMWithSGD.train(training, numIterations)

      // Clear the default threshold.
      model.clearThreshold()

      // Compute raw scores on the test set.
      val scoreAndLabels = test.map { point =>
         val score = model.predict(point.features)
         (score, point.label)
      }

      // Get evaluation metrics.
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      val auROC = metrics.areaUnderROC()

      logApp(s"Area under ROC = ${auROC}")

      // environment cleaning
      fc.stop()
      sc.stop()
   }
}
