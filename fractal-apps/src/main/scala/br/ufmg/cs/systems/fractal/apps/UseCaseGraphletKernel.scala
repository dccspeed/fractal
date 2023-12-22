package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Use case: simple example showing how to use fractal for graph
 * classification task using graphlet kernels and SVM.
 * Most of this code is data cleaning and machine learning routines.
 * Fractal's part is to get motif counting per input graph.
 */
object UseCaseGraphletKernel extends Logging {
   private val graphDatabasePath = "data/mutagenicity.sc"
   private val graphIndexesMapPath = "data/mutagenicity.idx"
   private val graphLabelsPath = "data/mutagenicity.labels"

   private def getVertexToGraphMapping(sc: SparkContext)
   : Broadcast[Array[Int]] = {
      val vertexToGraphIdx = sc.textFile(graphIndexesMapPath)
         .map(_.toInt).collect()
      sc.broadcast(vertexToGraphIdx)
   }

   private def getGraphIdxToGraphLabel(sc: SparkContext)
   : Broadcast[Array[Int]] = {
      val graphIdxToGraphLabel = sc.textFile(graphLabelsPath)
         .map(_.toInt).collect()
      sc.broadcast(graphIdxToGraphLabel)
   }

   private def getUniquePatternsToIndex
   (motifsCountingPerGraph: RDD[((Pattern,Int), Long)])
   : Broadcast[Map[Pattern,Int]] = {
      val sc = motifsCountingPerGraph.sparkContext
      // mapping unique patterns found to index for labeled points
      val patternToIdx = motifsCountingPerGraph.map(_._1._1)
         .distinct()
         .collect()
         .zipWithIndex
         .toMap

      sc.broadcast(patternToIdx)
   }

   private def getGraphIdxToNumSubgraphs
   (motifsCountingPerGraph: RDD[((Pattern,Int),Long)])
   : Broadcast[Map[Int,Long]] = {
      val sc = motifsCountingPerGraph.sparkContext
      val graphIdxToNumSubgraphs = motifsCountingPerGraph
         .map(kv => (kv._1._2, kv._2))
         .reduceByKey(_ + _)
         .collect()
         .toMap

      sc.broadcast(graphIdxToNumSubgraphs)
   }

   private def getMotifsCountingPerGraph
   (fc: FractalContext, vertexToGraphIdxBc: Broadcast[Array[Int]])
   : RDD[((Pattern,Int), Long)] = {

      // create fractal graph
      val fgraph = fc.textFile(graphDatabasePath)
         .set("edge_labeled", false)
         .set("vertex_labeled", true)

      // setup fractoid representing 5-vertex induced subgraphs
      val motifs = fgraph.vfractoid.extend(5)

      // setup aggreation key for subgraphs in Fractal
      val subgraphToPatternGraphIdx: VertexInducedSubgraph => (Pattern, Int) =
         subgraph => {
            val pattern = subgraph.quickPattern()
            val graphIdx = vertexToGraphIdxBc.value(subgraph.getVertices.get(0))
            (pattern, graphIdx) // not only pattern, but also the graphIdx
         }

      // fractal aggregation per (pattern, graphIdx)
      val motifsCountingPerGraph = motifs
         // aggregate quick patterns using function above as key function
         .aggregationObjLong (subgraphToPatternGraphIdx,0L, _ => 1L, _ + _)
         // turn patterns in tuples canonical for final aggregation
         .map(quickPatternGraphIdxToCount => {
            quickPatternGraphIdxToCount._1._1.turnCanonical()
            quickPatternGraphIdxToCount
         })
         // final aggregation (sum)
         .reduceByKey(_ + _)
         // we are going to use this collection more than one time, so we
         // better keep this collection in-memory
         .cache()

      motifsCountingPerGraph
   }

   private def getLabeledPoints
   (motifsCountingPerGraph: RDD[((Pattern,Int), Long)],
    patternToIdxBc: Broadcast[Map[Pattern,Int]],
    graphIdxToNumSubgraphsBc: Broadcast[Map[Int,Long]],
    graphIdxToGraphLabelBc: Broadcast[Array[Int]]): RDD[LabeledPoint]= {
      val numDistinctPatterns = patternToIdxBc.value.size
      // get feature vector for each graph
      val graphIdxVector = motifsCountingPerGraph
         // collection of graphs and their patternIdxs plus frequencies
         .map(kv => {
            val ((pattern,graphIdx), count)= kv
            val patternIdx = patternToIdxBc.value(pattern)
            val totalCount = graphIdxToNumSubgraphsBc.value(graphIdx)
            val frequency =  count / totalCount.toDouble
            (graphIdx, (patternIdx, frequency))
         })
         // group pattern/frequencies of the same graph in the same record
         .groupByKey()
         // create feature vector (array of double) from the patterns found
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

      // label vectors -> labeled points
      val labeledPoints = graphIdxVector.map(kv => {
         val (graphIdx, vector) = kv
         LabeledPoint(graphIdxToGraphLabelBc.value(graphIdx - 1), vector)
      })

      labeledPoints
   }

   private def mlTask(data: RDD[LabeledPoint]): Unit = {
      // split data into training (60%) and test (40%).
      val splits = data.randomSplit(Array(0.6, 0.4))
      val training = splits(0).cache()
      val test = splits(1)

      // run training algorithm to build the model
      val numIterations = 100
      val model = SVMWithSGD.train(training, numIterations)

      // clear the default threshold.
      model.clearThreshold()

      // compute raw scores on the test set.
      val scoreAndLabels = test.map { point =>
         val score = model.predict(point.features)
         (score, point.label)
      }

      // get evaluation metrics.
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      val auROC = metrics.areaUnderROC()

      logApp(s"Area under ROC = ${auROC}")
   }

   def main(args: Array[String]): Unit = {
      // environment setup
      val conf = new SparkConf().setAppName(this.getClass.getCanonicalName)
      val sc = new SparkContext(conf)
      val fc = new FractalContext(sc)

      // mapping vertex -> graphIdx
      val vertexToGraphIdxBc = getVertexToGraphMapping(sc)
      // mapping graphIdx -> label
      val graphIdxToGraphLabelBc = getGraphIdxToGraphLabel(sc)

      // fractal aggregation per (pattern, graphIdx) -> count
      val motifsCountingPerGraph = getMotifsCountingPerGraph(fc, vertexToGraphIdxBc)

      // mapping unique patterns found to index for labeled points
      val patternToIdxBc = getUniquePatternsToIndex(motifsCountingPerGraph)

      // mapping graphs to their total number of subgraphs
      val graphIdxToNumSubgraphsBc = getGraphIdxToNumSubgraphs(motifsCountingPerGraph)

      // get labeled points
      val labeledPoints = getLabeledPoints(motifsCountingPerGraph,
         patternToIdxBc, graphIdxToNumSubgraphsBc, graphIdxToGraphLabelBc)

      // run machine learning task over labeled points
      mlTask(labeledPoints)

      // environment cleaning
      fc.stop()
      sc.stop()
   }
}
