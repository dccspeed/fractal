package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.hadoop.io.LongWritable
import org.apache.spark.{SparkConf, SparkContext}

object MyMotifsApp extends Logging {
  def main(args: Array[String]): Unit = {
    // environment setup
    val conf = new SparkConf().setAppName("MyMotifsApp")
    val sc = new SparkContext(conf)
    val fc = new FractalContext(sc)
    val graphPath = args(0) // input graph
    val fgraph = fc.textFile (graphPath)

    val motifs = fgraph.vfractoid.expand(3)

    val motifsCountsRDD = motifs.aggregationCanonicalPatternLong(
      s => s.quickPattern(), 0L, _ => 1L, _ + _
    )

    motifsCountsRDD.cache()
    val iter = motifsCountsRDD.toLocalIterator
    while (iter.hasNext) {
      val (motif, count) = iter.next()
      logInfo(s"motif=${motif} count=${count}")
    }

    motifsCountsRDD.unpersist()
    
    // environment cleaning
    fc.stop()
    sc.stop()
  }
}
