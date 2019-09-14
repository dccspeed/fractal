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

    val AGG_MOTIFS = "motifs"
    val motifs = fgraph.vfractoid.
      expand(1).
      aggregate [Pattern,LongWritable] (
        AGG_MOTIFS,
        (e,c,k) => { e.getPattern },
        (e,c,v) => { v.set(1); v },
        (v1,v2) => { v1.set(v1.get() + v2.get()); v1 }).
      explore(2)

    val motifsMap = motifs.aggregationMap[Pattern,LongWritable](AGG_MOTIFS)
    for ((motif,count) <- motifsMap) {
      logInfo(s"motif=${motif} count=${count}")
    }
    
    // environment cleaning
    fc.stop()
    sc.stop()
  }
}
