package br.ufmg.cs.systems.fractal.apps

import java.io.{BufferedWriter, File, FileWriter}

import br.ufmg.cs.systems.fractal.FractalContext
import br.ufmg.cs.systems.fractal.util.Logging
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadWriteToCSVApp extends Logging {
  def time[R](block: => R): (R, Long) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    (result, t1 - t0)
  }

  def main(args: Array[String]) {
    val config = ujson.read(scala.reflect.io.File(args(0)).slurp)
    val steps = args(1).toInt

    val conf = new SparkConf()
      .setAppName(config("app").str)
      .setMaster(config("master").str)
      .set(config("driver").str, config("url").str)

    val ss = SparkSession.builder.config(conf).getOrCreate()

    if (!ss.sparkContext.isLocal) {
      // TODO: this is ugly but have to make sure all spark executors are up by
      //  the time we start executing fractal applications
      Thread.sleep(10000)
    }

    val fc = new FractalContext(ss.sparkContext)

    val hive = HiveWarehouseBuilder.session(ss).build()

    val edges = hive.execute(config("query").str)

    val graphPath = s"${fc.tmpPath}/graph.edges"
    var outputBuffer: BufferedWriter = null

    val file = new File(graphPath)
    new File(fc.tmpPath).mkdirs()

    outputBuffer = new BufferedWriter(new FileWriter(file))

    edges.collect.foreach(edge => {
      outputBuffer.write(s"${edge.get(0)} ${edge.get(1)}\n")
    })

    outputBuffer.close()

    val fgraph = fc.textFile(graphPath, "br.ufmg.cs.systems.fractal.graph.EdgeListGraph")

    val outputPath = s"${sys.env("HOME")}/fractal-outputs/${fc.tmpPath.split('/')(2)}.csv"
    outputBuffer = new BufferedWriter(new FileWriter(new File(outputPath)))

    //      TODO: without the collect, a not serializable
    //       error explodes due to ArrayBuffer mappedWords
    fgraph.cliques.explore(steps).mappedSubgraphs.collect.foreach(subgraph => {
      outputBuffer.write(s"${subgraph.mappedWords.mkString(",")}\n")
    })

    outputBuffer.close()
    hive.close()
    fc.stop()
    ss.stop()
  }

}
