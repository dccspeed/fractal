package br.ufmg.cs.systems.fractal.apps

import java.util.UUID

import br.ufmg.cs.systems.fractal.util.Logging
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadFromHiveApp extends Logging {

  def main(args: Array[String]) {
    // args
    val configPath = args(0)
    val config = ujson.read(scala.reflect.io.File(configPath).slurp)

    var conf = new SparkConf()
    config("settings").arr.foreach(setting => {
      conf = conf.set(setting("name").str, setting("value").str)
    })

    val ss = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
    val hive = HiveWarehouseBuilder.session(ss).build()

    config("temporary_tables").arr.foreach(table => {
      logInfo(s"\tLoading temporary table ${table("name").str} with query ${table("value").str}")
      hive.execute(table("value").str).createOrReplaceTempView(table("name").str)
    })

    logInfo(s"\tLoading edges with query: ${config("query").str}")
    val edges = hive.execute(config("query").str)

    val outputPath = s"/dados01/ufmg.m06dcc/${UUID.randomUUID()}"
    logInfo(s"\tWriting data to CSV at: ${outputPath}")
    edges.write.csv(outputPath)

    hive.close()
    ss.stop()
  }
}
