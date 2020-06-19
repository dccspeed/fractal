package br.ufmg.cs.systems.fractal

import java.util.UUID

import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object FractalSparkRunnerTrilha1 {

  def main(args: Array[String]) {
    // args
    var i = 0
    val configPath = args(i)
    val config = ujson.read(scala.reflect.io.File(configPath).slurp)

    val conf = new SparkConf()
      .setAppName(config("app").str)
      .setMaster(config("master").str)
      .set("spark.sql.hive.hiveserver2.jdbc.url", config("url").str)
      .set("spark.datasource.hive.warehouse.exec.results.max", Int.MaxValue.toString)

    val ss = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
    val hive = HiveWarehouseBuilder.session(ss).build()

    val edges = hive.execute(config("query").str)

    val uuid = UUID.randomUUID()

    edges.write.csv(s"/dados01/ufmg.m06dcc/${uuid}")

    hive.close()
    ss.stop()
  }

}




