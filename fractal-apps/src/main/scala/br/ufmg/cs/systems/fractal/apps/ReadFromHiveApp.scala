package br.ufmg.cs.systems.fractal.apps

import java.util.UUID

import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadFromHiveApp {

  def main(args: Array[String]) {
    // args
    val configPath = args(0)
    val config = ujson.read(scala.reflect.io.File(configPath).slurp)

    val conf = new SparkConf()
      .setAppName(config("app").str)
      .setMaster(config("master").str)
      .set("spark.sql.hive.hiveserver2.jdbc.url", config("url").str)
      .set("spark.datasource.hive.warehouse.exec.results.max", Int.MaxValue.toString)

    val ss = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
    val hive = HiveWarehouseBuilder.session(ss).build()

    hive.execute(config("query").str)
      .write
      .csv(s"/dados01/ufmg.m06dcc/${UUID.randomUUID()}")

    hive.close()
    ss.stop()
  }

}




