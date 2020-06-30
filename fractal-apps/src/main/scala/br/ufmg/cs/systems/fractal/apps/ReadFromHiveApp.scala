package br.ufmg.cs.systems.fractal.apps

import java.util.UUID

import br.ufmg.cs.systems.fractal.mpmg.{HiveApp, UtilApp}
import br.ufmg.cs.systems.fractal.util.Logging

object ReadFromHiveApp extends Logging {

  def main(args: Array[String]) {
    val configPath = args(0)
    val utilApp = new UtilApp
    val config = utilApp.readConfig(configPath)
    val ss = utilApp.getCreateSparkSession(config, null, "spark_fractal")

    val hiveApp = new HiveApp(configPath)
    val hive = hiveApp.hiveSession

    hiveApp.databaseConfigs("temporary_tables").arr.foreach(table => {
      logInfo(s"\tLoading temporary table ${table("name").str} with query ${table("value").str}")
      hive.execute(table("value").str).createOrReplaceTempView(table("name").str)
    })

    logInfo(s"\tLoading edges with query: ${hiveApp.databaseConfigs("query").str}")
    val edges = hive.execute(hiveApp.databaseConfigs("query").str)

    val outputPath = s"/dados01/ufmg.m06dcc/${UUID.randomUUID()}"
    logInfo(s"\tWriting data to CSV at: ${outputPath}")
    edges.write.csv(outputPath)

    ss.stop()
  }
}
