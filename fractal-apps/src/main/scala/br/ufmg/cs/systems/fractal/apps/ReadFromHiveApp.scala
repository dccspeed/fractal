package br.ufmg.cs.systems.fractal.apps

import java.util.UUID

import br.ufmg.cs.systems.fractal.mpmg.HiveApp
import br.ufmg.cs.systems.fractal.util.Logging
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder

object ReadFromHiveApp extends Logging {

  def main(args: Array[String]) {
    // args
    val hiveApp = new HiveApp(args(0))

    val ss = hiveApp.sparkSession
    val hive = HiveWarehouseBuilder.session(ss).build()

    hiveApp.databaseConfigs("temporary_tables").arr.foreach(table => {
      logInfo(s"\tLoading temporary table ${table("name").str} with query ${table("value").str}")
      hive.execute(table("value").str).createOrReplaceTempView(table("name").str)
    })

    logInfo(s"\tLoading edges with query: ${hiveApp.databaseConfigs("query").str}")
    val edges = hive.execute(hiveApp.databaseConfigs("query").str)

    val outputPath = s"/dados01/ufmg.m06dcc/${UUID.randomUUID()}"
    logInfo(s"\tWriting data to CSV at: ${outputPath}")
    edges.write.csv(outputPath)

    hive.close()
    ss.stop()
  }
}
