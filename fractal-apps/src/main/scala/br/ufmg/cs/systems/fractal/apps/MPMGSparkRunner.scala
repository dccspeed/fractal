package br.ufmg.cs.systems.fractal.mpmg

import java.util.UUID
import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.util.Logging

import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class HiveApp {

  /**
	Read from hive/database write in disk
   */

/*  def readWriteInput(configPath : String, output : String, ) {
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

    logInfo(s"\tWriting data to CSV at: ${outputPath}")
    edges.write.csv(outputPath)

    hive.close()
    ss.close()
  }*/
  /**
	Read from disk write in hive/database
   */
  def readWriteOutput(configPath : String, output : String) {

  }
}

class CliquesApp(
                    val fractalGraph : FractalGraph,
		    algs: FractalAlgorithms, 
                    commStrategy: String,
                    numPartitions: Int,
                    explorationSteps: Int) extends FractalSparkApp {
    def execute: Unit = {
    val cliquesRes = algs.cliques(fractalGraph, (explorationSteps + 1)).
      set("comm_strategy", commStrategy).
      set("num_partitions", numPartitions).
      explore(explorationSteps)

    val (accums, elapsed) = FractalSparkRunner.time {
      cliquesRes.compute()
    }

    logInfo(s"CliquesOptApp comm=${commStrategy}" +
		    s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
		    s" graph=${fractalGraph} " +
		    s" numValidSubgraphs=${cliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
	   )
    }
}

class ShortestPathsApp(
		val fractalGraph : FractalGraph,
		algs : FractalAlgorithms, 
		commStrategy: String,
		numPartitions: Int,
		explorationSteps: Int)  extends FractalSparkApp {
	def execute: Unit = {
    		fractalGraph.set("comm_strategy", commStrategy)
    		fractalGraph.set("num_partitions", numPartitions)

    	val (sps, elapsed) = FractalSparkRunner.time {
		algs.spaths(fractalGraph, explorationSteps)
	}
		logInfo(s"ShortestPathsApp comm=${commStrategy}" +
				s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
				s" graph=${fractalGraph} " +
				s" numValidSubgraphs=${sps.numValidSubgraphs()} elapsed=${elapsed}"
		       )
	}
}

object MPMGSparkRunner {
	def time[R](block: => R): (R, Long) = {
		val t0 = System.currentTimeMillis()
			val result = block // call-by-name
			val t1 = System.currentTimeMillis()
			(result, t1 - t0)
	}

	def main(args: Array[String]) {
		// args
		var i = 	0
		val graphClass = args(i) match {
				case "al" =>
					"br.ufmg.cs.systems.fractal.graph.BasicMainGraph"
				case "el" =>
					"br.ufmg.cs.systems.fractal.graph.EdgeListGraph"
				case other =>
					throw new RuntimeException(s"Input graph format '${other}' is invalid")
		}
		i += 1
			val graphPath = args(i)
			i += 1
			val algorithm = args(i)
			i += 1
			val commStrategy = args(i)
			i += 1
			val numPartitions = args(i).toInt
			i += 1
			val explorationSteps = args(i).toInt
			i += 1
			val logLevel = args(i)

			//Create Session
			val conf = new SparkConf()
			val sc = new SparkContext(conf) //just one 

			if (!sc.isLocal) {
				// TODO: this is ugly but have to make sure all spark executors are up by
				// the time we start executing fractal applications
				Thread.sleep(10000)
			}

		//query the input graph if is the case and write it in graphPath.

		//running fractal application
		val fc = new FractalContext(sc, logLevel)
		val fractalGraph = fc.textFile(graphPath, graphClass = graphClass)
		val algs = new FractalAlgorithms;

		val app = algorithm.toLowerCase match {
			case "cliques" =>
				new CliquesApp(fractalGraph, algs, commStrategy,
					numPartitions, explorationSteps)
			case "spaths" =>
				new ShortestPathsApp(fractalGraph, algs, commStrategy,
					numPartitions, explorationSteps)
			case appName =>
				throw new RuntimeException(s"Unknown app: ${appName}")
		}

		i += 1
			while (i < args.length) {
				println(s"Found config=${args(i)}")
					val kv = args(i).split(":")
					if (kv.length == 2) {
						fractalGraph.set(kv(0), kv(1))
					}
				i += 1
			}

		app.execute

		fc.stop()
		sc.stop()
	}
}


