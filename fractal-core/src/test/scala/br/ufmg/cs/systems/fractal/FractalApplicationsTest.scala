package br.ufmg.cs.systems.fractal

import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit.{After, AfterClass, Test}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source

object FractalApplicationsTest {
   val numPartitions: Int = 8
   val appName: String = "fractal-test"
   val logLevel: String = "info"

   val master: String = s"local[${numPartitions}]"

   val sc: SparkContext = {
      val conf = new SparkConf()
         .setMaster(master)
         .setAppName(appName)

      val sc = new SparkContext(conf)
      sc.setLogLevel(logLevel)
      sc
   }

   val fc: FractalContext = new FractalContext(sc, logLevel)

   val fgraph: FractalGraph = fc.textFile ("../data/cube.graph")

   val fgraphEdgeLabel: FractalGraph =
      fc.textFile ("../data/cube-edge-label.graph")

   val citeseerSingleLabelGraph: FractalGraph =
      fc.textFile("../data/citeseer-single-label.sc",
         graphClass = "br.ufmg.cs.systems.fractal.graph.SuccinctMainGraph")

   val citeseerGraph: FractalGraph =
      fc.textFile("../data/citeseer.sc",
         graphClass = "br.ufmg.cs.systems.fractal.graph.SuccinctMainGraph")

   @AfterClass def stopResources(): Unit = {
      println(s"Cleaning resources")
      if (sc != null) {
         sc.stop()
         fc.stop()
      }
   }
}

class FractalApplicationsTest extends Logging {
   implicit val executionContext = scala.concurrent.ExecutionContext.global

   @Test def tests: Unit = {
      // TODO: switch to junit tests
      //val f1 = Future(MotifsTest.test())
      //val f2 = Future(FSMTest.test())

      //Await.ready(f1, Duration.Inf)
      //Await.ready(f2, Duration.Inf)
   }
}
