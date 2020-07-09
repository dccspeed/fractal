package br.ufmg.cs.systems.fractal

import org.junit.Assert.{assertArrayEquals, assertEquals}
import org.junit.Test

import scala.io.Source

object MotifsTest {
   import FractalApplicationsTest._

   /**
    * Ground truth for testing motifs application
    */
   private val motifsGt: Map[(String,Int),Array[Long]] = {
      var motifsGt = Map.empty[(String,Int),Array[Long]]
      val in = Source.fromFile("../data/motifs-test.gt")
      for (line <- in.getLines) {
         val toks = line.trim.split(" ")
         val (graph, numVertices, numMotifs) = (toks(0), toks(1), toks(2))
         val counts = new Array[Long](numMotifs.toInt)
         for (i <- 0 until numMotifs.toInt) counts(i) = toks(i+3).toLong
         java.util.Arrays.sort(counts)
         motifsGt = motifsGt +
            ((graph,numVertices.toInt) -> counts)
      }
      in.close()
      motifsGt
   }

   def test(): Unit = {
      println(s"Starting MotifsTest")
      for (((graph,numVertices),counts) <- motifsGt) {
         val chosenGraph = if (graph == "citeseer-single-label") {
            citeseerSingleLabelGraph
         } else if (graph == "citeseer") {
            citeseerGraph
         } else {
            throw new RuntimeException(s"Invalid graph string ${graph}")
         }

         chosenGraph.set("num_partitions", numPartitions)

         val motifsMap1 = chosenGraph.motifsSF(numVertices).collectAsMap()
         val counts1 = motifsMap1.values.toArray
         java.util.Arrays.sort(counts1)
         assertArrayEquals(counts, counts1)

         val motifsMap2 = chosenGraph.motifsPF(numVertices).collectAsMap()
         assertEquals(motifsMap2, motifsMap1)

         val motifsMap3 = chosenGraph.motifsPFMCVC(numVertices).collectAsMap()
         assertEquals(motifsMap3, motifsMap1)

         synchronized {
            notify()
         }
      }
      println(s"Finishing MotifsTest")
   }
}
