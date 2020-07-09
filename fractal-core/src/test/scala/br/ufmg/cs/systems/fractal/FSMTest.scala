package br.ufmg.cs.systems.fractal

import org.junit.Assert.{assertArrayEquals, assertEquals}

import scala.io.Source

object FSMTest {
   import FractalApplicationsTest._

   /**
    * Ground-truth for testing the FSM application
    */
   private val fsmGt: Map[(String,Int),Long] = {
      var fsmGt = Map.empty[(String,Int),Long]
      val in = Source.fromFile("../data/fsm-test.gt")
      for (line <- in.getLines) {
         val toks = line.trim.split(" ")
         val (graph, minSupport, numPatterns) = (toks(0), toks(1), toks(2))
         fsmGt = fsmGt + ((graph,minSupport.toInt) -> numPatterns.toLong)
      }
      in.close()
      fsmGt
   }

   def test(): Unit = {
      println(s"Starting FSMTest")
      for (((graph,minSupport), numPatterns) <- fsmGt) {
         val chosenGraph = if (graph == "citeseer-single-label") {
            citeseerSingleLabelGraph
         } else if (graph == "citeseer") {
            citeseerGraph
         } else {
            throw new RuntimeException(s"Invalid graph string ${graph}")
         }

         chosenGraph.set("num_partitions", numPartitions)

         // subgraph-first approach: edge by edge
         val frequentPatterns = chosenGraph
            .fsmSF(minSupport, Int.MaxValue)
            .collectAsMap()

         assertEquals(frequentPatterns.size, numPatterns)

         // pattern-first approach: pattern-matching on every possible pattern
         val frequentPatternsPf = chosenGraph
            .fsmPF(minSupport, Int.MaxValue)
            .collectAsMap()

         assertEquals(frequentPatternsPf.size, numPatterns)

         assertEquals(frequentPatterns, frequentPatternsPf)

         // pattern-first approach using MCVC optimization
         val frequentPatternsPf2 = chosenGraph
            .fsmPFMCVC(minSupport, Int.MaxValue)
            .collectAsMap()

         assertEquals(frequentPatternsPf2.size, numPatterns)

         assertEquals(frequentPatterns, frequentPatternsPf2)
      }
      println(s"Finishing FSMTest")
   }
}
