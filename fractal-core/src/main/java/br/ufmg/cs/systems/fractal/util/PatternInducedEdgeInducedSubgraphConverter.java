package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.computation.SparkFromScratchEngine;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.subgraph.EdgeInducedSubgraph;
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;

/**
 * This is a converter between a canonical code for pattern induced subgraphs
 * into a canonical code for edge-induced subgraphs.
 */
public class PatternInducedEdgeInducedSubgraphConverter
        implements SubgraphConverter<PatternInducedSubgraph, EdgeInducedSubgraph> {
   private SparkFromScratchEngine<EdgeInducedSubgraph> nextEngine;
   private Computation<EdgeInducedSubgraph> nextComputation;
   private EdgeInducedSubgraph nextSubgraph;
   private MainGraph graph;

   @Override
   public void apply(PatternInducedSubgraph subgraph,
                     Computation<PatternInducedSubgraph> computation) {
      convert(subgraph, computation, nextSubgraph, nextComputation);
      nextEngine.initialWorkCompute();
   }

   @Override
   public void init(Computation<PatternInducedSubgraph> computation) {
      nextEngine = (SparkFromScratchEngine<EdgeInducedSubgraph>)
              computation.getExecutionEngine().getNextEngine();
      nextComputation = nextEngine.computation();
      nextSubgraph = nextComputation.getSubgraphEnumerator().getSubgraph();
      graph = computation.getConfig().getMainGraph();
   }

   @Override
   public void convert(PatternInducedSubgraph psubgraph,
                       Computation<PatternInducedSubgraph> pcomputation,
                       EdgeInducedSubgraph esubgraph,
                       Computation<EdgeInducedSubgraph> ecomputation) {

      psubgraph.getEdges(pcomputation.getPattern()).clear();
      IntArrayList edges = psubgraph.getEdges(pcomputation.getPattern());
      int numEdges = edges.size();

      esubgraph.reset();

      for (int i = 0; i < numEdges; ++i) {
         int j = i;
         int chosenJ = -1;

         if (i == 0) {
            chosenJ = i;
            for (; j < numEdges; ++j) {
               if (edges.getu(j) < edges.getu(chosenJ)) chosenJ = j;
            }
         } else {
            for (; j < numEdges; ++j) {
               int w = 0;
               boolean validJ = false;
               for (; w < i; ++w) {
                  if (areEdgesNeighbors(edges.getu(w), edges.getu(j))) {
                     validJ = true;
                     w = i;
                  }
               }

               if (validJ && (chosenJ == -1 || edges.getu(j) < edges.getu(chosenJ))) {
                  chosenJ = j;
               }
            }
         }

         if (i != chosenJ) {
            int aux = edges.getu(i);
            edges.setu(i, edges.getu(chosenJ));
            edges.setu(chosenJ, aux);
         }
      }

      for (int i = 0; i < numEdges; ++i) esubgraph.addWord(edges.getu(i));
   }

   private boolean areEdgesNeighbors(int e1, int e2) {
      int src1 = graph.edgeSrc(e1);
      int src2 = graph.edgeSrc(e2);
      if (src1 == src2) return true;

      int dst2 = graph.edgeDst(e2);
      if (src1 == dst2) return true;

      int dst1 = graph.edgeDst(e1);
      return dst1 == src2 || dst1 == dst2;
   }
}
