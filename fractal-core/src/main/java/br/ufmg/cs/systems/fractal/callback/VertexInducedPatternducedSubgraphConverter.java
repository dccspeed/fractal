package br.ufmg.cs.systems.fractal.callback;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.computation.SparkFromScratchEngine;
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;

/**
 * This is a converter between a canonical code for pattern induced subgraphs
 * into a canonical code for vertex-induced subgraphs.
 */
public class VertexInducedPatternducedSubgraphConverter
        implements SubgraphConverter<VertexInducedSubgraph, PatternInducedSubgraph> {
   private SparkFromScratchEngine<PatternInducedSubgraph> nextEngine;
   private Computation<PatternInducedSubgraph> nextComputation;
   private PatternInducedSubgraph nextSubgraph;

   private IntArrayList verticesBackup;

   @Override
   public void apply(VertexInducedSubgraph subgraph,
                     Computation<VertexInducedSubgraph> computation) {
      int numVertices = subgraph.getNumVertices();
      int target1 = 0;
      int target2 = 1;
      verticesBackup.clear();
      verticesBackup.addAll(subgraph.getVertices());
      int aux1 = verticesBackup.getu(target1);
      int aux2 = verticesBackup.getu(target2);
      for (int i = 0; i < numVertices; ++i) {
         int v1 = verticesBackup.getu(i);
         for (int j = i + 1; j < numVertices; ++j) {
            int v2 = verticesBackup.getu(j);

            verticesBackup.setu(target1, v1);
            verticesBackup.setu(target2, v2);
            verticesBackup.setu(i, aux1);
            verticesBackup.setu(j, aux2);

            convert(subgraph, computation, nextSubgraph, nextComputation);
            nextEngine.initialWorkCompute();

            verticesBackup.setu(i, v1);
            verticesBackup.setu(j, v2);
            verticesBackup.setu(target1, aux1);
            verticesBackup.setu(target2, aux2);
         }
      }
      //convert(subgraph, computation, nextSubgraph, nextComputation);
      //nextEngine.initialWorkCompute();
   }

   @Override
   public void init(Computation<VertexInducedSubgraph> computation) {
      nextEngine = (SparkFromScratchEngine<PatternInducedSubgraph>)
              computation.getExecutionEngine().getNextEngine();
      nextComputation = nextEngine.computation();
      nextSubgraph = nextComputation.getSubgraphEnumerator().getSubgraph();
      verticesBackup = new IntArrayList();
   }

   @Override
   public void convert(VertexInducedSubgraph vsubgraph,
                       Computation<VertexInducedSubgraph> vcomputation,
                       PatternInducedSubgraph psubgraph,
                       Computation<PatternInducedSubgraph> pcomputation) {
      //IntArrayList vertices = vsubgraph.getVertices();
      IntArrayList vertices = verticesBackup;
      int numVertices = vertices.size();
      psubgraph.reset();
      for (int i = 0; i < numVertices; ++i) {
         psubgraph.addWord(vertices.getu(i));
      }
   }
}
