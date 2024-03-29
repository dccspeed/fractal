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
      IntArrayList vertices = subgraph.getVertices();
      int numVertices = vertices.size();
      int target1 = 0;
      int target2 = 1;
      verticesBackup.clear();
      verticesBackup.addAll(vertices);
      for (int i = 0; i < numVertices; ++i) {
         for (int j = i + 1; j < numVertices; ++j) {
            //verticesBackup.swap(target1, i);
            //verticesBackup.swap(target2, j);
            verticesBackup.setu(target1, vertices.getu(i));
            verticesBackup.setu(target2, vertices.getu(j));
            verticesBackup.setu(i, vertices.getu(target1));
            verticesBackup.setu(j, vertices.getu(target2));

            convert(subgraph, computation, nextSubgraph, nextComputation);
            nextEngine.initialWorkCompute();

            //verticesBackup.swap(target1, i);
            //verticesBackup.swap(target2, j);

            verticesBackup.setu(target1, vertices.getu(target1));
            verticesBackup.setu(target2, vertices.getu(target2));
            verticesBackup.setu(i, vertices.getu(i));
            verticesBackup.setu(j, vertices.getu(j));


            verticesBackup.clear();
            verticesBackup.addAll(vertices);

            //if (!subgraph.getVertices().equals(verticesBackup)) {
            //   throw new RuntimeException();
            //}
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
   public boolean convert(VertexInducedSubgraph vsubgraph,
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

      return false;
   }
}
