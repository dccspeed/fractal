package br.ufmg.cs.systems.fractal.optimization;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import com.koloboke.collect.IntCollection;

public class CliqueInducedSubgraphs {

   private ObjArrayList<CliqueInducedSubgraph> sgs;
   private int cliqueSize;

   public CliqueInducedSubgraphs(int cliqueSize) {
      this.sgs = new ObjArrayList<CliqueInducedSubgraph>();
      this.cliqueSize = cliqueSize;
   }

   public void clear() {
      sgs.clear();
   }

   public IntCollection extensions(Subgraph subgraph, Computation computation) {
      int numVertices = subgraph.getNumVertices();
      if (numVertices == 0) { // single vertices
         return subgraph.computeExtensions(computation);
      } else if (numVertices == 1) { // bootstrap from input graph
         bootstrap(subgraph, computation, subgraph.getVertices().getLast());
         return sgs.getLast().getAdjList().keySet();
      } else if (numVertices < cliqueSize - 1) { // extend from previous
         extend(subgraph, computation,
               subgraph.getVertices().getLast());
         return sgs.getLast().getAdjList().keySet();
      } else if (numVertices == cliqueSize - 1) { // get last adj list
         ensureConsistency(subgraph, computation);
         return sgs.getLast().getAdjList().get(subgraph.getVertices().getLast());
      } else {
         throw new RuntimeException("Not allowed for " +
               cliqueSize + "-cliques");
      }
   }

   private void bootstrap(Subgraph subgraph, Computation computation, int u) {
      MainGraph graph = computation.getConfig().getMainGraph();
      sgs.clear();
      CliqueInducedSubgraph sg = CliqueInducedSubgraph.bootstrap(
            computation.getConfig(), u);
      sgs.add(sg);
   }

   private void extend(Subgraph subgraph, Computation computation, int u) {
      ensureConsistency(subgraph, computation);
      CliqueInducedSubgraph sg = sgs.getLast().extend(subgraph, u);
      sgs.add(sg);
   }

   private void ensureConsistency(Subgraph subgraph, Computation computation) {
      IntArrayList vertices = subgraph.getVertices();
      int numVertices = vertices.size();

      // remove old subgraphs
      while (sgs.size() > numVertices - 1) {
         CliqueInducedSubgraph removed = sgs.remove(sgs.size() - 1);
         CliqueInducedSubgraphPool.instance().reclaimObject(removed);
      }

      // compute from scratch if necessary
      int numSubgraphs = sgs.size();
      while (numSubgraphs < numVertices - 1) {
         if (numSubgraphs == 0) {
            int v = vertices.get(numSubgraphs);
            bootstrap(subgraph, computation, vertices.get(0));
         } else {
            int v = vertices.get(numSubgraphs);
            CliqueInducedSubgraph sg = sgs.getLast().extend(subgraph, v);
            sgs.add(sg);
         }
         numSubgraphs = sgs.size();
      }
   }
}
