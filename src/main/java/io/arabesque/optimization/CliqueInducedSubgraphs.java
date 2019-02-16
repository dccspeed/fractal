package io.arabesque.optimization;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.IntObjCursor;
import com.koloboke.collect.set.hash.HashIntSet;
import io.arabesque.utils.pool.HashIntSetPool;

import io.arabesque.computation.Computation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.graph.MainGraph;
import io.arabesque.graph.VertexNeighbourhood;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.ObjArrayList;
import io.arabesque.utils.pool.IntArrayListPool;

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

   public IntCollection extensions(Embedding embedding, Computation computation) {
      int numVertices = embedding.getNumVertices();
      if (numVertices == 0) { // single vertices
         return embedding.getExtensibleWordIds(computation);
      } else if (numVertices == 1) { // bootstrap from input graph
         bootstrap(embedding, computation, embedding.getVertices().getLast());
         return sgs.getLast().getAdjList().keySet();
      } else if (numVertices < cliqueSize - 1) { // extend from previous
         extend(embedding, computation,
               embedding.getVertices().getLast());
         return sgs.getLast().getAdjList().keySet();
      } else if (numVertices == cliqueSize - 1) { // get last adj list
         ensureConsistency(embedding, computation);
         return sgs.getLast().getAdjList().get(embedding.getVertices().getLast());
      } else {
         throw new RuntimeException("Not allowed for " +
               cliqueSize + "-cliques");
      }
   }

   private void bootstrap(Embedding embedding, Computation computation, int u) {
      MainGraph graph = computation.getConfig().getMainGraph();
      sgs.clear();
      CliqueInducedSubgraph sg = CliqueInducedSubgraph.bootstrap(
            computation.getConfig(), u);
      sgs.add(sg);
   }

   private void extend(Embedding embedding, Computation computation, int u) {
      ensureConsistency(embedding, computation);
      CliqueInducedSubgraph sg = sgs.getLast().extend(embedding, u);
      sgs.add(sg);
   }

   private void ensureConsistency(Embedding embedding, Computation computation) {
      IntArrayList vertices = embedding.getVertices();
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
            bootstrap(embedding, computation, vertices.get(0));
         } else {
            int v = vertices.get(numSubgraphs);
            CliqueInducedSubgraph sg = sgs.getLast().extend(embedding, v);
            sgs.add(sg);
         }
         numSubgraphs = sgs.size();
      }
   }
}
