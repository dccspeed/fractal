package br.ufmg.cs.systems.fractal.callback;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.computation.SparkFromScratchEngine;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.pattern.PatternEdge;
import br.ufmg.cs.systems.fractal.pattern.PatternEdgeArrayList;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph;
import br.ufmg.cs.systems.fractal.util.Logging;
import br.ufmg.cs.systems.fractal.util.Logging$;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;

import java.util.function.IntConsumer;

/**
 * This is a converter between a canonical code for pattern induced subgraphs
 * into a canonical code for vertex-induced subgraphs.
 */
public class PatternInducedVertexInducedSubgraphConverter
        implements SubgraphConverter<PatternInducedSubgraph, VertexInducedSubgraph> {
   private SparkFromScratchEngine<VertexInducedSubgraph> nextEngine;
   private Computation<VertexInducedSubgraph> nextComputation;
   private VertexInducedSubgraph nextSubgraph;
   private MainGraph graph;

   private ObjArrayList<IntSet> patternAdjList;
   private IntIntMap vertexToPos;
   private IntArrayList posToVertex;
   private IntArrayList verticesAux;
   private IntSet cummulativeNeighborhood;
   private IntSet visitedVertices;
   private AdjListAdder adjListAdder;
   private MinFinder minFinder;

   @Override
   public void apply(PatternInducedSubgraph subgraph,
                     Computation<PatternInducedSubgraph> computation) {
      convert(subgraph, computation, nextSubgraph, nextComputation);
      nextEngine.initialWorkCompute();
   }

   @Override
   public void init(Computation<PatternInducedSubgraph> computation) {
      nextEngine = (SparkFromScratchEngine<VertexInducedSubgraph>)
              computation.getExecutionEngine().getNextEngine();
      nextComputation = nextEngine.computation();
      nextSubgraph = nextComputation.getSubgraphEnumerator().getSubgraph();
      graph = computation.getConfig().getMainGraph();
      fillPatternAdjLists(computation.getPattern());
      cummulativeNeighborhood = HashIntSets.newMutableSet();
      visitedVertices = HashIntSets.newUpdatableSet();
      adjListAdder = new AdjListAdder();
      minFinder = new MinFinder();
   }

   @Override
   public boolean convert(PatternInducedSubgraph psubgraph,
                       Computation<PatternInducedSubgraph> pcomputation,
                       VertexInducedSubgraph vsubgraph,
                       Computation<VertexInducedSubgraph> vcomputation) {
      // apply vertices to underlying pattern
      IntArrayList pvertices = psubgraph.getVertices();
      int numVertices = pvertices.size();
      applySubgraphVertices(pvertices);

      // clear auxiliary structures
      verticesAux.clear();
      cummulativeNeighborhood.clear();
      visitedVertices.clear();

      // find min vertex
      minFinder.clear();
      pvertices.forEach(minFinder);
      int u = minFinder.getMin();

      // add min vertex
      verticesAux.add(u);
      visitedVertices.add(u);

      // vertices reached from first vertex
      adjListAdder.setOutSet(cummulativeNeighborhood);
      IntSet neighborsPos = patternAdjList.getu(vertexToPos.get(u));
      neighborsPos.forEach(adjListAdder);

      // add other vertices
      for (int i = 0; i < numVertices - 1; ++i) {
         minFinder.clear();
         cummulativeNeighborhood.forEach(minFinder);
         u = minFinder.getMin();
         verticesAux.add(u);
         visitedVertices.add(u);
         cummulativeNeighborhood.removeInt(u);
         neighborsPos = patternAdjList.getu(vertexToPos.get(u));
         neighborsPos.forEach(adjListAdder);
      }

      // add vertices to vertex-induced subgraph
      vsubgraph.reset();
      for (int i = 0; i < numVertices; ++i) {
         vsubgraph.addWord(verticesAux.getu(i));
      }

      return false;
   }

   /**
    * Called once to create a adjacency list of the underlying pattern, with
    * the purpose of helping the reordering step on each convertion between
    * pattern-induced subgraph -> vertex-induced subgraph
    * @param pattern
    */
   private void fillPatternAdjLists(Pattern pattern) {
      int numVertices = pattern.getNumberOfVertices();
      int numEdges = pattern.getNumberOfEdges();
      PatternEdgeArrayList edges = pattern.getEdges();

      if (vertexToPos == null) {
         vertexToPos = HashIntIntMaps.newUpdatableMap(numVertices);
      }

      if (verticesAux == null) {
         verticesAux = new IntArrayList(numVertices);
      }

      if (patternAdjList == null) {
         patternAdjList = new ObjArrayList<>(pattern.getNumberOfVertices());
      }
      patternAdjList.clear();

      for (int i = 0; i < numVertices; ++i) {
         patternAdjList.add(HashIntSets.newUpdatableSet(numVertices - 1));
      }

      for (int i = 0; i < numEdges; ++i) {
         PatternEdge pe = edges.getu(i);
         int src = pe.getSrcPos();
         int dst = pe.getDestPos();
         patternAdjList.getu(src).add(dst);
         patternAdjList.getu(dst).add(src);
      }
   }

   /**
    * Fix current vertex mappings considering a new set of vertices and the
    * underlying pattern adjacency list
    * @param vertices
    */
   private void applySubgraphVertices(IntArrayList vertices) {
      int numVertices = vertices.size();
      vertexToPos.clear();
      for (int uPos = 0; uPos < numVertices; ++uPos) {
         int u = vertices.getu(uPos);
         vertexToPos.put(u, uPos);
      }

      posToVertex = vertices;
   }

   private class AdjListAdder implements IntConsumer {

      private IntSet outSet;
      private IntSet removeSet;
      private IntArrayList posToVertex;

      public void setOutSet(IntSet outSet) {
         this.outSet = outSet;
         this.posToVertex = PatternInducedVertexInducedSubgraphConverter.this
                 .posToVertex;
         this.removeSet = PatternInducedVertexInducedSubgraphConverter.this
                 .visitedVertices;
      }

      @Override
      public void accept(int uPos) {
         int u = posToVertex.getu(uPos);
         if (!removeSet.contains(u)) {
            outSet.add(u);
         }
      }
   }

   private class MinFinder implements IntConsumer {

      private int min;

      public void clear() {
         min = Integer.MAX_VALUE;
      }

      public int getMin() {
         return min;
      }

      @Override
      public void accept(int e) {
         if (e < min) {
            min = e;
         }
      }
   }

}
