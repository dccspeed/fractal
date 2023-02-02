package br.ufmg.cs.systems.fractal.graph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.EdgePredicates;
import br.ufmg.cs.systems.fractal.util.VertexPredicate;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayListView;
import com.koloboke.collect.IntCollection;

import java.io.IOException;
import java.util.function.IntConsumer;

public interface MainGraph {
   void init(Configuration configuration) throws IOException;

   boolean isEdgeValid(int u, int v, int e);

   boolean isVertexValid(int u);

   int numVertices();

   IntArrayListView neighborhoodEdges(int u);

   void neighborhoodEdges(int u, IntArrayListView view);

   int numEdges();

   boolean isEdgeValid(int e);

   int edgeSrc(int e);

   int edgeDst(int e);

   int firstEdgeLabel(int e);

   int firstVertexLabel(int u);

   void forEachEdge(int u, int v, IntConsumer consumer);

   IntArrayListView neighborhoodVertices(int u);

   void neighborhoodVertices(int u, IntArrayListView view);

   void forEachCommonEdgeLabels(IntArrayList edges, IntConsumer consumer);

   // Canonical subgraph enumeration

   void validExtensionsPatternInducedLabeled(Computation computation,
                                             Subgraph subgraph,
                                             IntArrayList intersectionVertexIdxs,
                                             IntArrayList differenceVertexIdxs,
                                             IntArrayList starts,
                                             IntArrayList ends,
                                             int vertexLowerBound,
                                             int vertexUpperBound,
                                             VertexPredicate vpred,
                                             EdgePredicates epreds,
                                             IntCollection result);

   void validExtensionsPatternInduced(Computation computation,
                                      Subgraph subgraph,
                                      IntArrayList intersectionVertexIdxs,
                                      IntArrayList differenceVertexIdxs,
                                      IntArrayList starts, IntArrayList ends,
                                      int vertexLowerBound,
                                      int vertexUpperBound,
                                      IntCollection result);

   void validExtensionsEdgeInduced(Computation computation, Subgraph subgraph,
                                   IntCollection validExtensions);

   void validExtensionsVertexInduced(Computation computation,
                                     Subgraph subgraph,
                                     IntCollection validExtensions);


   int vertexDegree(int u);
}
