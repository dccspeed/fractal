package br.ufmg.cs.systems.fractal.graph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;
import br.ufmg.cs.systems.fractal.util.EdgePredicates;
import br.ufmg.cs.systems.fractal.util.VertexPredicate;
import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayListView;
import com.koloboke.collect.IntCollection;
import com.koloboke.function.IntIntConsumer;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

public interface MainGraph<V, E> {
   MainGraph addEdge(Edge edge);

   void addEdge(int u, int v, int e);

   void addEdgeLabel(int e, int label);

   MainGraph addVertex(Vertex vertex);

   /* Revised API */
   /* update graph */
   void addVertex(int u);

   void addVertexLabel(int u, int label);

   void afterGraphUpdate();

   boolean containsEdge(int e);

   boolean containsVertex(int u);

   int edgeDst(int e);

   int edgeLabel(int e);

   IntArrayListView edgeLabels(int e);

   void edgeLabels(int e, IntArrayListView view);

   int edgeSrc(int e);

   int filter(AtomicBitSetArray vtag, AtomicBitSetArray etag);

   int filterEdges(Predicate<Edge<E>> epred);

   int filterVertices(Predicate<Vertex<V>> vpred);

   void forEachCommonEdgeLabels(IntArrayList edges, IntConsumer consumer);

   void forEachEdge(IntConsumer consumer);

   void forEachEdge(int u, int v, IntConsumer consumer);

   Edge getEdge(int edgeId);

   int getId();

   void validExtensionsPatternInducedLabeled(Computation computation,
                                             Subgraph subgraph, IntArrayList intersectionVertexIdxs, IntArrayList differenceVertexIdxs, IntArrayList starts, IntArrayList ends, int vertexLowerBound, int vertexUpperBound, VertexPredicate vpred, EdgePredicates epreds, IntCollection result);

   void validExtensionsPatternInduced(Computation computation,
                                      Subgraph subgraph,
                                      IntArrayList intersectionVertexIdxs,
                                      IntArrayList differenceVertexIdxs,
                                      IntArrayList starts, IntArrayList ends,
                                      int vertexLowerBound,
                                      int vertexUpperBound,
                                      IntCollection result);

   void setId(int id);

   Vertex getVertex(int vertexId);

   VertexNeighbourhood getVertexNeighbourhood(int vertexId);

   IntCollection getVertexNeighbours(int vertexId);

   boolean isEdgeLabelled();

   boolean isMultiGraph();

   /* graph traversals */

   void validExtensionsEdgeInduced(Computation computation, Subgraph subgraph,
                                   IntCollection validExtensions);

   void validExtensionsVertexInduced(Computation computation,
                                     Subgraph subgraph,
                                     IntCollection validExtensions);

   IntArrayListView neighborhoodVertices(int u);

   void neighborhoodVertices(int u, IntArrayListView view);

   int numEdges();

   /* query graph properties */
   int numVertices();

   int undoEdgeFilter();

   int undoVertexFilter();

   int vertexLabel(int u);

   IntArrayListView vertexLabels(int u);

   void vertexLabels(int u, IntArrayListView view);
}
