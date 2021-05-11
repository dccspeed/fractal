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
   void addEdge(int u, int v, int e);

   void addEdgeLabel(int e, int label);

   void addVertex(int u);

   void addVertexLabel(int u, int label);

   int edgeDst(int e);

   int edgeLabel(int e);

   int edgeSrc(int e);

   void forEachCommonEdgeLabels(IntArrayList edges, IntConsumer consumer);

   void forEachEdge(IntConsumer consumer);

   void forEachEdge(int u, int v, IntConsumer consumer);

   void init(Configuration configuration) throws IOException;

   boolean isEdgeValid(int e);

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

   /* graph traversals */

   void validExtensionsEdgeInduced(Computation computation, Subgraph subgraph,
                                   IntCollection validExtensions);

   void validExtensionsVertexInduced(Computation computation,
                                     Subgraph subgraph,
                                     IntCollection validExtensions);

   IntArrayListView neighborhoodVertices(int u);

   void neighborhoodVertices(int u, IntArrayListView view);

   int numEdges();

   int numValidEdges();

   int numVertices();

   int vertexLabel(int u);

}
