package br.ufmg.cs.systems.fractal.graph;

import br.ufmg.cs.systems.fractal.util.EdgePredicates;
import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ReclaimableIntCollection;
import com.koloboke.collect.IntCollection;
import com.koloboke.function.IntIntConsumer;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

public interface MainGraph<V,E> {
    int getId();
    
    void setId(int id);

    MainGraph addVertex(Vertex vertex);

    Vertex getVertex(int vertexId);

    Edge getEdge(int edgeId);

    MainGraph addEdge(Edge edge);

    VertexNeighbourhood getVertexNeighbourhood(int vertexId);

    IntCollection getVertexNeighbours(int vertexId);

    boolean isEdgeLabelled();

    boolean isMultiGraph();

    int filterVertices(Predicate<Vertex<V>> vpred);

    int filterEdges(Predicate<Edge<E>> epred);

    int undoVertexFilter();
    
    int undoEdgeFilter();

    int filter(AtomicBitSetArray vtag, AtomicBitSetArray etag);

    void afterGraphUpdate();

    /* Revised API */
    /* update graph */
    void addVertex(int u);
    void addEdge(int u, int v, int e);
    void addVertexLabel(int u, int label);
    void addEdgeLabel(int e, int label);

    /* query graph properties */
    int numVertices();
    int numEdges();
    int edgeSrc(int e);
    int edgeDst(int e);
    int vertexLabel(int u);
    int edgeLabel(int e);
    boolean containsVertex(int u);
    boolean containsEdge(int e);

    /* graph traversals */
    void neighborhoodTraversalVertexRange(int u, int lowerBound, IntIntConsumer consumer);
    void neighborhoodTraversalEdgeRange(int u, int lowerBound, IntIntConsumer consumer);
    void neighborhoodTraversal(IntArrayList intersection, IntArrayList difference, int vertexLowerBound,
                               IntConsumer consumer, IntPredicate vertexPredicate, EdgePredicates edgePredicates);
    void neighborhoodTraversal(IntArrayList intersection, IntArrayList difference, int vertexLowerBound, IntConsumer consumer);
    void forEachEdge(IntConsumer consumer);
    void forEachEdge(int u, int v, IntConsumer consumer);
}
