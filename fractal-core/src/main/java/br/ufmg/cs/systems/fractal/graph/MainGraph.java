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

    void reset();

    boolean isNeighborVertex(int v1, int v2);

    MainGraph addVertex(Vertex vertex);

    Vertex[] getVertices();

    Vertex getVertex(int vertexId);


    Edge[] getEdges();

    Edge getEdge(int edgeId);

    int getNumberEdges();

    ReclaimableIntCollection getEdgeIds(int v1, int v2);

    MainGraph addEdge(Edge edge);

    boolean areEdgesNeighbors(int edge1Id, int edge2Id);

    @Deprecated
    boolean isNeighborEdge(int src1, int dest1, int edge2);

    VertexNeighbourhood getVertexNeighbourhood(int vertexId);

    IntCollection getVertexNeighbours(int vertexId);

    boolean isEdgeLabelled();

    boolean isMultiGraph();


    int filterVertices(AtomicBitSetArray tag);

    int filterVertices(Predicate<Vertex<V>> vpred);

    int filterEdges(AtomicBitSetArray tag);

    int filterEdges(Predicate<Edge<E>> epred);

    int undoVertexFilter();
    
    int undoEdgeFilter();

    int filter(AtomicBitSetArray vtag, AtomicBitSetArray etag);

    void buildSortedNeighborhood();

    /* need to keep */
    int getNumberVertices();
    int edgeSrc(int e);
    int edgeDst(int e);
    int vertexLabel(int u);
    int edgeLabel(int e);
    boolean containsVertex(int u);
    boolean containsEdge(int e);
    void neighborhoodTraversalVertexRange(int u, int lowerBound, IntIntConsumer consumer);
    void neighborhoodTraversalEdgeRange(int u, int lowerBound, IntIntConsumer consumer);
    void neighborhoodTraversal(IntArrayList intersection, IntArrayList difference, int vertexLowerBound,
                               IntConsumer consumer, IntPredicate vertexPredicate, EdgePredicates edgePredicates);
    void forEachEdgeId(int v1, int v2, IntConsumer intConsumer);
}
