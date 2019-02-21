package br.ufmg.cs.systems.fractal.graph;

import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.ReclaimableIntCollection;
import com.koloboke.collect.IntCollection;
import java.util.function.IntConsumer;
import java.util.function.Predicate;

public interface MainGraph<V,E> {
    int getId();
    
    void setId(int id);

    void reset();

    boolean isNeighborVertex(int v1, int v2);

    MainGraph addVertex(Vertex vertex);

    Vertex[] getVertices();

    Vertex getVertex(int vertexId);

    int getNumberVertices();

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

    void forEachEdgeId(int v1, int v2, IntConsumer intConsumer);

    int filterVertices(AtomicBitSetArray tag);

    int filterVertices(Predicate<Vertex<V>> vpred);

    int filterEdges(AtomicBitSetArray tag);

    int filterEdges(Predicate<Edge<E>> epred);

    int undoVertexFilter();
    
    int undoEdgeFilter();

    int filter(AtomicBitSetArray vtag, AtomicBitSetArray etag);

    void buildSortedNeighborhood();

}
