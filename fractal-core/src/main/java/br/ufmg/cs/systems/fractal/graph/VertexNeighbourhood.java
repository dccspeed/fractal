package br.ufmg.cs.systems.fractal.graph;

import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ReclaimableIntCollection;
import com.koloboke.collect.IntCollection;
import java.util.function.IntConsumer;
import java.util.function.Predicate;

import com.koloboke.function.IntIntConsumer;

public interface VertexNeighbourhood {
    IntCollection getNeighborVertices();
    IntCollection getNeighborEdges();
    ReclaimableIntCollection getEdgesWithNeighbourVertex(int neighbourVertexId);

    boolean isNeighbourVertex(int vertexId);

    void addEdge(int neighbourVertexId, int edgeId);

    void forEachEdgeId(int nId, IntConsumer intConsumer);
    
    void forEachVertexEdge(IntIntConsumer consumer);
   
    int filterVertices(AtomicBitSetArray tag);
    
    int filterEdges(AtomicBitSetArray tag);
    
    int filter(AtomicBitSetArray vtag, AtomicBitSetArray etag);

    int filter(Predicate<Vertex> vpred, Predicate<Edge> epred);

    void removeVertex(int vertexId);

    void reset();

    void buildSortedNeighborhood();

    IntArrayList getOrderedVertices();
    
    IntArrayList getOrderedEdges();
}
