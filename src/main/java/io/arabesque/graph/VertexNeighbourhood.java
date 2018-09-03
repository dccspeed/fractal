package io.arabesque.graph;

import io.arabesque.utils.collection.ReclaimableIntCollection;
import io.arabesque.utils.collection.AtomicBitSetArray;
import io.arabesque.utils.collection.IntArrayList;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.function.IntConsumer;
import com.koloboke.function.IntIntConsumer;

public interface VertexNeighbourhood {
    IntCollection getNeighborVertices();
    IntCollection getNeighborEdges();
    ReclaimableIntCollection getEdgesWithNeighbourVertex(int neighbourVertexId);

    boolean isNeighbourVertex(int vertexId);

    void addEdge(int neighbourVertexId, int edgeId);

    void forEachEdgeId(int nId, IntConsumer intConsumer);
    
    void forEachVertexEdge(IntIntConsumer consumer);
   
    int forEachVertexEdgeLowerBound(
         IntIntConsumer consumer, int lowerBound);
    
    int applyTagVertexes(AtomicBitSetArray tag);
    
    int applyTagEdges(AtomicBitSetArray tag);
    
    int applyTag(AtomicBitSetArray vtag, AtomicBitSetArray etag);
    
    void removeVertex(int vertexId);

    void reset();

    void buildSortedNeighborhood();

    IntArrayList getOrderedVertices();
    
    IntArrayList getOrderedEdges();
}
