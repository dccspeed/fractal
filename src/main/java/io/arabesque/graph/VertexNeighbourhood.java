package io.arabesque.graph;

import io.arabesque.utils.collection.ReclaimableIntCollection;
import io.arabesque.utils.collection.AtomicBitSetArray;
import io.arabesque.utils.collection.RoaringBitSet;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.function.IntConsumer;

public interface VertexNeighbourhood {
    IntCollection getNeighborVertices();
    IntCollection getNeighborEdges();
    ReclaimableIntCollection getEdgesWithNeighbourVertex(int neighbourVertexId);

    boolean isNeighbourVertex(int vertexId);

    void addEdge(int neighbourVertexId, int edgeId);

    void forEachEdgeId(int nId, IntConsumer intConsumer);
    
    int applyTagVertexes(AtomicBitSetArray tag);
    
    int applyTagEdges(AtomicBitSetArray tag);
    
    int applyTag(AtomicBitSetArray vtag, AtomicBitSetArray etag);
    
    void removeVertex(int vertexId);

    void reset();

    void buildSortedNeighborhood();

    int[] getOrderedVertices();
    
    int[] getOrderedEdges();

    RoaringBitSet getVerticesBitmap();
    
    RoaringBitSet getEdgesBitmap();
}
