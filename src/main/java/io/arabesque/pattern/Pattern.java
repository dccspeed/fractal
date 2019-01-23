package io.arabesque.pattern;

import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.utils.collection.IntArrayList;
import com.koloboke.collect.map.IntIntMap;
import org.apache.hadoop.io.Writable;

import java.io.Externalizable;

public interface Pattern extends Writable, Externalizable {
    Pattern copy();

    void init(Configuration config);
    
    void reset();

    void setEmbedding(Embedding embedding);

    int getNumberOfVertices();

    boolean addEdge(int edgeId);

    boolean addEdge(PatternEdge patternEdge);

    int getNumberOfEdges();

    boolean turnCanonical();

    IntArrayList getVertices();

    PatternEdgeArrayList getEdges();

    VertexPositionEquivalences getVertexPositionEquivalences();
    
    VertexPositionEquivalences getVertexPositionEquivalences(IntArrayList vertexLabels);
    
    EdgePositionEquivalences getEdgePositionEquivalences();
    
    EdgePositionEquivalences getEdgePositionEquivalences(IntArrayList edgeLabels);

    IntIntMap getCanonicalLabeling();

    public boolean testSymmetryBreakerExt(Embedding embedding, int targetVertex);

    public boolean testSymmetryBreakerPos(Embedding embedding, int pos);
    
    public int sbLowerBound(Embedding embedding, int pos);

    public void readSymmetryBreakingConditions(Object path);
    
    public Configuration getConfig();

    String toOutputString();
   
    ////////
    boolean equals(Object o, int upTo);
    
}
