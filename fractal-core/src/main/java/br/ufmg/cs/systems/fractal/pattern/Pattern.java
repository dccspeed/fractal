package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import com.koloboke.collect.map.IntIntMap;
import org.apache.hadoop.io.Writable;

import java.io.Externalizable;
import java.io.IOException;

public interface Pattern extends Writable, Externalizable {
    Pattern copy();

    void init(Configuration config);
    
    void reset();

    void setSubgraph(Subgraph subgraph);

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

    boolean testSymmetryBreakerExt(Subgraph subgraph, int targetVertex);

    boolean testSymmetryBreakerPos(Subgraph subgraph, int pos);

    ObjArrayList<IntArrayList> vsymmetryBreaker();

    int sbLowerBound(Subgraph subgraph, int pos);

    void readSymmetryBreakingConditions(String path) throws IOException;

    boolean induced();

    void setInduced(boolean induced);

    Configuration getConfig();

    String toOutputString();
   
    ////////
    boolean equals(Object o, int upTo);
    
}
