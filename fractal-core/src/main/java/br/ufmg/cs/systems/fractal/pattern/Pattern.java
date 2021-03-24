package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import com.koloboke.collect.map.IntIntMap;

import java.io.Externalizable;

public interface Pattern extends Externalizable {
    Pattern copy();

   int getFirstVertexLabel();

   void init(Configuration config);

   void removeLastNEdges(int n);

   PatternExplorationPlan explorationPlan();

   void setExplorationPlan(PatternExplorationPlan explorationPlan);

   void reset();

    void setSubgraph(Subgraph subgraph);

    int getNumberOfVertices();

    boolean addEdge(int edgeId);

    boolean addEdge(PatternEdge patternEdge);

    int getNumberOfEdges();

    boolean relabel(IntIntMap labeling);

    boolean turnCanonical();

    IntArrayList getVertices();

    PatternEdgeArrayList getEdges();

    VertexPositionEquivalences getVertexPositionEquivalences();
    
    VertexPositionEquivalences getVertexPositionEquivalences(IntArrayList vertexLabels);

   IntIntMap getCanonicalLabeling();

   ObjArrayList<IntArrayList> vsymmetryBreakerUpperBound();

   ObjArrayList<IntArrayList> vsymmetryBreakerLowerBound();

   void updateSymmetryBreaker();

   void updateSymmetryBreakerVertexUnlabeled();

   int sbUpperBound(Subgraph subgraph, int pos);

   int sbLowerBound(Subgraph subgraph, int pos);

   boolean sbValidOrdering(IntArrayList ordering);

   boolean connectedValidOrdering(IntArrayList ordering);

   void updateSymmetryBreaker(IntArrayList ordering);

   boolean induced();

    void setInduced(boolean induced);

   boolean vertexLabeled();

   boolean edgeLabeled();

   void setVertexLabeled(boolean vertexLabeled);

   Configuration getConfig();

    String toOutputString();
   
    ////////
    boolean equals(Object o, int upTo);
    
}
