package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.set.ObjSet;

import java.io.Externalizable;

public interface Pattern extends Externalizable {
    Pattern copy();

   ObjArrayList<IntArrayList> getVertexPosToEdgeIndices();

   void setVertexLabels(int... vlabels);

   int getFirstVertexLabel();

   void addVertexStandalone(int vlabel);

   void addVertexStandalone();

   void init(Configuration config);

   void removeLastNEdges(int n);

   void removeLastNVertices(int n);

   IntArrayList getVertexLabels(boolean shouldConsiderVertexLabels);

   IntArrayList getEdgeLabels(boolean shouldConsiderEdgeLabels);

   PatternExplorationPlan explorationPlan();

   void setExplorationPlan(PatternExplorationPlan explorationPlan);

   void reset();

    void setSubgraph(Subgraph subgraph);

    int getNumberOfVertices();

    boolean addEdge(int edgeId);

    boolean addEdge(PatternEdge patternEdge);

   void addEdgeStandalone(PatternEdge edge);

   int getNumberOfEdges();

    boolean relabel(IntIntMap labeling);

    boolean turnCanonical();

    IntArrayList getVertices();

    PatternEdgeArrayList getEdges();

    VertexPositionEquivalences getVertexPositionEquivalences();
    
    VertexPositionEquivalences getVertexPositionEquivalences(IntArrayList vertexLabels, IntArrayList edgeLabels);

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

   void setEdgeLabeled(boolean edgeLabeled);

   Configuration getConfig();

   boolean equals(Object o, int upTo);
    
}
