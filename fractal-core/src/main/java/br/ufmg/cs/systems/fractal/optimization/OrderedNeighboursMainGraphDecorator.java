package br.ufmg.cs.systems.fractal.optimization;

import br.ufmg.cs.systems.fractal.graph.Edge;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.graph.Vertex;
import br.ufmg.cs.systems.fractal.graph.VertexNeighbourhood;
import br.ufmg.cs.systems.fractal.util.EdgePredicates;
import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayListView;
import com.koloboke.collect.IntCollection;
import com.koloboke.function.IntIntConsumer;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

public class OrderedNeighboursMainGraphDecorator implements OrderedNeighboursMainGraph {
    protected MainGraph underlyingMainGraph;

    protected IntArrayList[] orderedNeighbours;

    public OrderedNeighboursMainGraphDecorator(MainGraph underlyingMainGraph) {
        this.underlyingMainGraph = underlyingMainGraph;

        int numVertices = underlyingMainGraph.numVertices();

        orderedNeighbours = new IntArrayList[numVertices];

        for (int i = 0; i < numVertices; ++i) {
            IntCollection neighboursOfI = underlyingMainGraph.getVertexNeighbours(i);

            if (neighboursOfI != null) {
                orderedNeighbours[i] = new IntArrayList(neighboursOfI);
                orderedNeighbours[i].sort();
            }
        }
    }

    @Override
    public int getId() {
       return underlyingMainGraph.getId();
    }

    @Override
    public void setId(int id) {
       underlyingMainGraph.setId(id);
    }

    @Override
    public int filter(AtomicBitSetArray vtag, AtomicBitSetArray etag) {
       return underlyingMainGraph.filter(vtag, etag);
    }

    @Override
    public int undoVertexFilter() {
       return underlyingMainGraph.undoVertexFilter();
    }
    
    @Override
    public int undoEdgeFilter() {
       return underlyingMainGraph.undoEdgeFilter();
    }

    @Override
    public int filterVertices(Predicate vpred) {
       return underlyingMainGraph.filterVertices(vpred);
    }

   @Override
    public int filterEdges(Predicate epred) {
       return underlyingMainGraph.filterEdges(epred);
    }

    @Override
    public void afterGraphUpdate() {
       underlyingMainGraph.afterGraphUpdate();
    }

   @Override
   public void addVertex(int u) {

   }

   @Override
   public void addEdge(int u, int v, int e) {

   }

   @Override
   public void addVertexLabel(int u, int label) {

   }

   @Override
   public void addEdgeLabel(int e, int label) {

   }

   @Override
    public int edgeSrc(int e) {
        return 0;
    }

    @Override
    public int edgeDst(int e) {
        return 0;
    }

    @Override
    public int vertexLabel(int u) {
        return 0;
    }

    @Override
    public int edgeLabel(int e) {
        return 0;
    }

    @Override
    public boolean containsVertex(int u) {
        return false;
    }

    @Override
    public boolean containsEdge(int e) {
        return false;
    }

    @Override
    public void neighborhoodTraversalVertexRange(int u, int lowerBound, IntIntConsumer consumer) {

    }

   @Override
   public IntArrayListView neighborhoodVertices(int u) {
      return null;
   }

   @Override
    public void neighborhoodTraversalEdgeRange(int u, int lowerBound, IntIntConsumer consumer) {

    }

   @Override
   public void neighborhoodTraversal(IntArrayList intersection, IntArrayList difference, int vertexLowerBound, int vertexUpperBound, IntConsumer consumer, IntPredicate vertexPredicate, EdgePredicates edgePredicates) {

   }

   @Override
   public void forEachEdge(IntConsumer consumer) {
       underlyingMainGraph.forEachEdge(consumer);
   }

   @Override
    public MainGraph addVertex(Vertex vertex) {
        return underlyingMainGraph.addVertex(vertex);
    }

   @Override
    public Vertex getVertex(int vertexId) {
        return underlyingMainGraph.getVertex(vertexId);
    }

    @Override
    public int numVertices() {
        return underlyingMainGraph.numVertices();
    }

   @Override
    public Edge getEdge(int edgeId) {
        return underlyingMainGraph.getEdge(edgeId);
    }

    @Override
    public int numEdges() {
        return underlyingMainGraph.numEdges();
    }

   @Override
    public MainGraph addEdge(Edge edge) {
        return underlyingMainGraph.addEdge(edge);
    }

   @Override
    public VertexNeighbourhood getVertexNeighbourhood(int vertexId) {
        return underlyingMainGraph.getVertexNeighbourhood(vertexId);
    }

    @Override
    public IntCollection getVertexNeighbours(int vertexId) {
        return orderedNeighbours[vertexId];
    }

    @Override
    public boolean isEdgeLabelled() {
        return underlyingMainGraph.isEdgeLabelled();
    }

    @Override
    public boolean isMultiGraph() {
        return underlyingMainGraph.isMultiGraph();
    }

    @Override
    public void forEachEdge(int existingVertexId, int v, IntConsumer consumer) {
        underlyingMainGraph.forEachEdge(existingVertexId, v, consumer);
    }

}
