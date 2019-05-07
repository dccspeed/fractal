package br.ufmg.cs.systems.fractal.subgraph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.Edge;
import br.ufmg.cs.systems.fractal.graph.LabelledEdge;
import br.ufmg.cs.systems.fractal.graph.Vertex;
import br.ufmg.cs.systems.fractal.misc.WritableObject;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.map.hash.HashIntObjMap;

import java.io.Externalizable;

public interface Subgraph extends WritableObject, Externalizable {
    void init(Configuration configuration);

    Configuration getConfig();

    IntArrayList getWords();

    IntArrayList getVertices();
    
    <V> Vertex<V> vertex(int vertexId);

    int getNumVertices();

    IntArrayList getEdges();
    
    <E> Edge<E> edge(int edgeId);
    
    <E> LabelledEdge<E> labelledEdge(int edgeId);
    
    int getNumEdges();

    int getNumWords();

    Pattern getPattern();

    int numVerticesAdded();

    int numEdgesAdded();

    void addWord(int word);

    void removeLastWord();

    IntCollection computeExtensions(Computation computation);

  IntCollection extensions();

    boolean isCanonicalSubgraphWithWord(int wordId);

    String toOutputString();
    
    void nextExtensionLevel();
    
    void nextExtensionLevel(Subgraph other);
    
    void previousExtensionLevel();

    void applyTagFrom(Computation computation,
          AtomicBitSetArray vtag, AtomicBitSetArray etag, int pos);
    
    void applyTagTo(Computation computation,
          AtomicBitSetArray vtag, AtomicBitSetArray etag, int pos);

    HashIntObjMap cacheStore();
}
