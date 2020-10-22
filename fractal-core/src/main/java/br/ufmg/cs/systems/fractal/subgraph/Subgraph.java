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

import java.io.Externalizable;

public interface Subgraph extends WritableObject, Externalizable {
   long computeExtensionCost(IntArrayList extensionCandidates);

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

   Pattern quickPattern();

   int numVerticesAdded();

   int numEdgesAdded();

   int numVerticesAdded(int wordIdx);

   void addWord(int word);

   void setWordAndTruncate(int word, int index);

   void removeLastWord();

   IntCollection computeExtensions(Computation computation);

   void reset();

   String toOutputString();

}
