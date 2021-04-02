package br.ufmg.cs.systems.fractal.subgraph;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;

public interface Subgraph {
   void addWord(int word);

   long computeExtensionCost(IntArrayList extensionCandidates);

   void computeExtensions(Computation computation, IntArrayList extensions);

   void computeFirstLevelExtensions(Computation computation,
                                    IntArrayList extensions);

   Configuration getConfig();

   IntArrayList getEdges();

   int getNumEdges();

   int getNumVertices();

   int getNumWords();

   MainGraph getMainGraph();

   IntArrayList getVertices();

   IntArrayList getWords();

   void init(Configuration configuration);

   int numEdgesAdded();

   int numVerticesAdded();

   int numVerticesAdded(int wordIdx);

   Pattern quickPattern();

   void removeLastWord();

   void reset();
}
