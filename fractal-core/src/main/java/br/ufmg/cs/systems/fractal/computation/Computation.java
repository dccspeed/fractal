package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.aggregation.SubgraphAggregation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;

import java.io.Serializable;

public interface Computation<S extends Subgraph> extends Serializable {

   void addCanonicalSubgraphs(long inc);

   void addExpansionNeighborhood(IntArrayList extensionCandidates);

   long getNumUniqueExtensions();

   void addExtensionUniqueCandidates(long inc);

   void addValidSubgraphs(long inc);

   void compute();

   void computeAndProcessExtensions();

   boolean filter(S Subgraph);

   long getNumCanonicalExtensions();

   Configuration getConfig();

   int getDepth();

   ExecutionEngine<S> getExecutionEngine();

   void setExecutionEngine(ExecutionEngine<S> executionEngine);

   long getNumExtensions();

   int getInitialNumWords();

   int getNumberPartitions();

   int getPartitionId();

   Pattern getPattern();

   int getStep();

   SubgraphAggregation<S> getSubgraphAggregation();

   Class<? extends Subgraph> getSubgraphClass();

   SubgraphEnumerator<S> getSubgraphEnumerator();

   long getNumValidExtensions();

   void init(Configuration config);

   void init(ExecutionEngine<S> engine, Configuration config);

   void initAggregations(Configuration config);

   Computation<S> lastComputation();

   Computation<S> nextComputation();

   // {{{ runtime
   Primitive primitive();

   Primitive[] primitives();

   void process(S Subgraph);

   long processCompute(SubgraphEnumerator<S> expansions);

   // java computations
   void processExtensions();

   int setDepth(int depth);

   void setSubgraph(S subgraph);

   boolean shouldBypass();
}
