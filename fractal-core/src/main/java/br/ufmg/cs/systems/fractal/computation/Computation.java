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

   long getInternalWorkSteals();

   long getExternalWorkSteals();

   void addInternalWorkSteals(long inc);

   void addExternalWorkSteals(long inc);

   void compute();

   boolean filter_FILTERING_PRIMITIVE(S Subgraph);

   long getNumCanonicalExtensions();

   Configuration getConfig();

   int getDepth();

   ExecutionEngine<S> getExecutionEngine();

   Class<? extends SubgraphEnumerator<S>> getSubgraphEnumeratorClass();

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

   int setDepth(int depth);

   void setSubgraph(S subgraph);
}
