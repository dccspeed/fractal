package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.aggregation.AggregationStorage;
import br.ufmg.cs.systems.fractal.aggregation.SubgraphAggregation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import org.apache.hadoop.io.Writable;

import java.io.Serializable;

public interface Computation<S extends Subgraph> extends Serializable {

    void init(Configuration config);

   void init(CommonExecutionEngine<S> engine, Configuration config);

   void initAggregations(Configuration config);
    long compute(S Subgraph);


   Computation<S> nextComputation();

    // {{{ runtime
    Primitive primitive();
    Primitive[] primitives();

   long processCompute(SubgraphEnumerator<S> expansions);
    boolean filter(S Subgraph);
    void process(S Subgraph);
    int getStep();

   SubgraphAggregation<S> getSubgraphAggregation();

   int getPartitionId();

    int getNumberPartitions();

    Configuration getConfig();

   void setSubgraph(S subgraph);

   boolean shouldBypass();

   double getComputeExtensionsMax();

   double getComputeExtensionsMin();

   Computation<S> lastComputation();

    void setExecutionEngine(CommonExecutionEngine<S> executionEngine);
    CommonExecutionEngine<S> getExecutionEngine();
    
    String computationLabel();
    int setDepth(int depth);
    int getDepth();

    SubgraphEnumerator<S> getSubgraphEnumerator();

   Class<? extends Subgraph> getSubgraphClass();
    
    int getInitialNumWords();

    boolean containsWord(int wordId);

   long getValidSubgraphs();

   void setValidSubgraphs(long validSubgraphs);

   void addValidSubgraphs(long inc);

   long getCanonicalSubgraphs();

   long getExpansionCandidates();

   void setCanonicalSubgraphs(long canonicalSubgraphs);

   void setExpansionCandidates(long expansionCandidates);

   void addCanonicalSubgraphs(long inc);

   void addExpansionNeighborhood(IntArrayList extensionCandidates);

   long getTotalComputeExtensionsTime();

   double getComputeExtensionsNumSamples();

   double getComputeExtensionsRunningM2();

   double getComputeExtensionsRunningMean();

   Pattern getPattern();


    // java computations
   void processExtensions();
   void computeAndProcessExtensions();
}
