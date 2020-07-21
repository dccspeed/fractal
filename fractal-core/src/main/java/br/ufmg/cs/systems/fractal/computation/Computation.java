package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.aggregation.AggregationStorage;
import br.ufmg.cs.systems.fractal.aggregation.SubgraphAggregation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import org.apache.hadoop.io.Writable;

import java.io.Serializable;

public interface Computation<S extends Subgraph> extends Serializable {

    // {{{ initialization
    void init(Configuration config);

   void init(CommonExecutionEngine<S> engine, Configuration config);

   void initAggregations(Configuration config);
    long compute(S Subgraph);

   void processExtensions();

   Computation<S> nextComputation();
   // }}}

    // {{{ runtime
    Primitive primitive();
    Primitive[] primitives();

   long processCompute(SubgraphEnumerator<S> expansions);
    boolean filter(S Subgraph);
    void process(S Subgraph);
    // }}}

   // }}}

   // }}}

    // {{{ Misc
    int getStep();

   SubgraphAggregation<S> getSubgraphAggregation();

   int getPartitionId();

    int getNumberPartitions();

    Configuration getConfig();

    boolean shouldBypass();
    // }}}

   Computation<S> lastComputation();

   // {{{ Internal
    void setExecutionEngine(CommonExecutionEngine<S> executionEngine);
    CommonExecutionEngine<S> getExecutionEngine();
    
    String computationLabel();
    int setDepth(int depth);
    int getDepth();

    SubgraphEnumerator<S> getSubgraphEnumerator();

   Class<? extends Subgraph> getSubgraphClass();
    
    int getInitialNumWords();

    boolean containsWord(int wordId);

    Pattern getPattern();
    // }}}
}
