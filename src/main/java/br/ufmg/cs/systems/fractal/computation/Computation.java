package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.aggregation.AggregationStorage;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import com.koloboke.collect.IntCollection;
import org.apache.hadoop.io.Writable;

import java.util.Iterator;

public interface Computation<E extends Subgraph> {
    // {{{ Initialization, computation and finish hooks
    void init(Configuration<E> config);

    void initAggregations(Configuration<E> config);

    long compute(E Subgraph);

    Computation<E> nextComputation();

    void finish();
    // }}}

    // {{{
    // |
    // |- compute
    //    |
    //    |- expandCompute
    //    |
    //    |- processCompute
    //       |- filter
    //       |- process

    Iterator<E> expandCompute(E Subgraph);
    IntCollection getPossibleExtensions(E Subgraph);
    long processCompute(Iterator<E> expansions);
    boolean filter(E Subgraph);
    void process(E Subgraph);
    // }}}

    boolean filter(E existingSubgraph, int newWord);
    // }}}

    // {{{ Output
    void output(E Subgraph);
    // }}}

    // {{{ Aggregation-related stuff
    <K extends Writable, V extends Writable>
       AggregationStorage<K, V> readAggregation(String name);
    
    <K extends Writable, V extends Writable> 
       AggregationStorage<K, V> getAggregationStorage(String name);

    <K extends Writable, V extends Writable>
       void map(String name, K key, V value);
    // }}}

    // {{{ Misc
    int getStep();

    int getPartitionId();

    int getNumberPartitions();

    Configuration<E> getConfig();

  SubgraphEnumerator<E> bypass(E subgraph);
    // }}}

    // {{{ Internal
    void setExecutionEngine(CommonExecutionEngine<E> executionEngine);
    CommonExecutionEngine<E> getExecutionEngine();
    
    String computationLabel();
    int setDepth(int depth);
    int getDepth();

    SubgraphEnumerator<E> getSubgraphEnumerator();
    SubgraphEnumerator<E> extend();
    void joinConsumer(SubgraphEnumerator<E> consumer);

    Class<? extends Subgraph> getSubgraphClass();
    
    int getInitialNumWords();

    boolean containsWord(int wordId);

    Pattern getPattern();
    // }}}
}
