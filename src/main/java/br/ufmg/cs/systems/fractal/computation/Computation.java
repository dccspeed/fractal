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

    // {{{ Filter-Process model
    // |
    // |- compute
    //    |
    //    |- aggregationCompute
    //    |  |- aggregationFilter
    //    |  |- aggregationProcess
    //    |
    //    |- expandCompute
    //    |  |- handleNoExpansions
    //    |
    //    |- processCompute
    //       |- filter
    //       |- process
    //       |- shouldExpand
   
    boolean aggregationCompute(E Subgraph);
    boolean aggregationFilter(E Subgraph);
    boolean aggregationFilter(Pattern pattern);
    void aggregationProcess(E Subgraph);
   
    Iterator<E> expandCompute(E Subgraph);
    IntCollection getPossibleExtensions(E Subgraph);
    void handleNoExpansions(E Subgraph);

    long processCompute(Iterator<E> expansions);
    boolean filter(E Subgraph);
    void process(E Subgraph);
    boolean shouldExpand(E newSubgraph);
    // }}}

    // {{{ Other filter-hooks (performance/canonicality related)
    void filter(E existingSubgraph, IntCollection extensionPoints);

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

    E getCurrentSubgraph();
    // }}}

    // {{{ Internal
    void setExecutionEngine(CommonExecutionEngine<E> executionEngine);
    CommonExecutionEngine<E> getExecutionEngine();
    
    String computationLabel();
    int setDepth(int depth);
    int getDepth();

    SubgraphEnumerator<E> forkConsumer(boolean local);
    void joinConsumer(SubgraphEnumerator<E> consumer);

    void expand(E Subgraph);

    Class<? extends Subgraph> getSubgraphClass();
    
    int getInitialNumWords();

    Pattern getPattern();
    // }}}
}
