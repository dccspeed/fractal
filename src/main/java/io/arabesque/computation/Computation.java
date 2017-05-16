package io.arabesque.computation;

import com.koloboke.collect.IntCollection;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.pattern.Pattern;

import java.util.Iterator;

import org.apache.hadoop.io.Writable;


public interface Computation<E extends Embedding> {
    // {{{ Initialization, computation and finish hooks
    void init(Configuration<E> config);

    void initAggregations(Configuration<E> config);

    void compute(E embedding);

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
   
    boolean aggregationCompute(E embedding);
    boolean aggregationFilter(E Embedding);
    boolean aggregationFilter(Pattern pattern);
    void aggregationProcess(E embedding);
   
    Iterator<E> expandCompute(E embedding);
    void handleNoExpansions(E embedding);

    void processCompute(Iterator<E> expansions);
    boolean filter(E embedding);
    void process(E embedding);
    boolean shouldExpand(E newEmbedding);
    // }}}

    // {{{ Other filter-hooks (performance/canonicality related)
    void filter(E existingEmbedding, IntCollection extensionPoints);

    boolean filter(E existingEmbedding, int newWord);
    // }}}

    // {{{ Output
    void output(E embedding);
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
    // }}}

    // {{{ Internal
    void setExecutionEngine(CommonExecutionEngine<E> executionEngine);
    CommonExecutionEngine<E> getExecutionEngine();

    void expand(E embedding);

    Class<? extends Embedding> getEmbeddingClass();
    // }}}
}
