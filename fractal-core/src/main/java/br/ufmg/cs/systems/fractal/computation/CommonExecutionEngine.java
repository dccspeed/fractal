package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.aggregation.AggregationStorage;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public interface CommonExecutionEngine<O extends Subgraph> {

    <A extends Writable> A getAggregatedValue(String name);
    
    <K extends Writable, V extends Writable> AggregationStorage<K, V> getAggregationStorage(String name);

    <K extends Writable, V extends Writable> void map(String name, K key, V value);
    
    int getPartitionId();

    int getNumberPartitions();

    long getStep();

    void aggregate(String name, LongWritable value);
    
    void aggregate(String name, long value);
    
    void output(Subgraph subgraph);

}
