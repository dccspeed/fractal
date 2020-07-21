package br.ufmg.cs.systems.fractal.computation;

import akka.actor.ActorRef;
import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.aggregation.SubgraphAggregation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import org.apache.hadoop.io.LongWritable;

public interface CommonExecutionEngine<S extends Subgraph> {
    int getPartitionId();

    int numPartitions();

    int getStep();

    int getStageId();

    void aggregate(String name, LongWritable value);
    
    void aggregate(String name, long value);

    SubgraphAggregation<S> getSubgraphAggregation();

    void addValidSubgraphs(long n);

    Primitive[] primitives();

    Configuration getConfig();

    ActorRef slaveActor();
}
