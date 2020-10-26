package br.ufmg.cs.systems.fractal.computation;

import akka.actor.ActorRef;
import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.aggregation.SubgraphAggregation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

public interface CommonExecutionEngine<S extends Subgraph> {
    int getPartitionId();

    int numPartitions();

    int getStep();

    int getStageId();

    SubgraphAggregation<S> getSubgraphAggregation();

    void addValidSubgraphs(long n);

    void addValidSubgraphs(int depth, long n);

    Primitive[] primitives();

    Configuration getConfig();

    ActorRef slaveActor();
}
