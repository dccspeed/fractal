package br.ufmg.cs.systems.fractal.computation;

import akka.actor.ActorRef;
import br.ufmg.cs.systems.fractal.Primitive;
import br.ufmg.cs.systems.fractal.aggregation.SubgraphAggregation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

public interface ExecutionEngine<S extends Subgraph> {
   Configuration getConfig();

   Computation<S> getComputation();

   int getPartitionId();

   int getStageId();

   int getStep();

   SubgraphAggregation<S> getSubgraphAggregation();

   int numPartitions();

   Primitive[] primitives();

   ActorRef slaveActor();

   long getComputationTimeStart();
   long getComputationTimeEnd();

   long getInitTimeStart();
   long getInitTimeEnd();

   long getComputationWorkStealingTimeStart();
   long getComputationWorkStealingTimeEnd();

   ExecutionEngine getNextEngine();

   ExecutionEngine getPreviousEngine();

   void terminate();

   void initialWorkCompute();
}
