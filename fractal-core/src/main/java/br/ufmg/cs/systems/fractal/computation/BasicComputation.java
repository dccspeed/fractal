package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.aggregation.AggregationStorage;
import br.ufmg.cs.systems.fractal.aggregation.SubgraphAggregation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public abstract class BasicComputation<S extends Subgraph>
        implements Computation<S> {

   private static final Logger LOG = Logger.getLogger(BasicComputation.class);

   protected transient int depth;

   protected transient SubgraphEnumerator<S> subgraphEnumerator;

   protected transient CommonExecutionEngine<S> executionEngine;
   protected transient MainGraph mainGraph;
   protected transient Configuration configuration;

   protected transient Computation<S> lastComputation;

   @Override
   public Computation<S> lastComputation() {
      return lastComputation;
   }

   @Override
   public final void setExecutionEngine(
           CommonExecutionEngine<S> executionEngine) {
      this.executionEngine = executionEngine;
   }

   @Override
   public final CommonExecutionEngine<S> getExecutionEngine() {
      return this.executionEngine;
   }

   public MainGraph getMainGraph() {
      return mainGraph;
   }

   @Override
   public Configuration getConfig() {
      return configuration;
   }

   @Override
   public void init(Configuration config) {
      configuration = config;
      mainGraph = configuration.getMainGraph();
      subgraphEnumerator = configuration.createSubgraphEnumerator(this);
      lastComputation = this;
      while (lastComputation.nextComputation() != null) {
         lastComputation = lastComputation.nextComputation();
      }
   }

   @Override
   public void init(CommonExecutionEngine<S> engine,
                    Configuration config) {
      S subgraph = (S) config.createSubgraph(getSubgraphClass());

      Computation<S> currComp = this;
      while (currComp != null) {
         currComp.setExecutionEngine(engine);
         currComp.init(config);
         currComp.getSubgraphEnumerator().set(currComp, subgraph);
         currComp.initAggregations(config);
         currComp = currComp.nextComputation();
      }

      setDepth(0);
   }

   @Override
   public void initAggregations(Configuration config) {
      configuration = config;
   }

   @Override
   public long compute(S subgraph) {
      subgraphEnumerator.computeExtensions();
      return processCompute(subgraphEnumerator);
   }

   @Override
   public void processExtensions() {

   }

   @Override
   public Computation<S> nextComputation() {
      return null;
   }

   @Override
   public boolean shouldBypass() {
      return false;
   }

   @Override
   public SubgraphAggregation<S> getSubgraphAggregation() {
      if (executionEngine != null) {
         return executionEngine.getSubgraphAggregation();
      } else {
         return null;
      }
   }

   @Override
   public int getPartitionId() {
      return executionEngine.getPartitionId();
   }

   @Override
   public int getNumberPartitions() {
      return executionEngine.numPartitions();
   }

   @Override
   public final int getStep() {
      return executionEngine.getStep();
   }

   @Override
   public void process(S subgraph) {
      // Empty by default
   }

   @Override
   public boolean filter(S subgraph) {
      return true;
   }

   @Override
   public String computationLabel() {
      return null;
   }

   @Override
   public SubgraphEnumerator<S> getSubgraphEnumerator() {
      return this.subgraphEnumerator;
   }

   @Override
   public final int setDepth(int depth) {
      this.depth = depth;
      Computation<S> nextComp = nextComputation();
      if (nextComp != null) {
         return 1 + nextComp.setDepth(this.depth + 1);
      } else {
         return 1;
      }
   }

   @Override
   public final int getDepth() {
      return this.depth;
   }

   @Override
   public Pattern getPattern() {
      return null;
   }
}
