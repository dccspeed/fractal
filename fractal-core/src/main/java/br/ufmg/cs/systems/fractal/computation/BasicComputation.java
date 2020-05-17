package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.aggregation.AggregationStorage;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import com.koloboke.collect.IntCollection;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public abstract class BasicComputation<S extends Subgraph>
      implements Computation<S>, java.io.Serializable {
    
    private static final Logger LOG = Logger.getLogger(BasicComputation.class);

    private transient int depth;
    
    private transient SubgraphEnumerator<S> subgraphEnumerator;

    private transient CommonExecutionEngine<S> executionEngine;
    private transient MainGraph mainGraph;
    private transient Configuration configuration;

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
    public void init(Configuration<S> config) {
        configuration = config;
        mainGraph = configuration.getMainGraph();
        subgraphEnumerator = configuration.createSubgraphEnumerator(this);
    }

    @Override
    public void initAggregations(Configuration<S> config) {
        configuration = config;
    }

    @Override
    public final long compute(S subgraph) {
      //subgraphEnumerator.set(this, subgraph);
      return processCompute(expandCompute(subgraph));
    }

    @Override
    public Computation<S> nextComputation() {
       return null;
    }

    @Override
    public SubgraphEnumerator<S> expandCompute(S subgraph) {
      subgraphEnumerator.computeExtensions();
      return subgraphEnumerator;
    }

    @Override
    public boolean shouldBypass() {
       return false;
    }

    @Override
    public IntCollection getPossibleExtensions(S subgraph) {
       return subgraph.computeExtensions(this);
    }

   @Override
    public <K extends Writable, V extends Writable>
    AggregationStorage<K, V> readAggregation(String name) {
        return executionEngine.getAggregatedValue(name);
    }
    
    @Override
    public <K extends Writable, V extends Writable>
    AggregationStorage<K, V> getAggregationStorage(String name) {
        return executionEngine.getAggregationStorage(name);
    }

    @Override
    public <K extends Writable, V extends Writable>
    void map(String name, K key, V value) {
        executionEngine.map(name, key, value);
    }

    @Override
    public int getPartitionId() {
        return executionEngine.getPartitionId();
    }

    @Override
    public int getNumberPartitions() {
        return executionEngine.getNumberPartitions();
    }

    @Override
    public final int getStep() {
        return (int) executionEngine.getStep();
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
    public SubgraphEnumerator<S> forkEnumerator(Computation<S> computation) {
       Computation<S> thisCurr = this;
       Computation<S> thatCurr = computation;
       SubgraphEnumerator<S> consumer = null;

       while (thisCurr != null && thisCurr.nextComputation() != null) {
          if (thisCurr.getSubgraphEnumerator() != null &&
                thisCurr.getSubgraphEnumerator().isActive()) {
             consumer = thisCurr.getSubgraphEnumerator().forkEnumerator(thatCurr);
             if (consumer.hasNext()) {
                return consumer;
             }
          }
          thisCurr = thisCurr.nextComputation();
          thatCurr = thatCurr.nextComputation();
      }

      return null;
    }

    @Override
    public void finish() {
    }

    @Override
    public final void output(S subgraph) {
       executionEngine.output(subgraph);
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
