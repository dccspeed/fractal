package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.aggregation.AggregationStorage;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import com.koloboke.collect.IntCollection;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.util.Iterator;

public abstract class BasicComputation<E extends Subgraph>
      implements Computation<E>, java.io.Serializable {
    
    private static final Logger LOG = Logger.getLogger(BasicComputation.class);

    private transient int depth;
    
    private transient SubgraphEnumerator<E> emptyIter;
    
    private transient SubgraphEnumerator<E> bypassIter;
    
    private transient SubgraphEnumerator<E> subgraphEnumerator;

    private transient CommonExecutionEngine<E> executionEngine;
    private transient MainGraph mainGraph;
    private transient Configuration configuration;
    private transient E currentSubgraph;

    @Override
    public final void setExecutionEngine(
          CommonExecutionEngine<E> executionEngine) {
        this.executionEngine = executionEngine;
    }

    @Override
    public final CommonExecutionEngine<E> getExecutionEngine() {
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
    public void init(Configuration<E> config) {
        configuration = config;
        mainGraph = configuration.getMainGraph();

        emptyIter = new SubgraphEnumerator<E>() {
           @Override
           public boolean hasNext() {
              return false;
           }
        };

        bypassIter = new SubgraphEnumerator<E>() {
           private boolean hasNext;

           @Override
           public synchronized SubgraphEnumerator<E> set(
                 Computation<E> computation, E Subgraph) {
              this.computation = computation;
              this.Subgraph = Subgraph;
              this.hasNext = true;
              return this;
           }

           @Override
           public boolean hasNext() {
              return hasNext;
           }

           @Override
           public E next() {
              hasNext = false;
              return Subgraph;
           }
        };

        subgraphEnumerator = new SubgraphEnumerator<E>();
    }

    @Override
    public void initAggregations(Configuration<E> config) {
        configuration = config;
    }

    @Override
    public final long compute(E subgraph) {
       return processCompute(expandCompute(subgraph));
    }

    @Override
    public Computation<E> nextComputation() {
       return null;
    }

    @Override
    public Iterator<E> expandCompute(E subgraph) {
        IntCollection possibleExtensions = getPossibleExtensions(subgraph);
        
        if (possibleExtensions == null) {
            return emptyIter;
        }
        
        if (possibleExtensions.isEmpty() && getConfig().keepMaximal()) {
          return bypass(subgraph);
        }
       
        currentSubgraph = subgraph;
        return subgraphEnumerator.set(this, subgraph, possibleExtensions);
    }

    @Override
    public IntCollection getPossibleExtensions(E Subgraph) {
       return Subgraph.getExtensibleWordIds(this);
    }

  @Override
    public boolean filter(E existingSubgraph, int newWord) {
        return existingSubgraph.isCanonicalSubgraphWithWord(newWord);
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
        return (int) executionEngine.getSuperstep();
    }

    @Override
    public void process(E Subgraph) {
       // Empty by default
    }

    @Override
    public boolean filter(E newSubgraph) {
        return true;
    }

  @Override
    public String computationLabel() {
       return null;
    }

    @Override
    public SubgraphEnumerator<E> bypass(E subgraph) {
       return bypassIter.set(this, subgraph);
    }

    public SubgraphEnumerator<E> getSubgraphEnumerator() {
       return this.subgraphEnumerator;
    }

    @Override
    public SubgraphEnumerator<E> forkEnumerator(boolean local) {
       BasicComputation<E> curr = this;
       SubgraphEnumerator<E> consumer = null;

       while (curr != null && curr.nextComputation() != null) {
          if (curr.subgraphEnumerator != null &&
                curr.subgraphEnumerator.isActive()) {
             consumer = curr.subgraphEnumerator.forkConsumer(local);
             if (consumer.hasNext()) {
                return consumer;
             } else {
                consumer.joinConsumer();
             }
          }
         curr = (BasicComputation<E>) curr.nextComputation();
      }

       return null;
    }

    @Override
    public void joinConsumer(SubgraphEnumerator<E> consumer) {
       consumer.joinConsumer();
    }

    @Override
    public void finish() {
    }

    @Override
    public final void output(E Subgraph) {
       executionEngine.output(Subgraph);
    }

    @Override
    public final int setDepth(int depth) {
       this.depth = depth;
       Computation<E> nextComp = nextComputation();
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
    public E getCurrentSubgraph() {
       return currentSubgraph;
    }

    @Override
    public Pattern getPattern() {
       return null;
    }
}
