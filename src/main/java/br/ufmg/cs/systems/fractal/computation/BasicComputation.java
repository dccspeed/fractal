package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.aggregation.AggregationStorage;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import com.koloboke.collect.IntCollection;
import com.koloboke.function.IntConsumer;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.util.Iterator;

public abstract class BasicComputation<E extends Subgraph>
      implements Computation<E>, java.io.Serializable {
    
    private static final Logger LOG = Logger.getLogger(BasicComputation.class);

    private transient int depth;
    
    private transient SubgraphEnumerator<E> emptyIter;
    
    private transient SubgraphEnumerator<E> bypassIter;
    
    private transient SubgraphEnumerator<E> SubgraphEnumerator;

    private transient CommonExecutionEngine<E> executionEngine;
    private transient MainGraph mainGraph;
    private transient Configuration configuration;
    private transient IntConsumer expandConsumer;
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
        expandConsumer = new IntConsumer() {
            @Override
            public void accept(int wordId) {
                doExpandFilter(wordId);
            }
        };

        configuration = config;
        mainGraph = configuration.getMainGraph();

        emptyIter = new SubgraphEnumerator<E>() {
           @Override
           public boolean hasNext() {
              return false;
           }
        };

        bypassIter = new SubgraphEnumerator<E>() {
           private boolean hasNext = true;
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

        SubgraphEnumerator = new SubgraphEnumerator<E>();
    }

    @Override
    public void initAggregations(Configuration<E> config) {
        configuration = config;
    }

    @Override
    public final long compute(E Subgraph) {
       if (!aggregationCompute(Subgraph)) {
          return 0;
       }

       long ret = processCompute(expandCompute(Subgraph));
       // finish();
       return ret;
    }

    @Override
    public Computation<E> nextComputation() {
       return null;
    }

    @Override
    public boolean aggregationCompute(E Subgraph) {
        if (getStep() > 0) {
            if (!aggregationFilter(Subgraph)) {
                return false;
            }

            aggregationProcess(Subgraph);
        }
        return true;
    }

    @Override
    public Iterator<E> expandCompute(E Subgraph) {
        IntCollection possibleExtensions = getPossibleExtensions(Subgraph);
        
        if (possibleExtensions != null) {
            filter(Subgraph, possibleExtensions);
        }

        if (possibleExtensions == null) {
            handleNoExpansions(Subgraph);
            return emptyIter;
        }
        
        //if (possibleExtensions.isEmpty()) {
        //   //if (getConfig().shouldKeepMaximal()) {
        //   //   return bypassIter.set(this, subgraph, possibleExtensions);
        //   //} else {
        //      handleNoExpansions(subgraph);
        //      return emptyIter;
        //   //}
        //}
       
        currentSubgraph = Subgraph;
        return SubgraphEnumerator.set(this, Subgraph, possibleExtensions);
    }

    @Override
    public long processCompute(Iterator<E> expansionsIter) {
       Computation<E> nextComp = nextComputation();

       if (nextComp != null) {
          nextComp.setExecutionEngine (getExecutionEngine());
          nextComp.init(getConfig());
          nextComp.initAggregations(getConfig());

          while (expansionsIter.hasNext()) {
             currentSubgraph = expansionsIter.next();
             if (filter(currentSubgraph)) {
                process(currentSubgraph);

                currentSubgraph.nextExtensionLevel();
                nextComp.compute(currentSubgraph);
                currentSubgraph.previousExtensionLevel();
             }
          }

       } else {
          while (expansionsIter.hasNext()) {
             currentSubgraph = expansionsIter.next();
             if (filter(currentSubgraph)) {
                if (shouldExpand(currentSubgraph)) {
                   executionEngine.processExpansion(currentSubgraph);
                }
                process(currentSubgraph);
             }
          }
       }

       return 0;
    }

    @Override
    public void expand(E Subgraph) {
        if (getStep() > 0) {
            if (!aggregationFilter(Subgraph)) {
                return;
            }

            aggregationProcess(Subgraph);
        }

        IntCollection possibleExtensions = getPossibleExtensions(Subgraph);
        
        if (possibleExtensions != null) {
            filter(Subgraph, possibleExtensions);
        }

        if (possibleExtensions == null || possibleExtensions.isEmpty()) {
            handleNoExpansions(Subgraph);
            return;
        }

        currentSubgraph = Subgraph;
        possibleExtensions.forEach(expandConsumer);
    }

    private void doExpandFilter(int wordId) {
        if (filter(currentSubgraph, wordId)) {
            currentSubgraph.addWord(wordId);

            if (filter(currentSubgraph)) {
                if (shouldExpand(currentSubgraph)) {
                    executionEngine.
                       processExpansion(currentSubgraph);
                }

                process(currentSubgraph);
            }

            currentSubgraph.removeLastWord();
        }

    }

    @Override
    public void handleNoExpansions(E Subgraph) {
        // Empty by default
    }

    @Override
    public IntCollection getPossibleExtensions(E Subgraph) {
       return Subgraph.getExtensibleWordIds(this);
    }

    @Override
    public boolean shouldExpand(E Subgraph) {
        return true;
    }

    @Override
    public void filter(E existingSubgraph, IntCollection extensionPoints) {
        // Do nothing by default
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
    public boolean aggregationFilter(E Subgraph) {
        return true;
    }

    @Override
    public boolean aggregationFilter(Pattern pattern) {
        return true;
    }

    @Override
    public void aggregationProcess(E Subgraph) {
        // Empty by default
    }

    @Override
    public String computationLabel() {
       return null;
    }

    public SubgraphEnumerator<E> getSubgraphEnumerator() {
       return this.SubgraphEnumerator;
    }

    @Override
    public SubgraphEnumerator<E> forkConsumer(boolean local) {
       BasicComputation<E> curr = this;
       SubgraphEnumerator<E> consumer = null;

       while (curr != null && curr.nextComputation() != null) {
          if (curr.SubgraphEnumerator != null &&
                curr.SubgraphEnumerator.isActive()) {
             consumer = curr.SubgraphEnumerator.forkConsumer(local);
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
