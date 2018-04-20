package io.arabesque.computation;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.graph.MainGraph;
import io.arabesque.pattern.Pattern;
import io.arabesque.utils.collection.IntArrayList;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import com.koloboke.function.IntConsumer;

import java.lang.Math;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import scala.Tuple2;

public abstract class BasicComputation<E extends Embedding> 
      implements Computation<E>, java.io.Serializable {
    
    private static final Logger LOG = Logger.getLogger(BasicComputation.class);

    private transient int depth;
    
    private transient EmbeddingIterator<E> emptyIter;
    
    private transient EmbeddingIterator<E> bypassIter;
    
    private transient EmbeddingIterator<E> embeddingIterator;

    private transient CommonExecutionEngine<E> executionEngine;
    private transient MainGraph mainGraph;
    private transient Configuration configuration;
    private transient IntConsumer expandConsumer;
    private transient E currentEmbedding;

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

        emptyIter = new EmbeddingIterator<E>() {
           @Override
           public boolean hasNext() {
              return false;
           }
        };

        bypassIter = new EmbeddingIterator<E>() {
           private boolean hasNext = true;
           @Override
           public boolean hasNext() {
              return hasNext;
           }

           @Override
           public E next() {
              hasNext = false;
              return embedding;
           }
        };

        embeddingIterator = new EmbeddingIterator<E>();
    }

    @Override
    public void initAggregations(Configuration<E> config) {
        configuration = config;
    }

    @Override
    public final long compute(E embedding) {
       if (!aggregationCompute(embedding)) {
          return 0;
       }

       long ret = processCompute(expandCompute(embedding));
       // finish();
       return ret;
    }

    @Override
    public Computation<E> nextComputation() {
       return null;
    }

    @Override
    public boolean aggregationCompute(E embedding) {
        if (getStep() > 0) {
            if (!aggregationFilter(embedding)) {
                return false;
            }

            aggregationProcess(embedding);
        }
        return true;
    }

    @Override
    public Iterator<E> expandCompute(E embedding) {
        IntCollection possibleExtensions = getPossibleExtensions(embedding);
        
        if (possibleExtensions != null) {
            filter(embedding, possibleExtensions);
        }

        if (possibleExtensions == null) {
            handleNoExpansions(embedding);
            return emptyIter;
        }
        
        if (possibleExtensions.isEmpty()) {
           //if (getConfig().shouldKeepMaximal()) {
           //   return bypassIter.set(this, embedding, possibleExtensions);
           //} else {
              handleNoExpansions(embedding);
              return emptyIter;
           //}
        }
       
        currentEmbedding = embedding;
        return embeddingIterator.set(this, embedding, possibleExtensions);
    }

    @Override
    public long processCompute(Iterator<E> expansionsIter) {
       Computation<E> nextComp = nextComputation();

       if (nextComp != null) {
          nextComp.setExecutionEngine (getExecutionEngine());
          nextComp.init(getConfig());
          nextComp.initAggregations(getConfig());

          while (expansionsIter.hasNext()) {
             currentEmbedding = expansionsIter.next();
             if (filter(currentEmbedding)) {
                process(currentEmbedding);

                currentEmbedding.nextExtensionLevel();
                nextComp.compute(currentEmbedding);
                currentEmbedding.previousExtensionLevel();
             }
          }

       } else {
          while (expansionsIter.hasNext()) {
             currentEmbedding = expansionsIter.next();
             if (filter(currentEmbedding)) {
                if (shouldExpand(currentEmbedding)) {
                   executionEngine.processExpansion(currentEmbedding);
                }
                process(currentEmbedding);
             }
          }
       }

       return 0;
    }

    @Override
    public void expand(E embedding) {
        if (getStep() > 0) {
            if (!aggregationFilter(embedding)) {
                return;
            }

            aggregationProcess(embedding);
        }

        IntCollection possibleExtensions = getPossibleExtensions(embedding);
        
        if (possibleExtensions != null) {
            filter(embedding, possibleExtensions);
        }

        if (possibleExtensions == null || possibleExtensions.isEmpty()) {
            handleNoExpansions(embedding);
            return;
        }

        currentEmbedding = embedding;
        possibleExtensions.forEach(expandConsumer);
    }

    private void doExpandFilter(int wordId) {
        if (filter(currentEmbedding, wordId)) {
            currentEmbedding.addWord(wordId);

            if (filter(currentEmbedding)) {
                if (shouldExpand(currentEmbedding)) {
                    executionEngine.
                       processExpansion(currentEmbedding);
                }

                process(currentEmbedding);
            }

            currentEmbedding.removeLastWord();
        }

    }

    @Override
    public void handleNoExpansions(E embedding) {
        // Empty by default
    }

    @Override
    public IntCollection getPossibleExtensions(E embedding) {
       return embedding.getExtensibleWordIds(this);
    }

    @Override
    public boolean shouldExpand(E embedding) {
        return true;
    }

    @Override
    public void filter(E existingEmbedding, IntCollection extensionPoints) {
        // Do nothing by default
    }

    @Override
    public boolean filter(E existingEmbedding, int newWord) {
        return existingEmbedding.isCanonicalEmbeddingWithWord(newWord);
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
    public void process(E embedding) {
       // Empty by default
    }

    @Override
    public boolean filter(E newEmbedding) {
        return true;
    }

    @Override
    public boolean aggregationFilter(E Embedding) {
        return true;
    }

    @Override
    public boolean aggregationFilter(Pattern pattern) {
        return true;
    }

    @Override
    public void aggregationProcess(E embedding) {
        // Empty by default
    }

    @Override
    public String computationLabel() {
       return null;
    }

    public EmbeddingIterator<E> getEmbeddingIterator() {
       return this.embeddingIterator;
    }

    @Override
    public EmbeddingIterator<E> forkConsumer(boolean local) {
       BasicComputation<E> curr = this;
       EmbeddingIterator<E> consumer = null;

       while (curr != null) {
          if (curr.embeddingIterator != null &&
                curr.embeddingIterator.isActive()) {
             consumer = curr.embeddingIterator.forkConsumer(local);
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
    public void joinConsumer(EmbeddingIterator<E> consumer) {
       consumer.joinConsumer();
    }

    @Override
    public void finish() {
    }

    @Override
    public final void output(E embedding) {
       executionEngine.output(embedding);
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
    public E getCurrentEmbedding() {
       return currentEmbedding;
    }
}
