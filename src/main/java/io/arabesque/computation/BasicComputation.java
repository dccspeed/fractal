package io.arabesque.computation;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.graph.MainGraph;
import io.arabesque.pattern.Pattern;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import com.koloboke.function.IntConsumer;

import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public abstract class BasicComputation<E extends Embedding> 
      implements Computation<E>, java.io.Serializable {
    
    class EmbeddingIterator implements Iterator<E> {

       private E embedding;

       private IntCursor cur;

       private boolean shouldRemoveLastWord;

       public Iterator<E> set(E embedding, IntCollection wordIds) {
          this.embedding = embedding;
          this.cur = wordIds.cursor();
          this.shouldRemoveLastWord = false;
          return this;
       }

       @Override
       public boolean hasNext() {
          // this test is to make sure we do not remove the last word in the
          // first *hasNext* call
          if (shouldRemoveLastWord) {
             embedding.removeLastWord();
          } else {
             shouldRemoveLastWord = true;
          }

          // skip extensions that turn the embedding not canonical
          while (cur.moveNext()) {
             if (filter(embedding, cur.elem()))
                return true;
          }

          return false;
       }

       @Override
       public E next() {
          embedding.addWord(cur.elem());
          return embedding;
       }

       @Override
       public void remove() {
          throw new UnsupportedOperationException();
       }
    }
   
    private EmbeddingIterator emptyIter;
    private EmbeddingIterator embeddingIterator;

    private static final Logger LOG = Logger.getLogger(BasicComputation.class);

    private boolean outputEnabled;

    private CommonExecutionEngine<E> executionEngine;
    private MainGraph mainGraph;
    private Configuration configuration;
    private IntConsumer expandConsumer;
    private long numChildrenEvaluated = 0;
    private E currentEmbedding;

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
        numChildrenEvaluated = 0;

        outputEnabled = configuration.isOutputActive();

        emptyIter = new EmbeddingIterator() {
           @Override
           public boolean hasNext() {
              return false;
           }
        };

        embeddingIterator = new EmbeddingIterator();
    }

    @Override
    public void initAggregations(Configuration<E> config) {
        configuration = config;
    }

    @Override
    public final void compute(E embedding) {
       if (!aggregationCompute(embedding)) {
          return;
       }

       processCompute(expandCompute(embedding));
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

        if (possibleExtensions == null || possibleExtensions.isEmpty()) {
            handleNoExpansions(embedding);
            return emptyIter;
        }
       
        currentEmbedding = embedding;
        return embeddingIterator.set(embedding, possibleExtensions);
    }

    @Override
    public void processCompute(Iterator<E> expansionsIter) {
       Computation<E> nextComp = nextComputation();

       if (nextComp != null) {
          nextComp.setExecutionEngine (getExecutionEngine());
          nextComp.init(getConfig());
          nextComp.initAggregations(getConfig());
          
          while (expansionsIter.hasNext()) {
             currentEmbedding = expansionsIter.next();
             if (filter(currentEmbedding)) {
                numChildrenEvaluated++;
                process(currentEmbedding);
                currentEmbedding.nextEntensionLevel();
                nextComp.compute(currentEmbedding);
                currentEmbedding.previousExtensionLevel();
             }
          }

       } else {
          while (expansionsIter.hasNext()) {
             currentEmbedding = expansionsIter.next();
             if (filter(currentEmbedding)) {
                if (shouldExpand(currentEmbedding)) {
                   executionEngine.
                      processExpansion(currentEmbedding);
                }
                numChildrenEvaluated++;
                process(currentEmbedding);
             }
          }
       }
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

                numChildrenEvaluated++;
                process(currentEmbedding);
            }

            currentEmbedding.removeLastWord();
        }

    }

    @Override
    public void handleNoExpansions(E embedding) {
        // Empty by default
    }

    private IntCollection getPossibleExtensions(E embedding) {
        if (embedding.getNumWords() > 0) {
            return embedding.getExtensibleWordIds();
        } else {
            // TODO: put getInitialExtensions into embedding class
            return getInitialExtensions();
        }
    }

    protected HashIntSet getInitialExtensions() {
        int totalNumWords = getInitialNumWords();
        int numPartitions = getNumberPartitions();
        int myPartitionId = getPartitionId();
        int numWordsPerPartition = Math.max(totalNumWords / numPartitions, 1);
        int startMyWordRange = myPartitionId * numWordsPerPartition;
        int endMyWordRange = startMyWordRange + numWordsPerPartition;

        // If we are the last partition or our range end goes over the total
        // number of vertices, set the range end to the total number of vertices
        if (myPartitionId == numPartitions - 1 ||
              endMyWordRange > totalNumWords) {
            endMyWordRange = totalNumWords;
        }

        // TODO: Replace this by a list implementing IntCollection.
        // No need for set.
        HashIntSet initialExtensions = HashIntSets.newMutableSet(
              numWordsPerPartition);

        for (int i = startMyWordRange; i < endMyWordRange; ++i) {
            initialExtensions.add(i);
        }

        return initialExtensions;
    }

    protected abstract int getInitialNumWords();

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
        // When we achieve steps that reach long values, the universe
        // will probably have ended anyway
        // ... that's true, doesn't matter
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
    public void finish() {
        LongWritable longWritable = new LongWritable();
        LOG.info("Num children evaluated: " + numChildrenEvaluated);
    }

    @Override
    public final void output(E embedding) {
       executionEngine.output(embedding);
    }
}
