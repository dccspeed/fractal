package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.aggregation.SubgraphAggregation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.ReflectionSerializationUtils;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import org.apache.log4j.Logger;

public abstract class BasicComputation<S extends Subgraph>
        implements Computation<S> {

   private static final Logger LOG = Logger.getLogger(BasicComputation.class);

   protected transient int depth;

   protected transient SubgraphEnumerator<S> subgraphEnumerator;
   protected transient S subgraph;

   protected transient ExecutionEngine<S> executionEngine;
   protected transient MainGraph mainGraph;
   protected transient Configuration configuration;
   protected transient Computation<S> nextComputation;
   protected transient Computation<S> previousComputation;
   protected transient Computation<S> lastComputation;

   // basic counters
   private transient long extensionUniqueCandidates;
   private transient long expansionCandidates;
   private transient long canonicalSubgraphs;
   private transient long validSubgraphs;
   private transient long internalWorkSteals;
   private transient long externalWorkSteals;
   private transient boolean forcedTermination;

   private transient boolean active;

   @Override
   public void setForcedTermination(boolean forcedTermination) {
      this.forcedTermination = forcedTermination;
   }

   @Override
   public boolean getForcedTermination() {
      return this.forcedTermination;
   }

   @Override
   public void addCanonicalSubgraphs(long inc) {
      this.canonicalSubgraphs += inc;
   }

   @Override
   public void addExpansionNeighborhood(IntArrayList extensionCandidates) {
      this.expansionCandidates +=
              subgraph.computeExtensionCost(extensionCandidates);
   }

   @Override
   public long getNumUniqueExtensions() {
      return extensionUniqueCandidates;
   }

   @Override
   public void addExtensionUniqueCandidates(long inc) {
      this.extensionUniqueCandidates += inc;
   }

   @Override
   public void addValidSubgraphs(long inc) {
      this.validSubgraphs += inc;
   }

   @Override
   public long getInternalWorkSteals() {
      return internalWorkSteals;
   }

   @Override
   public long getExternalWorkSteals() {
      return externalWorkSteals;
   }

   @Override
   public void addInternalWorkSteals(long inc) {
      this.internalWorkSteals += inc;
   }

   @Override
   public void addExternalWorkSteals(long inc) {
      this.externalWorkSteals += inc;
   }

   @Override
   public void compute() {
      subgraphEnumerator.computeExtensions_EXTENSION_PRIMITIVE();
      processCompute(subgraphEnumerator);
   }

   @Override
   public boolean filter_FILTERING_PRIMITIVE(S subgraph) {
      return true;
   }

   @Override
   public long getNumCanonicalExtensions() {
      return canonicalSubgraphs;
   }

   @Override
   public Configuration getConfig() {
      return configuration;
   }

   @Override
   public final int getDepth() {
      return this.depth;
   }

   @Override
   public final ExecutionEngine<S> getExecutionEngine() {
      return this.executionEngine;
   }

   @Override
   public final void setExecutionEngine(ExecutionEngine<S> executionEngine) {
      this.executionEngine = executionEngine;
   }

   @Override
   public long getNumExtensions() {
      return expansionCandidates;
   }

   @Override
   public int getNumberPartitions() {
      return executionEngine.numPartitions();
   }

   public int getPartitionId() {
      return executionEngine.getPartitionId();
   }

   @Override
   public Pattern getPattern() {
      return null;
   }

   @Override
   public void setPattern(Pattern pattern) {

   }

   @Override
   public final int getStep() {
      return executionEngine.getStep();
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
   public SubgraphEnumerator<S> getSubgraphEnumerator() {
      return this.subgraphEnumerator;
   }

   @Override
   public long getNumValidExtensions() {
      return validSubgraphs;
   }

   @Override
   public void init(Configuration config) {
      active = true;
      configuration = config;
      mainGraph = configuration.getMainGraph();
      subgraphEnumerator = ReflectionSerializationUtils.newInstance(
              getSubgraphEnumeratorClass()
      );
      subgraphEnumerator.init(config, this);
      nextComputation = nextComputation();
      lastComputation = this;
      while (lastComputation.nextComputation() != null) {
         Computation<S> currNextComputation = lastComputation.nextComputation();
         currNextComputation.setPreviousComputation(lastComputation);
         lastComputation = currNextComputation;
      }
   }

   @Override
   public void init(ExecutionEngine<S> engine, Configuration config) {
      S subgraph = (S) config.createSubgraph(getSubgraphClass());

      Computation<S> currComp = this;
      while (currComp != null) {
         currComp.setSubgraph(subgraph);
         currComp.setExecutionEngine(engine);
         currComp.init(config);
         currComp.getSubgraphEnumerator().setComputation(currComp);
         currComp.getSubgraphEnumerator().setSubgraph(subgraph);
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
   public Computation<S> lastComputation() {
      return lastComputation;
   }

   @Override
   public Computation<S> nextComputation() {
      return null;
   }

   @Override
   public final Computation<S> previousComputation() {
      return previousComputation;
   }

   @Override
   public final void setPreviousComputation(Computation<S> previousComputation) {
      this.previousComputation = previousComputation;
   }

   @Override
   public void process(S subgraph) {
      // Empty by default
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
   public void setSubgraph(S subgraph) {
      this.subgraph = subgraph;
   }

   @Override
   public void terminate() {
      setForcedTermination(true);
      this.active = false;
      getSubgraphEnumerator().terminate();
   }

   @Override
   public boolean isActive() {
      return active;
   }

   public MainGraph getMainGraph() {
      return mainGraph;
   }

   @Override
   public Class<? extends SubgraphEnumerator<S>> getSubgraphEnumeratorClass() {
      return null;
   }
}
