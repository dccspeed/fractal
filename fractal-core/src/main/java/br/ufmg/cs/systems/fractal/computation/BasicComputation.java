package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.aggregation.SubgraphAggregation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import org.apache.log4j.Logger;

public abstract class BasicComputation<S extends Subgraph> implements Computation<S> {

   private static final Logger LOG = Logger.getLogger(BasicComputation.class);

   protected transient int depth;

   protected transient SubgraphEnumerator<S> subgraphEnumerator;
   protected transient S subgraph;

   protected transient CommonExecutionEngine<S> executionEngine;
   protected transient MainGraph mainGraph;
   protected transient Configuration configuration;

   protected transient Computation<S> lastComputation;

   /* Characterization stats */

   // basic counters
   private long validSubgraphs;
   private long canonicalSubgraphs;
   private long expansionCandidates;

   // compute extensions time
   private long totalComputeExtensionsTime;
   private double computeExtensionsMin = Double.MAX_VALUE;
   private double computeExtensionsMax = Double.MIN_VALUE;
   private double computeExtensionsNumSamples;
   private double computeExtensionsRunningMean;
   private double computeExtensionsRunningM2;

   public MainGraph getMainGraph() {
      return mainGraph;
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
   public void init(CommonExecutionEngine<S> engine, Configuration config) {
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
   }   @Override
   public final void setExecutionEngine(CommonExecutionEngine<S> executionEngine) {
      this.executionEngine = executionEngine;
   }

   @Override
   public long compute(S subgraph) {
      if (Configuration.OPCOUNTER_ENABLED) {
         long start = System.nanoTime();
         subgraphEnumerator.computeExtensions();
         double elapsedMs = (System.nanoTime() - start) * 1e-6;
         totalComputeExtensionsTime += elapsedMs;
         computeExtensionsMax = Math.max(computeExtensionsMax, elapsedMs);
         computeExtensionsMin = Math.min(computeExtensionsMin, elapsedMs);
         if (++computeExtensionsNumSamples == 1) {
            computeExtensionsRunningMean = elapsedMs;
            computeExtensionsRunningM2 = 0;
         } else {
            double d1 = elapsedMs - computeExtensionsRunningMean;
            computeExtensionsRunningMean += d1 / computeExtensionsNumSamples;
            double d2 = elapsedMs - computeExtensionsRunningMean;
            computeExtensionsRunningM2 += d1 * d2;
         }

      } else {
         subgraphEnumerator.computeExtensions();
      }

      return processCompute(subgraphEnumerator);
   }   @Override
   public final CommonExecutionEngine<S> getExecutionEngine() {
      return this.executionEngine;
   }

   @Override
   public Computation<S> nextComputation() {
      return null;
   }

   @Override
   public boolean filter(S subgraph) {
      return true;
   }

   @Override
   public void process(S subgraph) {
      // Empty by default
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
   public int getPartitionId() {
      return executionEngine.getPartitionId();
   }

   @Override
   public int getNumberPartitions() {
      return executionEngine.numPartitions();
   }   @Override
   public void processExtensions() {

   }

   @Override
   public Configuration getConfig() {
      return configuration;
   }   @Override
   public void computeAndProcessExtensions() {

   }

   @Override
   public boolean shouldBypass() {
      return false;
   }

   @Override
   public double getComputeExtensionsMax() {
      return computeExtensionsMax;
   }

   @Override
   public double getComputeExtensionsMin() {
      return computeExtensionsMin;
   }

   @Override
   public Computation<S> lastComputation() {
      return lastComputation;
   }









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

   /**
    * Stats counters
    **/

   @Override
   public long getValidSubgraphs() {
      return validSubgraphs;
   }

   @Override
   public void setValidSubgraphs(long validSubgraphs) {
      this.validSubgraphs = validSubgraphs;
   }

   @Override
   public void addValidSubgraphs(long inc) {
      this.validSubgraphs += inc;
   }

   @Override
   public long getCanonicalSubgraphs() {
      return canonicalSubgraphs;
   }

   @Override
   public void setCanonicalSubgraphs(long canonicalSubgraphs) {
      this.canonicalSubgraphs = canonicalSubgraphs;
   }

   @Override
   public void addCanonicalSubgraphs(long inc) {
      this.canonicalSubgraphs += inc;
   }

   @Override
   public long getExpansionCandidates() {
      return expansionCandidates;
   }

   @Override
   public void setExpansionCandidates(long expansionCandidates) {
      this.expansionCandidates = expansionCandidates;
   }

   @Override
   public void addExpansionNeighborhood(IntArrayList extensionCandidates) {
      this.expansionCandidates += subgraph.computeExtensionCost(extensionCandidates);
   }

   @Override
   public long getTotalComputeExtensionsTime() {
      return totalComputeExtensionsTime;
   }

   @Override
   public double getComputeExtensionsNumSamples() {
      return computeExtensionsNumSamples;
   }

   @Override
   public double getComputeExtensionsRunningM2() {
      return computeExtensionsRunningM2;
   }

   @Override
   public double getComputeExtensionsRunningMean() {
      return computeExtensionsRunningMean;
   }

   @Override
   public Pattern getPattern() {
      return null;
   }

   @Override
   public void setSubgraph(S subgraph) {
      this.subgraph = subgraph;
   }
}
