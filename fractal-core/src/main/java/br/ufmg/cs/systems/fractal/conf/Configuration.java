package br.ufmg.cs.systems.fractal.conf;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.graph.*;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.ReflectionSerializationUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;

public class Configuration implements Serializable {
   private static final Logger LOG = Logger.getLogger(Configuration.class);

   public static final boolean INSTRUMENTATION_ENABLED = false;
   public static final String CONF_COLLECT_THREAD_STATS =
           "fractal.collect.thread_stats";
   public static final boolean CONF_COLLECT_THREAD_STATS_DEFAULT = false;
   public static final String CONF_THREAD_STATS_KEY =
           "fractal.thread_stats.key";
   public static final String CONF_THREAD_STATS_KEY_DEFAULT = "";
   public static final String CONF_MASTER_HOSTNAME = "fractal.master.hostname";
   public static final String CONF_MASTER_HOSTNAME_DEFAULT = "localhost";
   public static final String CONF_LOG_LEVEL = "fractal.log.level";
   public static final String CONF_LOG_LEVEL_DEFAULT = "app";
   public static final String CONF_MAINGRAPH_CLASS = "fractal.graph.class";
   public static final String CONF_MAINGRAPH_CLASS_DEFAULT =
           "br.ufmg.cs.systems.fractal.graph.VELabeledMainGraph";
   public static final String CONF_MAINGRAPH_PATH = "fractal.graph.location";
   public static final String CONF_MAINGRAPH_PATH_DEFAULT = null;
   public static final String CONF_MAINGRAPH_LOCAL = "fractal.graph.local";
   public static final String CONF_MAINGRAPH_EDGE_LABELED =
           "fractal.graph.edge_labeled";
   public static final boolean CONF_MAINGRAPH_EDGE_LABELED_DEFAULT = false;
   public static final String CONF_MAINGRAPH_VERTEX_LABELED =
           "fractal.graph.vertex_labeled";
   public static final boolean CONF_MAINGRAPH_VERTEX_LABELED_DEFAULT = true;
   public static final String CONF_MAINGRAPH_MULTIGRAPH =
           "fractal.graph.multigraph";
   public static final boolean CONF_MAINGRAPH_MULTIGRAPH_DEFAULT = false;
   public static final String INFO_PERIOD = "fractal.info.period";
   public static final long INFO_PERIOD_DEFAULT_MS = 60000;
   public static final String CONF_COMPUTATION_CLASS = "fractal.computation.class";
   public static final String CONF_COMPUTATION_CLASS_DEFAULT =
           "br.ufmg.cs.systems.fractal.computation.ComputationContainer";
   public static final String CONF_COMM_STRATEGY = "fractal.comm.strategy";
   public static final String CONF_COMM_STRATEGY_DEFAULT = "scratch";
   public static final String CONF_PATTERN_CLASS = "fractal.pattern.class";
   public static final String CONF_PATTERN_CLASS_DEFAULT =
           "br.ufmg.cs.systems.fractal.pattern.JBlissPattern";
   public static final String CONF_WS_EXTERNAL_BATCHSIZE =
           "fractal.ws.external.batchsize";
   public static final int CONF_WS_EXTERNAL_BATCHSIZE_DEFAULT = 10;
   public static final String CONF_WS_INTERNAL = "fractal.ws.mode.internal";
   public static final boolean CONF_WS_MODE_INTERNAL_DEFAULT = true;
   public static final String CONF_WS_EXTERNAL = "fractal.ws.mode.external";
   public static final boolean CONF_WS_MODE_EXTERNAL_DEFAULT = true;
   public static final String CONF_MAINGRAPH_EDGE_FILTERING_PREDICATE =
           "fractal.maingraph.edge.filtering.predicate";
   public static final String CONF_MAINGRAPH_VERTEX_FILTERING_PREDICATE =
           "fractal.maingraph.vertex.filtering.predicate";
   protected transient long infoPeriod;
   protected transient MainGraph mainGraph;
   protected transient boolean initialized = false;
   protected transient boolean isGraphMulti;
   private transient Class<? extends MainGraph> mainGraphClass;
   private transient Class<? extends Pattern> patternClass;
   private transient Class<? extends Computation> computationClass;
   private transient Class<? extends Subgraph> subgraphClass;
   private transient boolean isGraphEdgeLabeled;
   private transient boolean isGraphVertexLabeled;
   private transient EdgeFilteringPredicate edgePredicate;
   private transient VertexFilteringPredicate vertexPredicate;

   public Configuration() {
   }

   private MainGraph createMainGraph() {
      return ReflectionSerializationUtils.newInstance(mainGraphClass);
   }

   public Pattern createPattern() {
      Pattern pattern = ReflectionSerializationUtils.newInstance(getPatternClass());
      pattern.init(this);
      return pattern;
   }

   public Class<? extends Pattern> getPatternClass() {
      return patternClass;
   }

   public void setPatternClass(Class<? extends Pattern> patternClass) {
      this.patternClass = patternClass;
   }

   public <S extends Subgraph> S createSubgraph(Class<S> subgraphClass) {
      S subgraph = (S) ReflectionSerializationUtils.newInstance(subgraphClass);
      subgraph.init(this);
      return subgraph;
   }

   public Class<?> getClass(String key, String defaultValue) {
      try {
         return Class.forName(getString(key, defaultValue));
      } catch (ClassNotFoundException e) {
         throw new RuntimeException(e);
      }
   }

   public String getString(String key, String defaultValue) {
      return defaultValue;
   }

   public Double getDouble(String key, Double defaultValue) {
      return null;
   }

   public Float getFloat(String key, Float defaultValue) {
      return defaultValue;
   }

   public long getInfoPeriod() {
      return infoPeriod;
   }

   public String getLogLevel() {
      return getString(CONF_LOG_LEVEL, CONF_LOG_LEVEL_DEFAULT);
   }

   public Long getLong(String key, Long defaultValue) {
      return defaultValue;
   }

   public String getMainGraphPath() {
      return getString(CONF_MAINGRAPH_PATH, CONF_MAINGRAPH_PATH_DEFAULT);
   }

   public String getMasterHostname() {
      return getString(CONF_MASTER_HOSTNAME, CONF_MASTER_HOSTNAME_DEFAULT);
   }

   public int getNumEdges() {
      return getMainGraph().numEdges();
   }

   public Class<? extends Subgraph> getSubgraphClass() {
      return subgraphClass;
   }

   public void setSubgraphClass(Class<? extends Subgraph> SubgraphClass) {
      this.subgraphClass = SubgraphClass;
   }

   public MainGraph getMainGraph() {
      return mainGraph;
   }

   public <G extends MainGraph> void setMainGraph(G mainGraph) {
      if (mainGraph != null) {
         this.mainGraph = mainGraph;
      }
   }

   public <T> T getObject(String key, T defaultValue) {
      return null;
   }

   public MainGraph getOrCreateMainGraph() {
      //String path = getMainGraphPath();
      String path = getMainGraphKey();
      MainGraph graph;

      if (path == null) {
         LOG.info("Creating main graph (not loaded from a path)");
         graph = createMainGraph();
      } else {
         graph = MainGraphStore.get(path);
         if (graph == null) {
            LOG.info("Creating main graph (not found in store)");
            graph = createMainGraph();
            MainGraphStore.put(path, graph);
         } else {
            LOG.info("Found main graph in store: " + path + " " + graph);
         }
      }

      return graph;
   }

   public int getWsBatchSize() {
      return getInteger(CONF_WS_EXTERNAL_BATCHSIZE,
              CONF_WS_EXTERNAL_BATCHSIZE_DEFAULT);
   }

   public Integer getInteger(String key, Integer defaultValue) {
      return defaultValue;
   }

   public void initialize(boolean isMaster) {
      throw new UnsupportedOperationException();
   }

   public boolean isGraphMulti() {
      return isGraphMulti;
   }

   public boolean isGraphEdgeLabeled() {
      return isGraphEdgeLabeled;
   }

   public boolean isGraphVertexLabeled() {
      return isGraphVertexLabeled;
   }

   public boolean isInitialized() {
      return initialized;
   }

   protected boolean isMainGraphRead() {
      return mainGraph != null &&
              (mainGraph.numVertices() > 0 || mainGraph.numEdges() > 0);
   }

   public Boolean getBoolean(String key, Boolean defaultValue) {
      return defaultValue;
   }

   public void readMainGraph() {
      try {
         mainGraph.init(this);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   public void setComputationClass(
           Class<? extends Computation> computationClass) {
      this.computationClass = computationClass;
   }

   public void setIsGraphEdgeLabeled(boolean isGraphEdgeLabeled) {
      this.isGraphEdgeLabeled = isGraphEdgeLabeled;
   }

   public void setIsGraphVertexLabeled(boolean isGraphVertexLabeled) {
      this.isGraphVertexLabeled = isGraphVertexLabeled;
   }

   public void setMainGraphClass(Class<? extends MainGraph> graphClass) {
      mainGraphClass = graphClass;
   }

   public boolean wsEnabled() {
      return internalWsEnabled() || externalWsEnabled();
   }

   public boolean internalWsEnabled() {
      return getBoolean(CONF_WS_INTERNAL, CONF_WS_MODE_INTERNAL_DEFAULT);
   }

   public boolean externalWsEnabled() {
      return getBoolean(CONF_WS_EXTERNAL, CONF_WS_MODE_EXTERNAL_DEFAULT);
   }

   public boolean collectThreadStats() {
      return getBoolean(
              CONF_COLLECT_THREAD_STATS, CONF_COLLECT_THREAD_STATS_DEFAULT);
   }

   public String getThreadStatsKey() {
      return getString(CONF_THREAD_STATS_KEY, CONF_THREAD_STATS_KEY_DEFAULT);
   }

   public EdgeFilteringPredicate getEdgeFilteringPredicate() {
      return edgePredicate;
   }

   public VertexFilteringPredicate getVertexFilteringPredicate() {
      return vertexPredicate;
   }

   public void setEdgeFilteringPredicate(EdgeFilteringPredicate edgePredicate) {
      this.edgePredicate = edgePredicate;
   }

   public void setVertexFilteringPredicate(VertexFilteringPredicate vertexPredicate) {
      this.vertexPredicate = vertexPredicate;
   }

   private String getMainGraphKey() {
      String mainGraphPath = getMainGraphPath();
      if (mainGraphPath == null) return null;
      EdgeFilteringPredicate edgePredicate = getEdgeFilteringPredicate();
      VertexFilteringPredicate vertexPredicate = getVertexFilteringPredicate();
      String mainGraphClassStr = mainGraphClass.getName();
      if (vertexPredicate != null && edgePredicate == null) {
         return mainGraphPath + "-" + mainGraphClassStr + "-" + vertexPredicate.getId();
      } else if (vertexPredicate == null && edgePredicate != null) {
         return mainGraphPath + "-" + mainGraphClassStr + "-" + edgePredicate.getId();
      } else if (vertexPredicate != null && edgePredicate != null) {
         return mainGraphPath + "-" + mainGraphClassStr + "-"
                 + edgePredicate.getId() + "-" + vertexPredicate.getId();
      } else {
         return mainGraphPath + "-" + mainGraphClassStr;
      }
   }
}

