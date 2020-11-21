package br.ufmg.cs.systems.fractal.conf;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.graph.MainGraphStore;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.pattern.VICPattern;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.ReflectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Paths;

public class Configuration implements Serializable {
   private static final Logger LOG = Logger.getLogger(Configuration.class);

   public static final boolean INSTRUMENTATION_ENABLED = false;
   public static final String CONF_MASTER_HOSTNAME = "fractal.master.hostname";
   public static final String CONF_MASTER_HOSTNAME_DEFAULT = "localhost";
   public static final String CONF_TMP_DIR_DEFAULT = "/tmp/fractal";
   public static final String CONF_LOG_LEVEL = "fractal.log.level";
   public static final String CONF_LOG_LEVEL_DEFAULT = "app";
   public static final String CONF_MAINGRAPH_CLASS = "fractal.graph.class";
   public static final String CONF_MAINGRAPH_CLASS_DEFAULT =
           "br.ufmg.cs.systems.fractal.graph.SuccinctMainGraph";
   public static final String CONF_MAINGRAPH_PATH = "fractal.graph.location";
   public static final String CONF_MAINGRAPH_PATH_DEFAULT = null;
   public static final String CONF_MAINGRAPH_LOCAL = "fractal.graph.local";
   public static final boolean CONF_MAINGRAPH_LOCAL_DEFAULT = false;
   public static final String CONF_MAINGRAPH_EDGE_LABELLED =
           "fractal.graph.edge_labelled";
   public static final boolean CONF_MAINGRAPH_EDGE_LABELLED_DEFAULT = false;
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
   public static final String CONF_ENUMERATOR_CLASS =
           "fractal.enumerator.class";
   public static final String CONF_ENUMERATOR_CLASS_DEFAULT =
           "br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator";
   public static final String CONF_WS_EXTERNAL_BATCHSIZE =
           "fractal.ws.external.batchsize";
   public static final int CONF_WS_EXTERNAL_BATCHSIZE_DEFAULT = 10;
   public static final String CONF_WS_INTERNAL = "fractal.ws.mode.internal";
   public static final boolean CONF_WS_MODE_INTERNAL_DEFAULT = true;
   public static final String CONF_WS_EXTERNAL = "fractal.ws.mode.external";
   public static final boolean CONF_WS_MODE_EXTERNAL_DEFAULT = true;
   protected transient long infoPeriod;
   protected transient MainGraph mainGraph;
   protected transient boolean initialized = false;
   protected transient boolean isGraphMulti;
   private transient Class<? extends MainGraph> mainGraphClass;
   private transient Class<? extends Pattern> patternClass;
   private transient Class<? extends Computation> computationClass;
   private transient Class<? extends Subgraph> subgraphClass;
   private transient Class<? extends SubgraphEnumerator> subgraphEnumClass;
   private transient boolean isGraphEdgeLabelled;

   public Configuration() {
   }

   public <O extends Subgraph> Computation<O> createComputation() {
      return ReflectionUtils.newInstance(computationClass);
   }

   private MainGraph createMainGraph() {
      String path = getMainGraphPath();
      MainGraph graph;
      try {
         Constructor<? extends MainGraph> constructor;
         constructor = mainGraphClass
                 .getConstructor(String.class, boolean.class, boolean.class);
         graph = constructor
                 .newInstance(path, isGraphEdgeLabelled, isGraphMulti);
      } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
         throw new RuntimeException("Could not create main graph", e);
      }

      return graph;
   }

   public Pattern createPattern() {
      Pattern pattern = ReflectionUtils.newInstance(getPatternClass());
      pattern.init(this);
      return pattern;
   }

   public Class<? extends Pattern> getPatternClass() {
      return patternClass;
   }

   public void setPatternClass(Class<? extends Pattern> patternClass) {
      this.patternClass = patternClass;
   }

   public <S extends Subgraph> S createSubgraph() {
      S subgraph = (S) ReflectionUtils.newInstance(subgraphClass);
      subgraph.init(this);
      return subgraph;
   }

   public <S extends Subgraph> S createSubgraph(Class<S> subgraphClass) {
      S subgraph = (S) ReflectionUtils.newInstance(subgraphClass);
      subgraph.init(this);
      return subgraph;
   }

   public <O extends Subgraph> SubgraphEnumerator<O> createSubgraphEnumerator(
           Computation<O> computation) {
      boolean bypass = computation.shouldBypass();
      SubgraphEnumerator<O> senum;
      if (!bypass) {
         senum = (SubgraphEnumerator<O>) ReflectionUtils
                 .newInstance(subgraphEnumClass);
      } else {
         senum = (SubgraphEnumerator<O>) ReflectionUtils.newInstance(
                 br.ufmg.cs.systems.fractal.computation.BypassSubgraphEnumerator.class);
      }
      senum.init(computation.getConfig(), computation);
      return senum;
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

   public String getMainGraphPropertiesPath() {
      return getMainGraphPath() + ".prop";
   }

   public String getMasterHostname() {
      return getString(CONF_MASTER_HOSTNAME, CONF_MASTER_HOSTNAME_DEFAULT);
   }

   public int getNumEdges() {
      return getMainGraph().numEdges();
   }

   public int getNumVertices() {
      return getMainGraph().numVertices();
   }

   public Class<? extends Subgraph> getSubgraphClass() {
      return subgraphClass;
   }

   public void setSubgraphClass(Class<? extends Subgraph> SubgraphClass) {
      this.subgraphClass = SubgraphClass;
   }

   public <G extends MainGraph> G getMainGraph() {
      return (G) mainGraph;
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
      String path = getMainGraphPath();
      MainGraph graph;

      if (path == null) {
         LOG.debug("Creating main graph (not loaded from a path)");
         graph = createMainGraph();
      } else {
         graph = MainGraphStore.get(path);
         if (graph == null) {
            LOG.debug("Creating main graph (not found in store)");
            graph = createMainGraph();
            MainGraphStore.put(path, graph);
         } else {
            LOG.debug("Found main graph in store: " + path + " " + graph);
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

   public void initialize() {
      initialize(false);
   }

   public void initialize(boolean isMaster) {
      if (initialized) {
         return;
      }

      LOG.info("Initializing Configuration...");

      infoPeriod = getLong(INFO_PERIOD, INFO_PERIOD_DEFAULT_MS);

      mainGraphClass =
              (Class<? extends MainGraph>) getClass(CONF_MAINGRAPH_CLASS,
                      CONF_MAINGRAPH_CLASS_DEFAULT);

      isGraphEdgeLabelled = getBoolean(CONF_MAINGRAPH_EDGE_LABELLED,
              CONF_MAINGRAPH_EDGE_LABELLED_DEFAULT);

      isGraphMulti = getBoolean(CONF_MAINGRAPH_MULTIGRAPH,
              CONF_MAINGRAPH_MULTIGRAPH_DEFAULT);

      patternClass = (Class<? extends Pattern>) getClass(CONF_PATTERN_CLASS,
              CONF_PATTERN_CLASS_DEFAULT);

      // create (empty) graph
      setMainGraph(getOrCreateMainGraph());

      // TODO: Make this more flexible
      if (isGraphEdgeLabelled || isGraphMulti) {
         patternClass = VICPattern.class;
      }

      if (computationClass == null) {
         computationClass =
                 (Class<? extends Computation>) getClass(CONF_COMPUTATION_CLASS,
                         CONF_COMPUTATION_CLASS_DEFAULT);
      }

      Computation computation = createComputation();
      computation.initAggregations(this);

      if (!isMainGraphRead()) {
         readMainGraph();
      }

      initialized = true;
      LOG.info("Configuration initialized");
   }

   public boolean isGraphEdgeLabelled() {
      return isGraphEdgeLabelled;
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

   protected void readMainGraph() {
      boolean useLocalGraph =
              getBoolean(CONF_MAINGRAPH_LOCAL, CONF_MAINGRAPH_LOCAL_DEFAULT);

      // maybe read properties
      try {
         Method initProperties =
                 mainGraphClass.getMethod("initProperties", Object.class);

         if (useLocalGraph) {
            initProperties
                    .invoke(mainGraph, Paths.get(getMainGraphPropertiesPath()));
         } else {
            initProperties
                    .invoke(mainGraph, new Path(getMainGraphPropertiesPath()));
         }
      } catch (NoSuchMethodException | IllegalAccessException e) {
         throw new RuntimeException("Could not read graph properties", e);
      } catch (InvocationTargetException e) {
         if (e.getTargetException() instanceof IOException) {
            LOG.info("Graph properties file not found: " +
                    getMainGraphPropertiesPath());
         } else {
            throw new RuntimeException("Could not read graph properties", e);
         }
      }

      try {
         Method init = mainGraphClass.getMethod("init", Object.class);

         if (useLocalGraph) {
            init.invoke(mainGraph, Paths.get(getMainGraphPath()));
         } else {
            init.invoke(mainGraph, new Path(getMainGraphPath()));
         }
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
         throw new RuntimeException("Could not read main graph", e);
      }
   }

   public void setComputationClass(
           Class<? extends Computation> computationClass) {
      this.computationClass = computationClass;
   }

   public void setIsGraphEdgeLabelled(boolean isGraphEdgeLabelled) {
      this.isGraphEdgeLabelled = isGraphEdgeLabelled;
   }

   public void setMainGraphClass(Class<? extends MainGraph> graphClass) {
      mainGraphClass = graphClass;
   }

   public void setSubgraphEnumClass(
           Class<? extends SubgraphEnumerator> subgraphEnumClass) {
      this.subgraphEnumClass = subgraphEnumClass;
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


}

