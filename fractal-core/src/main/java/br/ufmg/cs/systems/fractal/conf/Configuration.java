package br.ufmg.cs.systems.fractal.conf;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.computation.MasterComputation;
import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.graph.MainGraphStore;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.pattern.VICPattern;
import br.ufmg.cs.systems.fractal.subgraph.EdgeInducedSubgraph;
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;
import br.ufmg.cs.systems.fractal.util.ReflectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class Configuration implements Serializable {
    private static final Logger LOG = Logger.getLogger(Configuration.class);

    // we keep a local (per JVM) pool of configurations potentially
    // representing several active fractal applications
    private static AtomicInteger nextConfId = new AtomicInteger(0);

    // static immutable parameters {
    public static final boolean OPCOUNTER_ENABLED = true;
    public static final int MAX_VERTICES_IN_SUBGRAPHS = 10;
    // }

    public static final String CONF_MASTER_HOSTNAME = "fractal.master.hostname";
    public static final String CONF_MASTER_HOSTNAME_DEFAULT = "localhost";

    public static final String CONF_TMP_DIR_DEFAULT = "/tmp/fractal";

    public static final String CONF_LOG_LEVEL = "fractal.log.level";
    public static final String CONF_LOG_LEVEL_DEFAULT = "info";

    public static final String CONF_MAINGRAPH_CLASS = "fractal.graph.class";
    public static final String CONF_MAINGRAPH_CLASS_DEFAULT = "br.ufmg.cs.systems.fractal.graph.BasicMainGraph";

    public static final String CONF_MAINGRAPH_PATH = "fractal.graph.location";
    public static final String CONF_MAINGRAPH_PATH_DEFAULT = null;

    public static final String CONF_MAINGRAPH_LOCAL = "fractal.graph.local";
    public static final boolean CONF_MAINGRAPH_LOCAL_DEFAULT = false;

    public static final String CONF_MAINGRAPH_EDGE_LABELLED = "fractal.graph.edge_labelled";
    public static final boolean CONF_MAINGRAPH_EDGE_LABELLED_DEFAULT = false;

    public static final String CONF_MAINGRAPH_MULTIGRAPH = "fractal.graph.multigraph";
    public static final boolean CONF_MAINGRAPH_MULTIGRAPH_DEFAULT = false;

    public static final String CONF_OPTIMIZATIONSETDESCRIPTOR_CLASS = "fractal.optimizations.descriptor";
    public static final String CONF_OPTIMIZATIONSETDESCRIPTOR_CLASS_DEFAULT = "br.ufmg.cs.systems.fractal.optimization.ConfigBasedOptimizationSetDescriptor";

    public static final String CONF_OUTPUT_ACTIVE = "fractal.output.active";
    public static final boolean CONF_OUTPUT_ACTIVE_DEFAULT = true;
    public static final String CONF_OUTPUT_FORMAT = "fractal.output.format";

   public static final String INFO_PERIOD = "fractal.info.period";
    public static final long INFO_PERIOD_DEFAULT = 60000;

    public static final String CONF_COMPUTATION_CLASS = "fractal.computation.class";
    public static final String CONF_COMPUTATION_CLASS_DEFAULT = "br.ufmg.cs.systems.fractal.computation.ComputationContainer";

    public static final String CONF_MASTER_COMPUTATION_CLASS = "fractal.master_computation.class";
    public static final String CONF_MASTER_COMPUTATION_CLASS_DEFAULT = "br.ufmg.cs.systems.fractal.computation.MasterComputation";

    public static final String CONF_COMM_STRATEGY = "fractal.comm.strategy";
    public static final String CONF_COMM_STRATEGY_DEFAULT = "scratch";

    public static final String CONF_PATTERN_CLASS = "fractal.pattern.class";
    public static final String CONF_PATTERN_CLASS_DEFAULT = "br.ufmg.cs.systems.fractal.pattern.JBlissPattern";

    public static final String CONF_ENUMERATOR_CLASS = "fractal.enumerator.class";
    public static final String CONF_ENUMERATOR_CLASS_DEFAULT = "br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator";

    public static final String CONF_OUTPUT_PATH = "fractal.output.path";
    public static final String CONF_OUTPUT_PATH_DEFAULT = "Output";

    public static final String CONF_INCREMENTAL_AGGREGATION = "fractal.aggregation.incremental";
    public static final boolean CONF_INCREMENTAL_AGGREGATION_DEFAULT = false;

   // work stealing {{
    public static final String CONF_WS_EXTERNAL_BATCHSIZE_LOW = "fractal.ws.external.batchsize.low";
    public static final int CONF_WS_EXTERNAL_BATCHSIZE_LOW_DEFAULT = 1;

    public static final String CONF_WS_EXTERNAL_BATCHSIZE_HIGH = "fractal.ws.external.batchsize.high";
    public static final int CONF_WS_EXTERNAL_BATCHSIZE_HIGH_DEFAULT = 10;
    
    public static final String CONF_WS_INTERNAL = "fractal.ws.mode.internal";
    public static final boolean CONF_WS_MODE_INTERNAL_DEFAULT = true;

    public static final String CONF_WS_EXTERNAL = "fractal.ws.mode.external";
    public static final boolean CONF_WS_MODE_EXTERNAL_DEFAULT = true;
    
    public static final String CONF_JVMPROF_CMD = "fractal.jvmprof.cmd";
    public static final String CONF_JVMPROF_CMD_DEFAULT = null;

   // }}

   protected final int id = newConfId();
   protected transient AtomicInteger taskCounter = new AtomicInteger(0);

   protected transient long infoPeriod;

    private transient Class<? extends MainGraph> mainGraphClass;
   private transient Class<? extends Pattern> patternClass;
   private transient Class<? extends Computation> computationClass;
    private transient Class<? extends MasterComputation> masterComputationClass;
    private transient Class<? extends Subgraph> subgraphClass;
    private transient Class<? extends SubgraphEnumerator> subgraphEnumClass;

    private transient String outputPath;

   protected transient MainGraph mainGraph;
    protected int mainGraphId = -1;
    private transient boolean isGraphEdgeLabelled;
    protected transient boolean initialized = false;
    protected transient boolean isGraphMulti;

    private static int newConfId() {
       return nextConfId.getAndIncrement();
    }

    public int getId() {
       return id;
    }

   public boolean taskCheckIn(int expect, int ntasks) {
       return taskCounter.compareAndSet(expect, ntasks);
    }

    public int taskCheckOut() {
       return taskCounter.decrementAndGet();
    }

    public int taskCounter() {
       return taskCounter.get();
    }

    public Configuration() {}

    public void initialize() {
       initialize(false);
    }
    
    public void initialize(boolean isMaster) {
        if (initialized) {
            return;
        }

        LOG.info("Initializing Configuration...");

        infoPeriod = getLong(INFO_PERIOD, INFO_PERIOD_DEFAULT);

        mainGraphClass = (Class<? extends MainGraph>) getClass(
              CONF_MAINGRAPH_CLASS, CONF_MAINGRAPH_CLASS_DEFAULT);

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
            computationClass = (Class<? extends Computation>)
                    getClass(CONF_COMPUTATION_CLASS, CONF_COMPUTATION_CLASS_DEFAULT);
        }
        masterComputationClass = (Class<? extends MasterComputation>)
           getClass(CONF_MASTER_COMPUTATION_CLASS,
                 CONF_MASTER_COMPUTATION_CLASS_DEFAULT);

        outputPath = getString(CONF_OUTPUT_PATH, CONF_OUTPUT_PATH_DEFAULT + "_"
              + computationClass.getName());

        Computation computation = createComputation();
        computation.initAggregations(this);

        if (!isMainGraphRead()) {
            // Load graph immediately (try to make it so that everyone loads the
            // graph at the same time) This prevents imbalances if aggregators
            // use the main graph (which means that master node would load first
            // on superstep -1) then all the others would load on (superstep 0).
            // mainGraph = createGraph();
            readMainGraph();
        }

        initialized = true;
        LOG.info("Configuration initialized");
    }

    public boolean isInitialized() {
       return initialized;
    }

    public <T> T getObject(String key, T defaultValue) {
       return null;
    }

    public String getString(String key, String defaultValue) {
        return defaultValue;
    }

    public Boolean getBoolean(String key, Boolean defaultValue) {
        return defaultValue;
    }

    public Integer getInteger(String key, Integer defaultValue) {
        return defaultValue;
    }

    public Long getLong(String key, Long defaultValue) {
        return defaultValue;
    }

    public Float getFloat(String key, Float defaultValue) {
        return defaultValue;
    }
    
    public Double getDouble(String key, Double defaultValue) {
        return null;
    }

    public Class<?> getClass(String key, String defaultValue) {
        try {
            return Class.forName(getString(key, defaultValue));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public String[] getStrings(String key, String... defaultValues) {
        return defaultValues;
    }

    public Class<?>[] getClasses(String key, Class<?>... defaultValues) {
       String classNamesStr = getString(key, null);
       if (classNamesStr == null) {
          return defaultValues;
       }

       String[] classNames = classNamesStr.split(",");
       if (classNames.length == 0) {
          return defaultValues;
       } else {
          try {
             Class<?>[] classes = new Class<?>[classNames.length];
             for (int i = 0; i < classes.length; ++i) {
                classes[i] = Class.forName(classNames[i]);
             }
             return classes;
          } catch (ClassNotFoundException e) {
             throw new RuntimeException(e);
          }
       }
    }

    public Class<? extends Pattern> getPatternClass() {
        return patternClass;
    }

    public void setPatternClass(Class<? extends Pattern> patternClass) {
       this.patternClass = patternClass;
    }

    public Pattern createPattern() {
       Pattern pattern = ReflectionUtils.newInstance(getPatternClass());
       pattern.init(this);
       return pattern;
    }

    public void setMainGraphClass(Class<? extends MainGraph> graphClass) {
       mainGraphClass = graphClass;
    }

    public String getLogLevel() {
       return getString (CONF_LOG_LEVEL, CONF_LOG_LEVEL_DEFAULT);
    }

    public String getMainGraphPath() {
        return getString(CONF_MAINGRAPH_PATH, CONF_MAINGRAPH_PATH_DEFAULT);
    }
    
    public String getMainGraphPropertiesPath() {
        return getMainGraphPath() + ".prop";
    }

    public long getInfoPeriod() {
        return infoPeriod;
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

   public Class<? extends Subgraph> getSubgraphClass() {
        return subgraphClass;
    }

    public void setSubgraphClass(Class<? extends Subgraph> SubgraphClass) {
        this.subgraphClass = SubgraphClass;
    }

    public <O extends Subgraph> SubgraphEnumerator<O> createSubgraphEnumerator(Computation<O> computation) {
       boolean bypass = computation.shouldBypass();
       SubgraphEnumerator<O> senum;
       if (!bypass) {
          senum = (SubgraphEnumerator<O>) ReflectionUtils.newInstance(subgraphEnumClass);
       } else {
          senum = (SubgraphEnumerator<O>) ReflectionUtils.newInstance(
                br.ufmg.cs.systems.fractal.computation.BypassSubgraphEnumerator.class);
       }
       senum.init(computation.getConfig(), computation);
       return senum;
    }

    public void setSubgraphEnumClass(Class<? extends SubgraphEnumerator> subgraphEnumClass) {
       this.subgraphEnumClass = subgraphEnumClass;
    }

    public <G extends MainGraph> G getMainGraph() {
       return (G) mainGraph;
    }

    public <G extends MainGraph> void setMainGraph(G mainGraph) {
        if (mainGraph != null) {
            this.mainGraphId = mainGraph.getId();
            this.mainGraph = mainGraph;
        }
    }

    public void setMainGraphId(int mainGraphId) {
       this.mainGraphId = mainGraphId;
    }

    protected boolean isMainGraphRead() {
       return mainGraph != null && (mainGraph.numVertices() > 0 ||
          mainGraph.numEdges() > 0);
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

   private MainGraph createMainGraph() {
      String path = getMainGraphPath();
      MainGraph graph;
      try {
         Constructor<? extends MainGraph> constructor;
         constructor = mainGraphClass.getConstructor(String.class, boolean.class, boolean.class);
         graph = constructor.newInstance(path, isGraphEdgeLabelled, isGraphMulti);
      } catch (NoSuchMethodException | IllegalAccessException |
              InstantiationException | InvocationTargetException e) {
         throw new RuntimeException("Could not create main graph", e);
      }

      if (this.mainGraphId > -1) {
         graph.setId(mainGraphId);
      }

      return graph;
   }

   protected void readMainGraph() {
        boolean useLocalGraph = getBoolean(CONF_MAINGRAPH_LOCAL,
              CONF_MAINGRAPH_LOCAL_DEFAULT);
        
        // maybe read properties
        try {
            Method initProperties = mainGraphClass.getMethod(
                  "initProperties", Object.class);

            if (useLocalGraph) {
                initProperties.invoke(mainGraph,
                      Paths.get(getMainGraphPropertiesPath()));
            } else {
                initProperties.invoke(mainGraph,
                      new Path(getMainGraphPropertiesPath()));
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
        } catch (NoSuchMethodException | IllegalAccessException |
              InvocationTargetException e) {
            throw new RuntimeException("Could not read main graph", e);
        }
    }

    public boolean isOutputActive() {
        return getBoolean(CONF_OUTPUT_ACTIVE, CONF_OUTPUT_ACTIVE_DEFAULT);
    }

   public <O extends Subgraph> Computation<O> createComputation() {
        return ReflectionUtils.newInstance(computationClass);
    }

   public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

   public MasterComputation createMasterComputation() {
        return ReflectionUtils.newInstance(masterComputationClass);
    }

    public void setIsGraphEdgeLabelled(boolean isGraphEdgeLabelled) {
       this.isGraphEdgeLabelled = isGraphEdgeLabelled;
    }

    public boolean isGraphEdgeLabelled() {
        return isGraphEdgeLabelled;
    }

   public void setMasterComputationClass(
          Class<? extends MasterComputation> masterComputationClass) {
       this.masterComputationClass = masterComputationClass;
    }

    public void setComputationClass(
          Class<? extends Computation> computationClass) {
       this.computationClass = computationClass;
    }

   public String getMasterHostname() {
       return getString(CONF_MASTER_HOSTNAME, CONF_MASTER_HOSTNAME_DEFAULT);
    }

    public int getWsBatchSizeLow() {
       return getInteger(CONF_WS_EXTERNAL_BATCHSIZE_LOW,
               CONF_WS_EXTERNAL_BATCHSIZE_LOW_DEFAULT);
    }
    
    public int getWsBatchSizeHigh() {
       return getInteger(CONF_WS_EXTERNAL_BATCHSIZE_HIGH,
               CONF_WS_EXTERNAL_BATCHSIZE_HIGH_DEFAULT);
    }

    public boolean internalWsEnabled() {
       return getBoolean(CONF_WS_INTERNAL, CONF_WS_MODE_INTERNAL_DEFAULT);
    }
    
    public boolean externalWsEnabled() {
       return getBoolean(CONF_WS_EXTERNAL, CONF_WS_MODE_EXTERNAL_DEFAULT);
    }

    public boolean wsEnabled() {
       return internalWsEnabled() || externalWsEnabled();
    }

    public int getNumWords() {
       Class<? extends Subgraph> SubgraphClass = getSubgraphClass();
       if (SubgraphClass == EdgeInducedSubgraph.class) {
          return getMainGraph().numEdges();
       } else if (SubgraphClass == VertexInducedSubgraph.class) {
          return getMainGraph().numVertices();
       } else if (SubgraphClass == PatternInducedSubgraph.class) {
          return getMainGraph().numVertices();
       } else {
          throw new RuntimeException(
                "Unknown subgraph type " + SubgraphClass);
       }
    }

    public int getNumVertices() {
       return getMainGraph().numVertices();
    }
    
    public int getNumEdges() {
       return getMainGraph().numEdges();
    }

    
}

