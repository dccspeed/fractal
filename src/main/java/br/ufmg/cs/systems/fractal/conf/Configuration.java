package br.ufmg.cs.systems.fractal.conf;

import br.ufmg.cs.systems.fractal.aggregation.AggregationStorage;
import br.ufmg.cs.systems.fractal.aggregation.AggregationStorageMetadata;
import br.ufmg.cs.systems.fractal.aggregation.EndAggregationFunction;
import br.ufmg.cs.systems.fractal.aggregation.reductions.ReductionFunction;
import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.computation.MasterComputation;
import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.optimization.OptimizationSet;
import br.ufmg.cs.systems.fractal.optimization.OptimizationSetDescriptor;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.pattern.VICPattern;
import br.ufmg.cs.systems.fractal.subgraph.EdgeInducedSubgraph;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;
import br.ufmg.cs.systems.fractal.util.pool.Pool;
import br.ufmg.cs.systems.fractal.util.pool.PoolRegistry;
import br.ufmg.cs.systems.fractal.util.ReflectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Configuration<O extends Subgraph> implements Serializable {
    private static final Logger LOG = Logger.getLogger(Configuration.class);

    // we keep a local (per JVM) pool of configurations potentially
    // representing several active fractal applications
    private static AtomicInteger nextConfId = new AtomicInteger(0);

    protected final int id = newConfId();

    protected AtomicInteger taskCounter = new AtomicInteger(0);

    public static final String CONF_MASTER_HOSTNAME = "fractal.master.hostname";
    public static final String CONF_MASTER_HOSTNAME_DEFAULT = "localhost";

    public static final String CONF_TMP_DIR_DEFAULT = "/tmp/fractal";

    public static final String CONF_LOG_LEVEL = "fractal.log.level";
    public static final String CONF_LOG_LEVEL_DEFAULT = "info";
    public static final String CONF_MAINGRAPH_CLASS = "fractal.graph.class";
    public static final String CONF_MAINGRAPH_CLASS_DEFAULT = "br.ufmg.cs.systems.fractal.graph.BasicMainGraph";
    public static final String CONF_MAINGRAPH_PATH = "fractal.graph.location";
    public static final String CONF_MAINGRAPH_PATH_DEFAULT = "main.graph";
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
    public static final String CONF_OUTPUT_FORMAT_DEFAULT = "plain_text";

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

    public static final String CONF_AGGREGATION_STORAGE_CLASS = "fractal.aggregation.storage.class";
    public static final String CONF_AGGREGATION_STORAGE_CLASS_DEFAULT = "br.ufmg.cs.systems.fractal.aggregation.AggregationStorage";

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

    private static final String[] NEIGHBORHOOD_LOOKUPS_ARR = new String[16];
    static {
       for (int i = 0; i < NEIGHBORHOOD_LOOKUPS_ARR.length; ++i) {
          NEIGHBORHOOD_LOOKUPS_ARR[i] = "neighborhood_lookups_" + i;
       }
    }

    protected static Configuration instance = null;
    protected static ConcurrentHashMap<Integer,Configuration> activeConfigs =
       new ConcurrentHashMap<Integer,Configuration>();

    protected long infoPeriod;

    private Class<? extends MainGraph> mainGraphClass;
    private Class<? extends OptimizationSetDescriptor>
       optimizationSetDescriptorClass;
    private Class<? extends Pattern> patternClass;
    private Class<? extends Computation> computationClass;
    private Class<? extends MasterComputation> masterComputationClass;
    private Class<? extends Subgraph> subgraphClass;
    private Class<? extends SubgraphEnumerator> subgraphEnumClass;

    private String outputPath;

    private transient Map<String, AggregationStorageMetadata>
       aggregationsMetadata;
    protected transient MainGraph mainGraph;
    protected int mainGraphId = -1;
    private boolean isGraphEdgeLabelled;
    protected transient boolean initialized = false;
    protected boolean isGraphMulti;

    private static int newConfId() {
       return nextConfId.getAndIncrement();
    }

    public int getId() {
       return id;
    }

    public static boolean isUnset() {
       return instance == null;
    }

    public static <C extends Configuration> C get() {
        if (instance == null) {
           LOG.error ("instance is null");
            throw new RuntimeException("Oh-oh, Null configuration");
        }

        if (!instance.isInitialized()) {
           instance.initialize();
        }

        return (C) instance;
    }

    public static void unset() {
       instance = null;
    }

    public synchronized static void setIfUnset(Configuration configuration) {
        if (isUnset()) {
            set(configuration);
        }
    }

    public static void set(Configuration configuration) {
        instance = configuration;

        // Whenever we set configuration, reset all known pools Since they might
        // have initialized things based on a previous configuration NOTE: This
        // is essential for the unit tests
        for (Pool pool : PoolRegistry.instance().getPools()) {
            pool.reset();
        }
    }

    public static void add(Configuration configuration) {
       activeConfigs.put(configuration.getId(), configuration);
    }

    public static void remove(int id) {
       activeConfigs.remove(id);
    }

    public static Configuration get(int id) {
       return activeConfigs.get(id);
    }

    public static boolean isUnset(int id) {
       return !activeConfigs.containsKey(id);
    }
    
    public static String NEIGHBORHOOD_LOOKUPS(int i) {
       return NEIGHBORHOOD_LOOKUPS_ARR[i];
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

        optimizationSetDescriptorClass =
           (Class<? extends OptimizationSetDescriptor>) getClass(
                 CONF_OPTIMIZATIONSETDESCRIPTOR_CLASS,
                 CONF_OPTIMIZATIONSETDESCRIPTOR_CLASS_DEFAULT);

        patternClass = (Class<? extends Pattern>) getClass(CONF_PATTERN_CLASS,
              CONF_PATTERN_CLASS_DEFAULT);

        // create (empty) graph
        setMainGraph(createGraph());

        // TODO: Make this more flexible
        if (isGraphEdgeLabelled || isGraphMulti) {
            patternClass = VICPattern.class;
        }

        computationClass = (Class<? extends Computation>)
           getClass(CONF_COMPUTATION_CLASS, CONF_COMPUTATION_CLASS_DEFAULT);
        masterComputationClass = (Class<? extends MasterComputation>)
           getClass(CONF_MASTER_COMPUTATION_CLASS,
                 CONF_MASTER_COMPUTATION_CLASS_DEFAULT);

        aggregationsMetadata = new HashMap<>();

        outputPath = getString(CONF_OUTPUT_PATH, CONF_OUTPUT_PATH_DEFAULT + "_"
              + computationClass.getName());

        Computation<O> computation = createComputation();
        computation.initAggregations(this);

        OptimizationSetDescriptor optimizationSetDescriptor =
           ReflectionUtils.newInstance(optimizationSetDescriptorClass);
        OptimizationSet optimizationSet =
           optimizationSetDescriptor.describe(this);

        LOG.info("Active optimizations: " + optimizationSet);

        optimizationSet.applyStartup();

        if (!isMainGraphRead()) {
            // Load graph immediately (try to make it so that everyone loads the
            // graph at the same time) This prevents imbalances if aggregators
            // use the main graph (which means that master node would load first
            // on superstep -1) then all the others would load on (superstep 0).
            // mainGraph = createGraph();
            readMainGraph();
        }

        optimizationSet.applyAfterGraphLoad();
        initialized = true;
        LOG.info("Configuration initialized");
    }

    public boolean isInitialized() {
       return initialized;
    }

    public String getString(String key, String defaultValue) {
        return null;
    }

    public Boolean getBoolean(String key, Boolean defaultValue) {
        return null;
    }

    public Integer getInteger(String key, Integer defaultValue) {
        return null;
    }

    public Long getLong(String key, Long defaultValue) {
        return null;
    }

    public Float getFloat(String key, Float defaultValue) {
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
        return null;
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

    public <E extends Subgraph> E createSubgraph() {
        E subgraph = (E) ReflectionUtils.newInstance(subgraphClass);
        subgraph.init(this);
        return subgraph;
    }

    public Class<? extends Subgraph> getSubgraphClass() {
        return subgraphClass;
    }

    public void setSubgraphClass(Class<? extends Subgraph> SubgraphClass) {
        this.subgraphClass = SubgraphClass;
    }

    public SubgraphEnumerator<O> createSubgraphEnumerator(boolean bypass) {
        SubgraphEnumerator<O> senum;
        if (!bypass) {
            senum = (SubgraphEnumerator<O>) ReflectionUtils.newInstance(subgraphEnumClass);
        } else {
            senum = (SubgraphEnumerator<O>) ReflectionUtils.newInstance(
                    br.ufmg.cs.systems.fractal.computation.BypassSubgraphEnumerator.class);
        }
        senum.init(this);
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
       return mainGraph != null && (mainGraph.getNumberVertices() > 0 ||
          mainGraph.getNumberEdges() > 0);
    }

    public MainGraph createGraph() {
        for (Map.Entry<Integer,Configuration> entry: activeConfigs.entrySet()) {
            if (entry.getValue().mainGraphId == mainGraphId &&
                  entry.getValue().isMainGraphRead()) {
                MainGraph graph = entry.getValue().getMainGraph();
                if (graph != null) {
                   return graph;
                }
            }
        }

        try {
            Constructor<? extends MainGraph> constructor;
            constructor = mainGraphClass.getConstructor(String.class,
                  boolean.class, boolean.class);
            MainGraph graph = constructor.newInstance(getMainGraphPath(),
                  isGraphEdgeLabelled, isGraphMulti);
            if (this.mainGraphId > -1) {
               graph.setId(mainGraphId);
            }
            return graph;
        } catch (NoSuchMethodException | IllegalAccessException |
              InstantiationException | InvocationTargetException e) {
            throw new RuntimeException("Could not create main graph", e);
        }
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
              LOG.warn("Graph properties file not found: " +
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

    public Map<String, AggregationStorageMetadata> getAggregationsMetadata() {
        return Collections.unmodifiableMap(aggregationsMetadata);
    }

    public void setAggregationsMetadata(
          Map<String,AggregationStorageMetadata> aggregationsMetadata) {
       this.aggregationsMetadata = aggregationsMetadata;
    }

    public <K extends Writable, V extends Writable>
    void registerAggregation(String name,
          Class<? extends AggregationStorage> aggStorageClass,
          Class<K> keyClass, Class<V> valueClass,
          boolean persistent, ReductionFunction<V> reductionFunction,
          EndAggregationFunction<K, V> endAggregationFunction, boolean isIncremental) {
       if (aggregationsMetadata.containsKey(name)) {
          return;
       }

       AggregationStorageMetadata<K, V> aggregationMetadata =
          new AggregationStorageMetadata<>(aggStorageClass,
                keyClass, valueClass, persistent, reductionFunction,
                endAggregationFunction, isIncremental);

       aggregationsMetadata.put(name, aggregationMetadata);
    }

    public <K extends Writable, V extends Writable>
    AggregationStorageMetadata<K, V> getAggregationMetadata(String name) {
        AggregationStorageMetadata<K,V> metadata =
           (AggregationStorageMetadata<K, V>) aggregationsMetadata.get(name);
        return metadata;
    }

    public <O extends Subgraph> Computation<O> createComputation() {
        return ReflectionUtils.newInstance(computationClass);
    }
    
    public <K extends Writable, V extends Writable>
    AggregationStorage<K,V> createAggregationStorage(String name) {
        return ReflectionUtils.newInstance (
              getAggregationMetadata(name).getAggregationStorageClass());
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public String getOutputFormat() {
       return getString(CONF_OUTPUT_FORMAT, CONF_OUTPUT_FORMAT_DEFAULT);
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

    public Class<? extends AggregationStorage> getAggregationStorageClass() {
        return (Class<? extends AggregationStorage>) getClass(
              CONF_AGGREGATION_STORAGE_CLASS,
              CONF_AGGREGATION_STORAGE_CLASS_DEFAULT);
    }

    public void setMasterComputationClass(
          Class<? extends MasterComputation> masterComputationClass) {
       this.masterComputationClass = masterComputationClass;
    }

    public void setComputationClass(
          Class<? extends Computation> computationClass) {
       this.computationClass = computationClass;
    }

    public void setOptimizationSetDescriptorClass(
          Class<? extends OptimizationSetDescriptor> optimizationSetDescriptorClass) {
       this.optimizationSetDescriptorClass = optimizationSetDescriptorClass;
    }

    public OptimizationSet getOptimizationSet() {
       OptimizationSetDescriptor optimizationSetDescriptor =
          ReflectionUtils.newInstance(optimizationSetDescriptorClass);
       OptimizationSet optimizationSet =
          optimizationSetDescriptor.describe(this);
       return optimizationSet;
    }

    public String getCommStrategy() {
       return getString (CONF_COMM_STRATEGY, CONF_COMM_STRATEGY_DEFAULT);
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
          return getMainGraph().getNumberEdges();
       } else if (SubgraphClass == VertexInducedSubgraph.class) {
          return getMainGraph().getNumberVertices();
       } else if (SubgraphClass == PatternInducedSubgraph.class) {
          return getMainGraph().getNumberVertices();
       } else {
          throw new RuntimeException(
                "Unknown subgraph type " + SubgraphClass);
       }
    }

    public int getNumVertices() {
       return getMainGraph().getNumberVertices();
    }
    
    public int getNumEdges() {
       return getMainGraph().getNumberEdges();
    }

    
}

