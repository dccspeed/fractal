package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.BasicMainGraph;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.subgraph.EdgeInducedSubgraph;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntIntMapPool;
import com.koloboke.collect.ObjCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.collect.map.hash.HashObjObjMap;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import com.koloboke.collect.set.ObjSet;
import com.koloboke.collect.set.hash.HashObjSet;
import com.koloboke.collect.set.hash.HashObjSets;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.log4j.Logger;

import java.util.Iterator;

public class PatternUtils {
   private static final Logger LOG = Logger.getLogger(PatternUtils.class);

   private static final ThreadLocal<Configuration> configThreadLocal =
           ThreadLocal.withInitial(() -> {
              Configuration config = new Configuration();
              config.setSubgraphClass(VertexInducedSubgraph.class);
              config.setMainGraphClass(BasicMainGraph.class);
              config.setPatternClass(JBlissPattern.class);
              return config;
           });

   private static final ThreadLocal<MainGraph> graphThreadLocal =
           ThreadLocal.withInitial(() -> {
              Configuration config = configThreadLocal.get();
              MainGraph graph = config.getOrCreateMainGraph();
              config.setMainGraph(graph);
              return graph;
           });

   private static final ThreadLocal<VertexInducedSubgraph> vsubgraph =
           ThreadLocal.withInitial(() -> configThreadLocal.get().createSubgraph());

   /**
    * Generates all canonical patterns obtained from *pattern* by extending
    * one vertex from it
    *
    * @param pattern
    * @return set of new canonical patterns
    */
   public static HashObjSet<Pattern> extendByVertex(Pattern pattern,
                                                    int vertexLabel) {
      HashObjSet<Pattern> newPatterns = HashObjSets.newMutableSet();
      IntArrayList vertexPositions = new IntArrayList(pattern.getNumberOfVertices());
      int newPosition = pattern.getNumberOfVertices();

      // create vertex positions
      for (int u = 0; u < pattern.getNumberOfVertices(); ++u) {
         vertexPositions.add(u);
      }

      // generate all connection patterns
      for (int n = 1; n <= vertexPositions.size(); ++n) {
         Iterator<IntArrayList> connectionPatternsIter = vertexPositions.combinations(n);
         while (connectionPatternsIter.hasNext()) {
            IntArrayList connectionPattern = connectionPatternsIter.next();
            Configuration config = createConfig();
            MainGraph graph = createGraph(config, pattern);
            graph.addVertex(newPosition);
            graph.addVertexLabel(newPosition, vertexLabel);

            // add connection pattern edges to temp graph
            for (int i = 0; i < connectionPattern.size(); ++i) {
               int u = connectionPattern.get(i);
               graph.addEdge(u, newPosition, graph.numEdges());
            }

            // create vertex induced subgraph representing the temp graph (with all of its vertices and edges)
            VertexInducedSubgraph subgraph = (VertexInducedSubgraph) config.createSubgraph();
            for (int u = 0; u < graph.numVertices(); ++u) subgraph.addWord(u);

            // get new quick pattern from subgraph, turn canonical (canonical labeling) and add to the resulting set to
            // remove duplicates
            Pattern newPattern = subgraph.quickPattern();
            newPattern.turnCanonical();
            newPatterns.add(newPattern);

         }
      }

      return newPatterns;
   }

   /**
    * Generates all canonical patterns obtained from *pattern* by extending one edge from it
    *
    * @param pattern
    * @return set of new canonical patterns
    */
   public static HashObjSet<Pattern> extendByEdge(Pattern pattern,
                                                  int vertexLabel) {
      HashObjObjMap<Pattern,Pattern> quickMap = HashObjObjMaps.newMutableMap();
      HashObjSet<Pattern> newPatterns = HashObjSets.newMutableSet();

      LOG.debug("ExtendingByEdge " + pattern + " numVertices=" + pattern.getNumberOfVertices());

      // patterns with internal edges
      for (int u = 0; u < pattern.getNumberOfVertices(); ++u) {
         for (int v = u + 1; v < pattern.getNumberOfVertices(); ++v) {
            // check if this edge already exists
            boolean edgeExists = false;
            for (PatternEdge pedge : pattern.getEdges()) {
               if ((pedge.getSrcPos() == u && pedge.getDestPos() == v)
                       || (pedge.getSrcPos() == v && pedge.getDestPos() == u)) {
                  edgeExists = true;
                  break;
               }
            }

            if (edgeExists) continue;

            Configuration config = createConfig();
            config.setSubgraphClass(EdgeInducedSubgraph.class);
            MainGraph graph = createGraph(config, pattern);
            graph.addEdge(u, v, graph.numEdges());

            // create vertex induced subgraph representing the temp graph (with all of its vertices and edges)
            EdgeInducedSubgraph subgraph = (EdgeInducedSubgraph) config.createSubgraph();
            for (int w = 0; w < graph.numEdges(); ++w) subgraph.addWord(w);

            // get new quick pattern from subgraph, turn canonical (canonical labeling) and add to the resulting set to
            // remove duplicates
            Pattern newPattern = subgraph.quickPattern();
            Pattern canonicalPattern = newPattern.copy();
            canonicalPattern.turnCanonical();
            if (!quickMap.containsKey(canonicalPattern)) {
               quickMap.put(canonicalPattern, newPattern);
               //newPatterns.add(newPattern);
               newPatterns.add(canonicalPattern);
            }
         }
      }

      // patterns with external edges
      int v = pattern.getNumberOfVertices();
      for (int u = 0; u < pattern.getNumberOfVertices(); ++u) {
         Configuration config = createConfig();
         config.setSubgraphClass(EdgeInducedSubgraph.class);
         MainGraph graph = createGraph(config, pattern);
         graph.addVertex(pattern.getNumberOfVertices());
         graph.addVertexLabel(pattern.getNumberOfVertices(), vertexLabel);
         graph.addEdge(u, v, graph.numEdges());

         // create vertex induced subgraph representing the temp graph (with all of its vertices and edges)
         EdgeInducedSubgraph subgraph = (EdgeInducedSubgraph) config.createSubgraph();
         for (int w = 0; w < graph.numEdges(); ++w) subgraph.addWord(w);

         // get new quick pattern from subgraph, turn canonical (canonical labeling) and add to the resulting set to
         // remove duplicates
         Pattern newPattern = subgraph.quickPattern();
         Pattern canonicalPattern = newPattern.copy();
         canonicalPattern.turnCanonical();
         if (!quickMap.containsKey(canonicalPattern)) {
            quickMap.put(canonicalPattern, newPattern);
            newPatterns.add(canonicalPattern);
         }
      }

      LOG.debug("ExtendingByEdge newPatterns=" + newPatterns);

      return newPatterns;
   }


   /**
    * Creates minimal configurations for isolated pattern handling
    *
    * @return new configuration
    */
   public static Configuration createConfig() {
      Configuration config = new Configuration();
      config.setSubgraphClass(VertexInducedSubgraph.class);
      config.setMainGraphClass(BasicMainGraph.class);
      config.setPatternClass(JBlissPattern.class);
      return config;
   }

   /**
    * Creates an empty graph containing exactly the same vertices and edges from *pattern*
    *
    * @param config  existing configuration
    * @param pattern pattern to use as template
    * @return new graph
    */
   private static MainGraph createGraph(Configuration config, Pattern pattern) {
      MainGraph graph = createGraph(config);
      IntArrayList vertexLabels = new IntArrayList(pattern.getNumberOfVertices());

      // add pattern vertices
      for (int u = 0; u < pattern.getNumberOfVertices(); ++u) {
         graph.addVertex(u);
         vertexLabels.add(1);
      }

      // add pattern edges and edge labels
      for (int e = 0; e < pattern.getNumberOfEdges(); ++e) {
         PatternEdge pedge = pattern.getEdges().getu(e);
         graph.addEdge(pedge.getSrcPos(), pedge.getDestPos(), e);
         //graph.addEdgeLabel(e, pedge.getLabel()); // TODO: handle edge label
         vertexLabels.set(pedge.getSrcPos(), pedge.getSrcLabel());
         vertexLabels.set(pedge.getDestPos(), pedge.getDestLabel());
      }

      // add vertex labels
      for (int u = 0; u < vertexLabels.size(); ++u) {
         graph.addVertexLabel(u, vertexLabels.get(u));
      }

      return graph;
   }

   /**
    * Creates an empty graph
    *
    * @param config existing configuration
    * @return new graph
    */
   private static MainGraph createGraph(Configuration config) {
      MainGraph graph = config.getOrCreateMainGraph();
      config.setMainGraph(graph);
      return graph;
   }

   /**
    * Generates a single vertex unlabeled pattern
    *
    * @return new pattern
    */
   public static Pattern singleVertexPattern() {
      Configuration config = createConfig();
      MainGraph graph = createGraph(config);
      graph.addVertex(0);
      VertexInducedSubgraph subgraph = (VertexInducedSubgraph) config.createSubgraph();
      subgraph.addWord(0);
      return subgraph.quickPattern();
   }

   /**
    * Generates a single vertex labeled pattern
    *
    * @return new pattern
    */
   public static Pattern singleVertexPattern(int vertexLabel) {
      Configuration config = createConfig();
      MainGraph graph = createGraph(config);
      graph.addVertex(0);
      graph.addVertexLabel(0, vertexLabel);
      VertexInducedSubgraph subgraph = (VertexInducedSubgraph) config.createSubgraph();
      subgraph.addWord(0);
      return subgraph.quickPattern();
   }

   /**
    * Generates a single edge unlabeled pattern
    *
    * @return new pattern
    */
   public static Pattern singleEdgePattern() {
      Configuration config = createConfig();
      config.setSubgraphClass(EdgeInducedSubgraph.class);
      MainGraph graph = createGraph(config);
      graph.addVertex(0);
      graph.addVertex(1);
      graph.addEdge(0, 1, graph.numEdges());
      EdgeInducedSubgraph subgraph = (EdgeInducedSubgraph) config.createSubgraph();
      subgraph.addWord(0);
      return subgraph.quickPattern();
   }

   /**
    * Maps a pattern to one of its automorphisms such that edges stay in increasing order of vertex positions
    *
    * @param pattern pattern to be modified in-place
    */
   public static void increasingPositions(Pattern pattern) {
      if (pattern.getNumberOfVertices() == 1) return;
      IntIntMap labeling = IntIntMapPool.instance().createObject();
      int i, j, src, dst;
      PatternEdge pedge = null;

      /**
       * Reorder pattern such as new edges's source vertices occurs in previous edges.
       * Also, build a new labeling that reflects this order.
       */
      pedge = pattern.getEdges().get(0);
      src = pedge.getSrcPos();
      dst = pedge.getDestPos();
      labeling.putIfAbsent(src, labeling.size());
      labeling.putIfAbsent(dst, labeling.size());
      for (i = 1; i < pattern.getNumberOfEdges(); ++i) {
         pedge = null;

         // find next edge with existing vertices
         for (j = i; j < pattern.getNumberOfEdges(); ++j) {
            pedge = pattern.getEdges().get(j);
            src = pedge.getSrcPos();
            dst = pedge.getDestPos();
            if (labeling.containsKey(src)) {
               break;
            }
            if (labeling.containsKey(dst)) {
               pedge.invert();
               break;
            }
         }

         // next safe edge at this point
         PatternEdge aux = pattern.getEdges().get(i);
         pattern.getEdges().set(i, pedge);
         pattern.getEdges().set(j, aux);
         labeling.putIfAbsent(src, labeling.size());
         labeling.putIfAbsent(dst, labeling.size());
      }

      pattern.relabel(labeling);
      pattern.getEdges().sort();

      ///**
      // * Maps the edges according to the new labeling
      // */
      //for (i = 0; i < pattern.getNumberOfEdges(); ++i) {
      //   pedge = pattern.getEdges().get(i);
      //   int newSrc = labeling.get(pedge.getSrcPos());
      //   int newDst = labeling.get(pedge.getDestPos());
      //   pedge.setSrcPos(newSrc);
      //   pedge.setDestPos(newDst);
      //   if (newSrc > newDst) pedge.invert();
      //}

      /**
       * Pre-compute the symmetry breaking of this pattern
       */
      LOG.debug("AfterReordering pattern=" + pattern + " labeling=" + labeling);

      IntIntMapPool.instance().reclaimObject(labeling);
   }

   /**
    * Add a vertex and its edges to *pattern*, returning the new pattern
    *
    * @param pattern the existing pattern
    * @param sources the edge sources representing edges with the new vertex
    * @return new pattern
    */
   public static Pattern addVertex(Pattern pattern, int... sources) {
      Configuration config = pattern.getConfig();
      MainGraph graph = config.getMainGraph();
      int numVertices = pattern.getNumberOfVertices();

      graph.addVertex(numVertices);
      for (int src : sources) {
         if (src >= numVertices) {
            throw new RuntimeException("Vertex source does not exists in pattern with " + numVertices + " vertices");
         }
         graph.addEdge(src, numVertices, graph.numEdges());
      }

      VertexInducedSubgraph subgraph = (VertexInducedSubgraph) config.createSubgraph();
      for (int u = 0; u < graph.numVertices(); ++u) subgraph.addWord(u);

      return subgraph.quickPattern();
   }

   public static ObjSet<Pattern> quickPatterns(Pattern pattern) {
         int numVertices = pattern.getNumberOfVertices();
         int numEdges = pattern.getNumberOfEdges();
         IntArrayList vertices = new IntArrayList();
         IntArrayList edges = new IntArrayList();
         for (int u = 0; u < numVertices; ++u) vertices.add(u);
         for (int e = 0; e < numEdges; ++e) edges.add(e);

         Iterator<IntArrayList> orderings = vertices.permutations();

         ObjSet<Pattern> patterns = HashObjSets.newMutableSet();
         ObjSet<Pattern> quickPatterns = HashObjSets.newMutableSet();
         IntIntMap relabeling = HashIntIntMaps.newMutableMap();

         // vertex permutation
         while (orderings.hasNext()) {
            IntArrayList ordering = orderings.next();
            relabeling.clear();
            for (int i = 0; i < ordering.size(); ++i) {
               relabeling.put(i, ordering.get(i));
            }

            Pattern newPattern = pattern.copy();
            newPattern.relabel(relabeling);
            newPattern.getEdges().sort();
            patterns.add(newPattern);
         }

         Iterator<IntArrayList> edgeOrderings = edges.permutations();

         while (edgeOrderings.hasNext()) {
            IntArrayList edgeOrdering = edgeOrderings.next();
            // edge permutation
            ObjCursor<Pattern> cur = patterns.cursor();
            while (cur.moveNext()) {
               Pattern relabeledPattern = cur.elem();
               Pattern newPattern = relabeledPattern.copy();

               for (int i = 0; i < edgeOrdering.size(); ++i) {
                  int targetEdgeIdx = edgeOrdering.get(i);
                  PatternEdge targetEdge = relabeledPattern.getEdges().get(targetEdgeIdx);
                  newPattern.getEdges().get(i).setFromOther(targetEdge);
               }

               LOG.info(relabeledPattern + " " + newPattern + " " + edgeOrdering);

               boolean validOrdering = true;
               PatternEdgeArrayList newPatternEdges = newPattern.getEdges();
               PatternEdge firstEdge = newPatternEdges.get(0);

               if (firstEdge.getSrcPos() != 0 || firstEdge.getDestPos() != 1) {
                  validOrdering = false;
               } else {
                  int lastVisitedVertex = 1;
                  for (int i = 1; i < newPatternEdges.size(); ++i) {
                     PatternEdge nextEdge = newPatternEdges.get(i);
                     int src = nextEdge.getSrcPos();
                     int dst = nextEdge.getDestPos();
                     if (src > lastVisitedVertex || dst > lastVisitedVertex + 1) {
                        validOrdering = false;
                        break;
                     }
                  }
               }

               if (validOrdering) quickPatterns.add(newPattern);

            }
         }

         return quickPatterns;

   }
}
