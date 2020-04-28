package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.BasicMainGraph;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import br.ufmg.cs.systems.fractal.util.pool.IntIntMapPool;
import br.ufmg.cs.systems.fractal.util.pool.IntSetPool;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.ObjObjMap;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashObjSet;
import com.koloboke.collect.set.hash.HashObjSets;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.function.IntConsumer;

public class PatternUtils {
   private static final Logger LOG = Logger.getLogger(PatternUtils.class);

   /**
    * Creates minimal configurations for isolated pattern handling
    * @return new configuration
    */
   public static Configuration createConfig() {
      Configuration config = new Configuration();
      //config.setComputationClass(VertexInducedComputation.class);
      config.setSubgraphClass(VertexInducedSubgraph.class);
      config.setMainGraphClass(BasicMainGraph.class);
      config.setPatternClass(JBlissPattern.class);
      //config.initialize();
      return config;
   }

   /**
    * Creates an empty graph
    * @param config existing configuration
    * @return new graph
    */
   private static MainGraph createGraph(Configuration config) {
      MainGraph graph = config.createGraph();
      config.setMainGraph(graph);
      return graph;
   }

   /**
    * Creates an empty graph containing exactly the same vertices and edges from *pattern*
    * @param config existing configuration
    * @param pattern pattern to use as template
    * @return new graph
    */
   private static MainGraph createGraph(Configuration config, Pattern pattern) {
      MainGraph graph = createGraph(config);
      IntArrayList vertexLabels = new IntArrayList(pattern.getNumberOfVertices() + 1);

      // add pattern vertices
      for (int u = 0; u < pattern.getNumberOfVertices(); ++u) {
         graph.addVertex(u);
         vertexLabels.add(1);
      }

      // add new vertex
      graph.addVertex(pattern.getNumberOfVertices());
      vertexLabels.add(1);

      // add pattern edges and edge labels
      for (int e = 0; e < pattern.getNumberOfEdges(); ++e) {
         PatternEdge pedge = pattern.getEdges().getUnchecked(e);
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
    * Generates all canonical patterns from extending each existing pattern by one vertex
    * @param patterns set of existing patterns
    * @return set of new patterns
    */
   public static HashObjSet<Pattern> extendByVertex(HashObjSet<Pattern> patterns) {
      HashObjSet<Pattern> newPatterns = HashObjSets.newMutableSet();
      patterns.forEach(p -> newPatterns.addAll(extendByVertex(p)));
      return newPatterns;
   }

   /**
    * Generates a set with one single vertex unlabeled pattern
    * @return set containing the new pattern
    */
   public static HashObjSet<Pattern> singleVertexPatternSet() {
      HashObjSet<Pattern> patterns = HashObjSets.newMutableSet();
      patterns.add(singleVertexPattern());
      return patterns;
   }

   /**
    * Generates a single vertex unlabeled pattern
    * @return new pattern
    */
   public static Pattern singleVertexPattern() {
      Configuration config = createConfig();
      MainGraph graph = createGraph(config);
      graph.addVertex(0);
      VertexInducedSubgraph subgraph = (VertexInducedSubgraph) config.createSubgraph();
      subgraph.addWord(0);
      return subgraph.getPattern();
   }

   /**
    * Add a vertex and its edges to *pattern*, returning the new pattern
    * @param pattern the existing pattern
    * @param sources the edge sources representing edges with the new vertex
    * @return new pattern
    */
   public static Pattern addVertex(Pattern pattern, int ... sources) {
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

      return subgraph.getPattern();
   }

   /**
    * Generates all canonical patterns obtained from *pattern* by extending one vertex from it
    * @param pattern
    * @return set of new canonical patterns
    */
   public static HashObjSet<Pattern> extendByVertex(Pattern pattern) {
      Configuration config = createConfig();
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
            MainGraph graph = createGraph(config, pattern);

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
            Pattern newPattern = subgraph.getPattern();
            newPattern.turnCanonical();
            newPatterns.add(newPattern);

         }
      }

      return newPatterns;
   }

   /**
    * Maps a pattern to one of its automorphisms such that edges stay in increasing order of vertex positions
    * @param pattern pattern to be modified in-place
    */
   public static void increasingPositions(Pattern pattern) {
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

      /**
       * Maps the edges according to the new labeling
       */
      for (i = 0; i < pattern.getNumberOfEdges(); ++i) {
         pedge = pattern.getEdges().get(i);
         int newSrc = labeling.get(pedge.getSrcPos());
         int newDst = labeling.get(pedge.getDestPos());
         pedge.setSrcPos(newSrc);
         pedge.setDestPos(newDst);
         if (newSrc > newDst) pedge.invert();
      }

      /**
       * Pre-compute the symmetry breaking of this pattern
       */
      pattern.vsymmetryBreaker();
      LOG.info("AfterReordering pattern=" + pattern + " labeling=" + labeling);

      IntIntMapPool.instance().reclaimObject(labeling);
   }

   public static long completeMatch(Computation computation, Subgraph partialMatch, Pattern pattern) {

      /**
       * Declarations
       */
      MainGraph graph = computation.getConfig().getMainGraph();
      IntArrayListPool pool = IntArrayListPool.instance();
      PatternEdgeArrayList patternEdges = pattern.getEdges();
      IntArrayList intersection = pool.createObject();
      IntArrayList difference = pool.createObject();
      ObjObjMap<IntArrayList,IntArrayList> validExtensionsMap = HashObjObjMaps.newMutableMap(); // TODO: pool this?
      int nextVertexPos = partialMatch.getNumVertices();
      ObjArrayList<IntArrayList> validExtensionsArray = new ObjArrayList<>(pattern.getNumberOfVertices() - nextVertexPos); // TODO: pool this?

      for (int i = 0; i < patternEdges.size(); ++i) {
         PatternEdge pedge = patternEdges.getUnchecked(i);
         if (pedge.getDestPos() < nextVertexPos) continue;

         // compute valid extensions for *nextVertexPos*
         if (pedge.getDestPos() > nextVertexPos) {
            IntArrayList validExtensions = validExtensionsMap.get(intersection);

            if (validExtensions == null) {
               // compute difference set if induced
               if (pattern.induced()) {
                  for (int src = 0; src < nextVertexPos; ++src) {
                     if (!intersection.contains(src)) difference.add(partialMatch.getVertices().getUnchecked(src));
                  }
               }

               // key now is *intersection*
               validExtensions = pool.createObject();
               IntArrayList finalValidExtensions = validExtensions;
               graph.neighborhoodTraversal(intersection, difference, Integer.MIN_VALUE, finalValidExtensions::add);

               validExtensionsMap.put(intersection, validExtensions);
            }

            validExtensionsArray.add(validExtensions);

            // prepare for next *nextVertexPos*
            intersection = pool.createObject();
            intersection.add(partialMatch.getVertices().getUnchecked(pedge.getSrcPos()));
            difference.clear();
            ++nextVertexPos;

         } else { // add into intersection
            intersection.add(partialMatch.getVertices().getUnchecked(pedge.getSrcPos()));
         }
      }

      IntArrayList validExtensions = validExtensionsMap.get(intersection);

      if (validExtensions == null) {
         // compute difference set if induced
         if (pattern.induced()) {
            for (int src = 0; src < nextVertexPos; ++src) {
               if (!intersection.contains(src)) difference.add(partialMatch.getVertices().getUnchecked(src));
            }
         }

         // key now is *intersection*
         validExtensions = pool.createObject();
         IntArrayList finalValidExtensions = validExtensions;
         graph.neighborhoodTraversal(intersection, difference, Integer.MIN_VALUE, finalValidExtensions::add);

         validExtensionsMap.put(intersection, validExtensions);
      }

      validExtensionsArray.add(validExtensions);

      /**
       * Process new subgraphs
       */
      IntSet vertices = IntSetPool.instance().createObject();
      long numSubgraphs = completeMatch(partialMatch, pattern, validExtensionsArray, 0, vertices);
      IntSetPool.instance().reclaimObject(vertices);

      pool.reclaimObject(intersection);
      pool.reclaimObject(difference);
      validExtensionsMap.forEach((k,v) -> { pool.reclaimObject(k); pool.reclaimObject(v); });

      return numSubgraphs;
   }

   private static long completeMatch(Subgraph partialMatch, Pattern pattern, ObjArrayList<IntArrayList> validExtensionsArray, int depth, IntSet vertices) {
      if (partialMatch.getNumVertices() == pattern.getNumberOfVertices()) {
         // callback because the match is complete
         return 1;
      }

      long validSubgraphs = 0;
      IntArrayList validExtensions = validExtensionsArray.getUnchecked(depth);
      int lowerBound = pattern.sbLowerBound(partialMatch, partialMatch.getNumVertices());
      int startIdx = validExtensions.binarySearch(lowerBound);
      startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx + 1;
      for (; startIdx < validExtensions.size(); ++startIdx) {
         int u = validExtensions.getUnchecked(startIdx);
         //if (vertices.contains(u)) continue;
         partialMatch.addWord(u);
         //vertices.add(u);
         validSubgraphs += completeMatch(partialMatch, pattern, validExtensionsArray, depth + 1, vertices);
         //vertices.removeInt(u);
         partialMatch.removeLastWord();
      }

      return validSubgraphs;
   }

   public static void main(String[] args) {
      Pattern pattern = singleVertexPattern();
      pattern = addVertex(pattern, 0);
      pattern = addVertex(pattern, 0, 1);
      pattern = addVertex(pattern, 0, 1);
   }
}
