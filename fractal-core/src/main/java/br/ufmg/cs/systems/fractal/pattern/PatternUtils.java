package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.pattern.pool.PatternEdgePool;
import br.ufmg.cs.systems.fractal.util.TextFileParser;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntIntMapPool;
import com.koloboke.collect.ObjCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.collect.map.hash.HashObjObjMap;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import com.koloboke.collect.set.ObjSet;
import com.koloboke.collect.set.hash.HashObjSet;
import com.koloboke.collect.set.hash.HashObjSets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import javax.xml.soap.Text;
import java.io.*;
import java.util.Iterator;

public class PatternUtils {
   private static final Logger LOG = Logger.getLogger(PatternUtils.class);

   private static final Configuration configuration;
   static {
      configuration = new Configuration();
      configuration.setPatternClass(JBlissPattern.class);
   }

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
      IntArrayList vertexLabels = new IntArrayList(pattern.getNumberOfVertices());
      int newPosition = pattern.getNumberOfVertices();
      PatternEdgePool edgePool =
              PatternEdgePool.instance(pattern.edgeLabeled());

      // create vertex positions
      for (int u = 0; u < pattern.getNumberOfVertices(); ++u) {
         vertexPositions.add(u);
         vertexLabels.add(-1);
      }

      vertexLabels.set(0, pattern.getFirstVertexLabel());

      for (PatternEdge pedge : pattern.getEdges()) {
         int src = pedge.getSrcPos();
         int dst = pedge.getDestPos();
         vertexLabels.set(src, pedge.getSrcLabel());
         vertexLabels.set(dst, pedge.getDestLabel());
      }

      // generate all connection patterns
      for (int n = 1; n <= vertexPositions.size(); ++n) {
         Iterator<IntArrayList> connectionPatternsIter = vertexPositions.combinations(n);
         while (connectionPatternsIter.hasNext()) {
            IntArrayList connectionPattern = connectionPatternsIter.next();
            Pattern newPattern = pattern.copy();
            newPattern.addVertexStandalone(vertexLabel);

            // add connection pattern edges to temp graph
            for (int i = 0; i < connectionPattern.size(); ++i) {
               int u = connectionPattern.get(i);
               PatternEdge edge = edgePool.createObject();
               edge.setSrcPos(u);
               edge.setSrcLabel(vertexLabels.get(u));
               edge.setDestPos(newPosition);
               edge.setDestLabel(vertexLabel);
               newPattern.addEdge(edge);
            }

            newPattern.turnCanonical();
            newPatterns.add(newPattern);
            //if (!newPatterns.add(newPattern)) {
            //   edgePool.reclaimObjects(newPattern.getEdges());
            //}
         }
      }

      return newPatterns;
   }

   /**
    * Generates all canonical patterns obtained from *pattern* by extending one edge from it
    *
    * @param pattern
    * @param vertexLabel vertex label for patterns with an additional vertex
    *                    (external edges)
    * @return set of new canonical patterns
    */
   public static HashObjSet<Pattern> extendByEdge(Pattern pattern,
                                                  int vertexLabel) {
      return extendByEdge(pattern, vertexLabel, -1);
   }
   /**
    * Generates all canonical patterns obtained from *pattern* by extending one edge from it
    *
    * @param pattern
    * @return set of new canonical patterns
    */
   public static HashObjSet<Pattern> extendByEdge(Pattern pattern,
                                                  int vertexLabel,
                                                  int edgeLabel) {
      HashObjSet<Pattern> newPatterns = extendByEdgeInternal(
              pattern, edgeLabel);
      HashObjSet<Pattern> externalPatterns = extendByEdgeExternal(
              pattern, vertexLabel, edgeLabel);

      newPatterns.addAll(externalPatterns);

      return newPatterns;
   }

   /**
    * Generates all external canonical patterns obtained from *pattern* by
    * extending one edge from it such that this edge spans a new vertex
    *
    * @param pattern
    * @param vertexLabel
    * @param edgeLabel
    * @return set of new canonical patterns
    */
   public static HashObjSet<Pattern> extendByEdgeExternal(Pattern pattern,
                                                          int vertexLabel,
                                                          int edgeLabel) {
      boolean edgeLabeled = pattern.edgeLabeled();
      HashObjObjMap<Pattern,Pattern> quickMap = HashObjObjMaps.newMutableMap();
      HashObjSet<Pattern> newPatterns = HashObjSets.newMutableSet();
      PatternEdgePool edgePool =
              PatternEdgePool.instance(pattern.edgeLabeled());
      int numVertices = pattern.getNumberOfVertices();
      IntArrayList vertexLabels = new IntArrayList(numVertices);
      for (int i = 0; i < numVertices; ++i) vertexLabels.add(-1);
      for (PatternEdge edge : pattern.getEdges()) {
         vertexLabels.setu(edge.getSrcPos(), edge.getSrcLabel());
         vertexLabels.setu(edge.getDestPos(), edge.getDestLabel());
      }

      // patterns with external edges
      int v = pattern.getNumberOfVertices();
      for (int u = 0; u < pattern.getNumberOfVertices(); ++u) {
         Pattern newPattern = pattern.copy();
         newPattern.addVertexStandalone(vertexLabel);
         PatternEdge edge = edgePool.createObject();
         edge.setSrcPos(u);
         edge.setSrcLabel(vertexLabels.getu(u));
         edge.setDestPos(v);
         edge.setDestLabel(vertexLabel);

         if (edgeLabeled) {
            LabelledPatternEdge ledge = (LabelledPatternEdge) edge;
            ledge.setLabel(edgeLabel);
         }

         newPattern.addEdgeStandalone(edge);
         Pattern canonicalPattern = newPattern.copy();
         canonicalPattern.turnCanonical();
         if (!quickMap.containsKey(canonicalPattern)) {
            quickMap.put(canonicalPattern, newPattern);
            newPatterns.add(canonicalPattern);
         }
      }

      return newPatterns;
   }

   /**
    * Generates all external canonical patterns obtained from *pattern* by
    * extending one edge from it such that this edge do not spans a new vertex
    *
    * @param pattern
    * @param edgeLabel
    * @return set of new canonical patterns
    */
   public static HashObjSet<Pattern> extendByEdgeInternal(Pattern pattern,
                                                          int edgeLabel) {
      boolean edgeLabeled = pattern.edgeLabeled();
      HashObjObjMap<Pattern,Pattern> quickMap = HashObjObjMaps.newMutableMap();
      HashObjSet<Pattern> newPatterns = HashObjSets.newMutableSet();
      PatternEdgePool edgePool =
              PatternEdgePool.instance(pattern.edgeLabeled());
      int numVertices = pattern.getNumberOfVertices();
      IntArrayList vertexLabels = new IntArrayList(numVertices);
      for (int i = 0; i < numVertices; ++i) vertexLabels.add(-1);
      for (PatternEdge edge : pattern.getEdges()) {
         vertexLabels.setu(edge.getSrcPos(), edge.getSrcLabel());
         vertexLabels.setu(edge.getDestPos(), edge.getDestLabel());
      }

      // patterns with internal edges
      for (int u = 0; u < numVertices; ++u) {
         for (int v = u + 1; v < numVertices; ++v) {
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

            Pattern newPattern = pattern.copy();
            PatternEdge edge = edgePool.createObject();
            edge.setSrcPos(u);
            edge.setSrcLabel(vertexLabels.getu(u));
            edge.setDestPos(v);
            edge.setDestLabel(vertexLabels.getu(v));

            if (edgeLabeled) {
               LabelledPatternEdge ledge = (LabelledPatternEdge) edge;
               ledge.setLabel(edgeLabel);
            }

            newPattern.addEdgeStandalone(edge);
            Pattern canonicalPattern = newPattern.copy();
            canonicalPattern.turnCanonical();
            if (!quickMap.containsKey(canonicalPattern)) {
               quickMap.put(canonicalPattern, newPattern);
               newPatterns.add(canonicalPattern);
            }
         }
      }

      return newPatterns;
   }

   public static Pattern singleVertexPattern() {
      Pattern pattern = configuration.createPattern();
      pattern.addVertexStandalone();
      return pattern;
   }

   public static Pattern singleVertexPattern(int vertexLabel) {
      Pattern pattern = configuration.createPattern();
      pattern.addVertexStandalone(vertexLabel);
      return pattern;
   }

   public static Pattern singleEdgePattern() {
      Pattern pattern = configuration.createPattern();
      pattern.addVertexStandalone();
      pattern.addVertexStandalone();
      PatternEdge edge = PatternEdgePool.instance(false).createObject();
      edge.setSrcPos(0);
      edge.setSrcLabel(1);
      edge.setDestPos(1);
      edge.setDestLabel(1);
      pattern.addEdgeStandalone(edge);
      return pattern;
   }

   /**
    * Maps a pattern to one of its automorphisms such that edges stay in increasing order of vertex positions
    *
    * @param pattern pattern to be modified in-place
    */
   public static void increasingPositions(Pattern pattern) {
      int numVertices = pattern.getNumberOfVertices();
      if (numVertices == 1) return;
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

      LOG.debug("AfterReordering pattern=" + pattern + " labeling=" + labeling);

      IntIntMapPool.instance().reclaimObject(labeling);
   }

   public static Pattern addVertex(Pattern pattern, int... sources) {
      boolean areEdgesLabeled = pattern.edgeLabeled();
      PatternEdgePool edgePool = PatternEdgePool.instance(areEdgesLabeled);
      Pattern newPattern = pattern.copy();
      newPattern.addVertexStandalone();
      int dst = pattern.getNumberOfVertices();
      for (int src : sources) {
         PatternEdge edge = edgePool.createObject();
         edge.setSrcPos(src);
         edge.setDestPos(dst);
         newPattern.addEdgeStandalone(edge);
      }

      return newPattern;
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
                     } else if (dst == lastVisitedVertex + 1) {
                        lastVisitedVertex++;
                     }
                  }
               }

               //LOG.info(relabeledPattern + " " + newPattern + " "
               //        + edgeOrdering +
               //        " " + validOrdering);

               if (validOrdering) quickPatterns.add(newPattern);

            }
         }

         return quickPatterns;

   }

   public static void toFS(Pattern pattern, String patternDirPath) throws IOException {
      FileSystem fs = FileSystem.get(new org.apache.hadoop.conf.Configuration());

      // files
      String metadataFilePath = patternDirPath + "/metadata";
      String vlabelsFilePath = patternDirPath + "/vlabels";
      String elabelsFilePath = patternDirPath + "/elabels";
      String edgesFilePath = patternDirPath + "/edges";

      Path hadoopPath;
      OutputStream os;

      // metadata
      int numVertices = pattern.getNumberOfVertices();
      int numEdges = pattern.getNumberOfEdges();
      hadoopPath = new Path(metadataFilePath);
      os = null;
      try {
         os = fs.create(hadoopPath);
         os.write(String.format("%d %d\n", numVertices, numEdges).getBytes());
      } finally {
         if (os != null) os.close();
      }

      // vlabels
      boolean vertexLabeled = pattern.vertexLabeled();
      hadoopPath = new Path(vlabelsFilePath);
      os = null;
      try {
         IntArrayList vlabels = pattern.getVertexLabels(vertexLabeled);
         if (vlabels != null) {
            os = fs.create(hadoopPath);
            for (int i = 0; i < vlabels.size(); ++i) {
               os.write(String.format("%d\n", vlabels.get(i)).getBytes());
            }
         }
      } finally {
         if (os != null) os.close();
      }

      // elabels
      boolean edgeLabeled = pattern.edgeLabeled();
      hadoopPath = new Path(elabelsFilePath);
      os = null;
      try {
         IntArrayList elabels = pattern.getEdgeLabels(edgeLabeled);
         if (elabels != null) {
            os = fs.create(hadoopPath);
            for (int i = 0; i < elabels.size(); ++i) {
               os.write(String.format("%d\n", elabels.get(i)).getBytes());
            }
         }
      } finally {
         if (os != null) os.close();
      }

      // edges
      PatternEdgeArrayList edges = pattern.copy().getEdges();
      edges.sort();
      hadoopPath = new Path(edgesFilePath);
      os = null;
      try {
         os = fs.create(hadoopPath);
         for (PatternEdge edge : edges) {
            int src = edge.getSrcPos();
            int dst = edge.getDestPos();
            os.write(String.format("%d %d\n", src, dst).getBytes());
         }
      } finally {
         if (os != null) os.close();
      }
   }

   public static Pattern fromFS(String patternDirPath) throws IOException {
      Pattern pattern = configuration.createPattern();
      FileSystem fs = FileSystem.get(new org.apache.hadoop.conf.Configuration());

      // files
      String metadataFilePath = patternDirPath + "/metadata";
      String vlabelsFilePath = patternDirPath + "/vlabels";
      String elabelsFilePath = patternDirPath + "/elabels";
      String edgesFilePath = patternDirPath + "/edges";

      Path hadoopPath;

      // metadata
      int numVertices, numEdges;
      hadoopPath = new Path(metadataFilePath);
      if (fs.exists(hadoopPath)) {
         InputStream is = null;
         try {
            is = fs.open(hadoopPath);
            TextFileParser stream = new TextFileParser(is);
            numVertices = stream.nextInt();
            numEdges = stream.nextInt();
         } finally {
            if (is != null) is.close();
         }
      } else {
         throw new RuntimeException("Metadata file must exist.");
      }

      // vlabels
      IntArrayList vlabels = new IntArrayList();
      boolean hasVlabels;
      hadoopPath = new Path(vlabelsFilePath);
      if (fs.exists(hadoopPath)) {
         hasVlabels = true;
         InputStream is = null;
         try {
            is = fs.open(hadoopPath);
            TextFileParser stream = new TextFileParser(is);
            for (int u = 0; u < numVertices; ++u) {
               while (!stream.skipNewLine()) {
                  vlabels.add(stream.nextInt());
               }
            }
         } finally {
            if (is != null) is.close();
         }

         if (vlabels.size() != numVertices) {
            throw new RuntimeException("Number of vertex labels differ." +
                    " Expected: " + numVertices +
                    " Found: " + vlabels.size() +
                    " " + vlabels);
         }

      } else {
         hasVlabels = false;
      }

      if (hasVlabels) {
         for (int u = 0; u < numVertices; ++u) {
            pattern.addVertexStandalone(vlabels.getu(u));
         }
      } else {
         for (int u = 0; u < numVertices; ++u) {
            pattern.addVertexStandalone();
         }
      }

      // elabels
      IntArrayList elabels = new IntArrayList();
      boolean hasElabels;
      hadoopPath = new Path(elabelsFilePath);
      if (fs.exists(hadoopPath)) {
         hasElabels = true;
         InputStream is = null;
         try {
            is = fs.open(hadoopPath);
            TextFileParser stream = new TextFileParser(is);
            for (int e = 0; e < numEdges; ++e) {
               while (!stream.skipNewLine()) {
                  elabels.add(stream.nextInt());
               }
            }
         } finally {
            if (is != null) is.close();
         }

         if (elabels.size() != numEdges) {
            throw new RuntimeException("Number of edge labels differ.");
         }

      } else {
         hasElabels = false;
      }

      // edge list
      hadoopPath = new Path(edgesFilePath);
      if (fs.exists(hadoopPath)) {
         InputStream is = null;
         try {
            is = fs.open(hadoopPath);
            TextFileParser stream = new TextFileParser(is);
            for (int e = 0; e < numEdges; ++e) {
               while (!stream.skipNewLine()) {
                  int src = stream.nextInt();
                  int dst = stream.nextInt();

                  if (src < 0 || dst >= numVertices
                          || dst < 0 || dst >= numVertices) {
                     throw new RuntimeException("Invalid edge: " + src + " " + dst);
                  }

                  PatternEdge edge =
                          PatternEdgePool.instance(hasElabels).createObject();

                  int srcLabel = hasVlabels ? vlabels.getu(src) : 1;
                  int dstLabel = hasVlabels ? vlabels.getu(dst) : 1;
                  edge.setSrcPos(src);
                  edge.setDestPos(dst);
                  edge.setSrcLabel(srcLabel);
                  edge.setDestLabel(dstLabel);

                  pattern.addEdgeStandalone(edge);
               }
            }
         } finally {
            if (is != null) is.close();
         }
      } else {
         throw new RuntimeException("Missing edges file");
      }

      pattern.setVertexLabeled(hasVlabels);
      pattern.setEdgeLabeled(hasElabels);
      pattern.setInduced(false);

      return pattern;
   }
}
