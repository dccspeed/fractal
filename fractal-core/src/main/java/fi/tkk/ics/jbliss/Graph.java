/**
 * Special acknowledgement to Tommi Junttila for changing the license of his
 * canonical labeling software. This code is derived from it.
 *
 * @(#)Graph.java
 *
 * Copyright 2007-2010 by Tommi Junttila.
 * Released under the GNU General Public License version 3.
 */

package fi.tkk.ics.jbliss;

import br.ufmg.cs.systems.fractal.pattern.JBlissPattern;
import br.ufmg.cs.systems.fractal.pattern.LabelledPatternEdge;
import br.ufmg.cs.systems.fractal.pattern.PatternEdge;
import br.ufmg.cs.systems.fractal.pattern.PatternEdgeArrayList;
import br.ufmg.cs.systems.fractal.util.FractalNativeUtils;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.ObjCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import org.apache.commons.lang3.SystemUtils;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * An undirected graph.
 * Vertices can be colored (with integers) and self-loops are allowed
 * but multiple edges between vertices are ignored.
 *
 * @author Tommi Junttila
 */
public class Graph<V extends Comparable> {
   protected JBlissPattern pattern;
   protected Reporter       _reporter;
   protected Object         _reporter_param;
   protected boolean isVertexIsomorphism = true;

   protected void _report(int[] aut)
   {
      if(_reporter == null)
         return;

      int numWords;
      if (isVertexIsomorphism) {
         numWords = pattern.getNumberOfVertices();
      } else {
         numWords = pattern.getNumberOfEdges();
      }

      HashIntIntMap real_aut = HashIntIntMaps.newMutableMap(numWords);

      for (int i = 0; i < numWords; ++i) {
         real_aut.put(i, aut[i]);
      }

      _reporter.report(real_aut, _reporter_param);
   }

   /* The internal JNI interface to true bliss */
   public static native long create();
   public static native void destroy(long true_bliss);
   public static native int _add_vertex(long true_bliss, int color);
   public static native void _add_edge(long true_bliss, int v1, int v2);
   protected native void _find_automorphisms(long true_bliss, Reporter r);
   public static native int[] _canonical_labeling(long true_bliss, Reporter r);

   private int mapEdgeLabelToVertexLabel(int label) {
      return - label - 1;
   }

   /**
    * Create a new undirected graph with no vertices or edges.
    */
   public Graph(JBlissPattern pattern) {
      this.pattern = pattern;
   }

   private long createBlissVertexEdgeLabeled() {
      int numVertices = pattern.getNumberOfVertices();
      int numEdges = pattern.getNumberOfEdges();
      IntArrayList vertexEdgeLabels = new IntArrayList(numVertices);

      if (numVertices == 1) {
         vertexEdgeLabels.add(pattern.getFirstVertexLabel());
         return createBlissVertexLabeled(vertexEdgeLabels);
      }

      // add original vertices
      for (int i = 0; i < numVertices; ++i) {
         vertexEdgeLabels.add(-1);
      }

      // add new vertices to account for edge labels
      for (int i = 0; i < numEdges; ++i) {
         vertexEdgeLabels.add(-1);
      }

      // set original vertex labels
      PatternEdgeArrayList edges = pattern.getEdges();
      for (int i = 0; i < edges.size(); ++i) {
         PatternEdge pedge = edges.getu(i);
         vertexEdgeLabels.setu(pedge.getSrcPos(), pedge.getSrcLabel());
         vertexEdgeLabels.setu(pedge.getDestPos(), pedge.getDestLabel());
      }

      // set new vertex labels representing edge labels
      for (int i = 0; i < numEdges; ++i) {
         LabelledPatternEdge pedge = (LabelledPatternEdge) edges.getu(i);
         // IMPORTANT: edge labels and vertex labels should not overlap, thus
         // this internal mapping
         int newVertexLabel = mapEdgeLabelToVertexLabel(pedge.getLabel());
         vertexEdgeLabels.setu(numVertices + i, newVertexLabel);
      }

      return createBlissVertexEdgeLabeled(vertexEdgeLabels);
   }

   private long createBlissVertexEdgeLabeled(IntArrayList vertexLabels,
                                             IntArrayList edgeLabels) {
      int numVertices = pattern.getNumberOfVertices();

      if (numVertices == 1) {
         vertexLabels.add(pattern.getFirstVertexLabel());
         return createBlissVertexLabeled(vertexLabels);
      }

      IntArrayList vertexEdgeLabels = new IntArrayList(vertexLabels);

      for (int i = 0; i < edgeLabels.size(); ++i) {
         vertexEdgeLabels.add(mapEdgeLabelToVertexLabel(edgeLabels.get(i)));
      }

      return createBlissVertexEdgeLabeled(vertexEdgeLabels);
   }

   private long createBlissVertexEdgeLabeled(IntArrayList vertexLabels) {
      int numVertices = pattern.getNumberOfVertices();
      PatternEdgeArrayList edges = pattern.getEdges();
      long bliss = create();
      assert bliss != 0;

      // add vertices to bliss
      IntCursor labelCursor = vertexLabels.cursor();
      while (labelCursor.moveNext()) {
         _add_vertex(bliss, labelCursor.elem());
      }

      // for each edge, we add two edges
      ObjCursor<PatternEdge> edgeCursor = edges.cursor();
      int newVertexId = numVertices;
      while (edgeCursor.moveNext()) {
         PatternEdge edge = edgeCursor.elem();

         if (edge.getSrcPos() >= numVertices || edge.getDestPos() >= numVertices) {
            throw new RuntimeException("Wrong (possibly old?) pattern edge " +
                    "found. Src (" + edge.getSrcPos() + "), Dst (" + edge.getDestPos() + ") or both are higher than numVertices (" + numVertices + ")" +
                    " " + pattern + " vertices=" + pattern.getVertices());
         }

         _add_edge(bliss, edge.getSrcPos(), newVertexId);
         _add_edge(bliss, newVertexId, edge.getDestPos());
         ++newVertexId;
      }

      return bliss;
   }

   private long createBlissVertexLabeled() {
      int numVertices = pattern.getNumberOfVertices();
      IntArrayList vertexLabels = new IntArrayList(numVertices);
      if (numVertices == 1) {
         vertexLabels.add(pattern.getFirstVertexLabel());
         return createBlissVertexLabeled(vertexLabels);
      }

      for (int i = 0; i < numVertices; ++i) {
         vertexLabels.add(-1);
      }

      PatternEdgeArrayList edges = pattern.getEdges();
      for (int i = 0; i < edges.size(); ++i) {
         PatternEdge pedge = edges.getu(i);
         vertexLabels.setu(pedge.getSrcPos(), pedge.getSrcLabel());
         vertexLabels.setu(pedge.getDestPos(), pedge.getDestLabel());
      }

      return createBlissVertexLabeled(vertexLabels);
   }

   private long createBlissVertexLabeled(IntArrayList vertexLabels) {
      int numVertices = vertexLabels.size();
      PatternEdgeArrayList edges = pattern.getEdges();

      long bliss = create();
      assert bliss != 0;

      IntCursor labelCursor = vertexLabels.cursor();
      while (labelCursor.moveNext()) {
         _add_vertex(bliss, labelCursor.elem());
      }

      ObjCursor<PatternEdge> edgeCursor = edges.cursor();

      while (edgeCursor.moveNext()) {
         PatternEdge edge = edgeCursor.elem();

         if (edge.getSrcPos() >= numVertices || edge.getDestPos() >= numVertices) {
            throw new RuntimeException("Wrong (possibly old?) pattern edge " +
                    "found. Src (" + edge.getSrcPos() + "), Dst (" + edge.getDestPos() + ") or both are higher than numVertices (" + numVertices + ")" +
                    " " + pattern + " vertices=" + pattern.getVertices());
         }
         _add_edge(bliss, edge.getSrcPos(), edge.getDestPos());
      }

      return bliss;
   }

   public void findAutomorphisms(Reporter reporter, Object reporter_param,
                                 IntArrayList vertexLabels,
                                 IntArrayList edgeLabels) {
      long bliss = 0;
      if (vertexLabels == null) {
         bliss = createBlissVertexLabeled();
      } else if (edgeLabels == null) {
         bliss = createBlissVertexLabeled(vertexLabels);
      } else {
         bliss = createBlissVertexEdgeLabeled(vertexLabels, edgeLabels);
      }

      _reporter = reporter;
      _reporter_param = reporter_param;
      _find_automorphisms(bliss, _reporter);
      destroy(bliss);
      _reporter = null;
      _reporter_param = null;
   }

   public void fillCanonicalLabeling(IntIntMap canonicalLabelling) {
      long bliss;
      if (pattern.edgeLabeled()) {
         bliss = createBlissVertexEdgeLabeled();
      } else {
         bliss = createBlissVertexLabeled();
      }

      fillCanonicalLabeling(null, null, canonicalLabelling, bliss);
   }

   public void fillCanonicalLabeling(Reporter reporter,
                                     Object reporter_param,
                                     IntIntMap canonicalLabellling,
                                     long bliss) {

      int numVertices = pattern.getNumberOfVertices();

      _reporter = reporter;
      _reporter_param = reporter_param;
      int[] cf = _canonical_labeling(bliss, _reporter);
      destroy(bliss);

      canonicalLabellling.clear();
      for (int i = 0; i < numVertices; ++i) {
         canonicalLabellling.put(i, cf[i]);
      }

      _reporter = null;
      _reporter_param = null;
   }

   static {
      int systemBits;

      try {
         Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
         unsafeField.setAccessible(true);
         Unsafe unsafe = (Unsafe) unsafeField.get(null);

         // Get system bits
         systemBits = unsafe.addressSize() * 8;

         if (SystemUtils.IS_OS_MAC || SystemUtils.IS_OS_MAC_OSX) {
            if (systemBits == 64) {
               FractalNativeUtils.loadNativeLibFromJar("libjbliss-mac.so");
            } else {
               throw new UnsupportedOperationException("Library not compiled for MAC " + systemBits + " bits");
            }
         }
         else if (SystemUtils.IS_OS_LINUX) {
            if (systemBits == 64) {
               FractalNativeUtils.loadNativeLibFromJar("libjbliss-linux.so");
            } else {
               throw new UnsupportedOperationException("Library not compiled for Linux " + systemBits + " bits");
            }
         }
         else if (SystemUtils.IS_OS_WINDOWS) {
            if (systemBits == 64) {
               FractalNativeUtils.loadNativeLibFromJar("libjbliss-win.dll");
            } else {
               throw new UnsupportedOperationException("Library not compiled for Windows " + systemBits + " bits");
            }
         }
         else {
            throw new UnsupportedOperationException("Library not compiled for " + SystemUtils.OS_NAME);
         }
      } catch (IOException | IllegalAccessException | NoSuchFieldException e) {
         throw new RuntimeException("Unable to load correct jbliss library for system: " + SystemUtils.OS_NAME + " " + SystemUtils.OS_ARCH + " " + SystemUtils.OS_NAME, e);
      }
   }
}
