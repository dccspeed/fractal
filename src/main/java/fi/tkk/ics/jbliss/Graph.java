/* 
 * @(#)Graph.java
 *
 * Copyright 2007-2010 by Tommi Junttila.
 * Released under the GNU General Public License version 3.
 */

package fi.tkk.ics.jbliss;

import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.JBlissPattern;
import br.ufmg.cs.systems.fractal.pattern.PatternEdge;
import br.ufmg.cs.systems.fractal.pattern.PatternEdgeArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.ObjCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import cz.adamh.utils.NativeUtils;
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


    /**
     * Create a new undirected graph with no vertices or edges.
     */
    public Graph(JBlissPattern pattern) {
        this.pattern = pattern;
    }

    private MainGraph getMainGraph() {
       return pattern.getMainGraph();
    }

    private long createBliss() {
       MainGraph mainGraph = getMainGraph();
       IntArrayList vertices = pattern.getVertices();
       int numVertices = vertices.size();
       IntArrayList vertexLabels = new IntArrayList(numVertices);

       IntCursor vertexCursor = vertices.cursor();
       while (vertexCursor.moveNext()) {
          vertexLabels.add(mainGraph.getVertex(vertexCursor.elem()).getVertexLabel());
       }

       return createBliss(vertexLabels);
    }

    private long createEdgeBliss() {
       PatternEdgeArrayList edges = pattern.getEdges();
       int numEdges = edges.size();
       IntArrayList edgeLabels = new IntArrayList(numEdges);
      
       ObjCursor<PatternEdge> edgeCursor = edges.cursor();
       while (edgeCursor.moveNext()) {
          edgeLabels.add(edgeCursor.elem().getLabel());
       }

       return createEdgeBliss(edgeLabels);
    }

    private long createEdgeBliss(IntArrayList edgeLabels) {
       long bliss = create();
       assert bliss != 0;

       IntCursor labelCursor = edgeLabels.cursor();
       while (labelCursor.moveNext()) {
          _add_vertex(bliss, labelCursor.elem());
       }
       
       int numEdges = edgeLabels.size();
       PatternEdgeArrayList edges = pattern.getEdges();

       for (int i = 0; i < numEdges; ++i) {
          PatternEdge edge1 = edges.get(i);
          for (int j = i + 1; j < numEdges; ++j) {
             PatternEdge edge2 = edges.get(j);

             if (edge1.getSrcPos() == edge2.getSrcPos() ||
                   edge1.getSrcPos() == edge2.getDestPos() ||
                   edge1.getDestPos() == edge2.getSrcPos() ||
                   edge1.getDestPos() == edge2.getDestPos()) {
                _add_edge(bliss, i, j);
            }
          }
       }

       return bliss;

    }
	
      private long createBliss(IntArrayList vertexLabels) {
                //MainGraph mainGraph = getMainGraph();
		//IntArrayList vertices = pattern.getVertices();
		//int numVertices = vertices.size();
		int numVertices = vertexLabels.size();
		PatternEdgeArrayList edges = pattern.getEdges();

		long bliss = create();
		assert bliss != 0;

                IntCursor labelCursor = vertexLabels.cursor();
                while (labelCursor.moveNext()) {
                   _add_vertex(bliss, labelCursor.elem());
                }

		//IntCursor vertexCursor = vertices.cursor();

		//while (vertexCursor.moveNext()) {
		//	Vertex vertex = mainGraph.getVertex(vertexCursor.elem());
		//	_add_vertex(bliss, vertex.getVertexLabel());
		//}

		ObjCursor<PatternEdge> edgeCursor = edges.cursor();

		while (edgeCursor.moveNext()) {
			PatternEdge edge = edgeCursor.elem();

			if (edge.getSrcPos() >= numVertices || edge.getDestPos() >= numVertices) {
				throw new RuntimeException("Wrong (possibly old?) pattern edge found. Src (" + edge.getSrcPos() + "), Dst (" + edge.getDestPos() + ") or both are higher than numVertices (" + numVertices + ")");
			}
			_add_edge(bliss, edge.getSrcPos(), edge.getDestPos());
		}

		return bliss;
	}

	public void findAutomorphisms(Reporter reporter, Object reporter_param, IntArrayList vertexLabels) {
           if (vertexLabels == null) {
              findAutomorphisms(reporter, reporter_param);
              return;
           }

           long bliss = createBliss(vertexLabels);
           _reporter = reporter;
           _reporter_param = reporter_param;
           _find_automorphisms(bliss, _reporter);
           destroy(bliss);
           _reporter = null;
           _reporter_param = null;
        }

        public void findEdgeAutomorphisms(Reporter reporter, Object reporter_param, IntArrayList edgeLabels) {
           if (edgeLabels == null) {
              findEdgeAutomorphisms(reporter, reporter_param);
              return;
           }

           long bliss = createEdgeBliss(edgeLabels);
           _reporter = reporter;
           _reporter_param = reporter_param;
           isVertexIsomorphism = false;
           _find_automorphisms(bliss, _reporter);
           destroy(bliss);
           _reporter = null;
           _reporter_param = null;
           isVertexIsomorphism = true;
        }

	public void findAutomorphisms(Reporter reporter, Object reporter_param) {
		long bliss = createBliss();

		_reporter = reporter;
		_reporter_param = reporter_param;
		_find_automorphisms(bliss, _reporter);
		destroy(bliss);
		_reporter = null;
		_reporter_param = null;
	}

	public void findEdgeAutomorphisms(Reporter reporter, Object reporter_param) {
		long bliss = createEdgeBliss();

		_reporter = reporter;
		_reporter_param = reporter_param;
                isVertexIsomorphism = false;
		_find_automorphisms(bliss, _reporter);
		destroy(bliss);
		_reporter = null;
		_reporter_param = null;
                isVertexIsomorphism = true;
	}

	public void fillCanonicalLabeling(IntIntMap canonicalLabelling) {
		fillCanonicalLabeling(null, null, canonicalLabelling);
	}

	public void fillCanonicalLabeling(Reporter reporter, Object reporter_param, IntIntMap canonicalLabellling) {
		int numVertices = pattern.getNumberOfVertices();
		long bliss = createBliss();

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
					NativeUtils.loadLibraryFromJar("/libjbliss-mac.so");
				} else {
					throw new UnsupportedOperationException("Library not compiled for MAC " + systemBits + " bits");
				}
			}
			else if (SystemUtils.IS_OS_LINUX) {
				if (systemBits == 64) {
					NativeUtils.loadLibraryFromJar("/libjbliss-linux.so");
				} else {
					throw new UnsupportedOperationException("Library not compiled for Linux " + systemBits + " bits");
				}
			}
			else if (SystemUtils.IS_OS_WINDOWS) {
				if (systemBits == 64) {
					NativeUtils.loadLibraryFromJar("/libjbliss-win.dll");
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
