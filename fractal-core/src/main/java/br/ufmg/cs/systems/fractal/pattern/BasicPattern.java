package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.Edge;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.graph.Vertex;
import br.ufmg.cs.systems.fractal.pattern.pool.PatternEdgePool;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntCollectionAddConsumer;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.ObjCursor;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMapFactory;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.collect.set.IntSet;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.StringTokenizer;

public abstract class BasicPattern implements Pattern {
    private static final Logger LOG = Logger.getLogger(BasicPattern.class);

    protected int configurationId;
    protected Configuration configuration;
    protected boolean isGraphEdgeLabelled;

    protected HashIntIntMapFactory positionMapFactory = 
       HashIntIntMaps.getDefaultFactory().withDefaultValue(-1);

    // Basic structure {{
    private IntArrayList vertices;
    private PatternEdgeArrayList edges;
    // K = vertex id, V = vertex position
    private IntIntMap vertexPositions;
    // }}

    // Incremental building {{
    private IntArrayList previousWords; // TODO: is it previous or current ?
    private int numVerticesAddedFromPrevious;
    private int numAddedEdgesFromPrevious;
    // }}

    // Isomorphisms {{
    private VertexPositionEquivalences vertexPositionEquivalences;
    private EdgePositionEquivalences edgePositionEquivalences;
    private IntIntMap canonicalLabelling;
    private ObjArrayList<IntArrayList> vsymmetryBreaker;
    private ObjArrayList<IntArrayList> esymmetryBreaker;
    // }}

    // Others {{
    private PatternEdgePool patternEdgePool;

    protected volatile boolean dirtyVertexPositionEquivalences;
    protected volatile boolean dirtyEdgePositionEquivalences;
    protected volatile boolean dirtyCanonicalLabelling;

    protected IntCollectionAddConsumer intAddConsumer = new IntCollectionAddConsumer();
    // }}

    public BasicPattern() {
        vertices = new IntArrayList();
        vertexPositions = positionMapFactory.newMutableMap();
        previousWords = new IntArrayList();
        reset();
    }

    public BasicPattern(BasicPattern basicPattern) {
        this();

        intAddConsumer.setCollection(vertices);
        basicPattern.vertices.forEach(intAddConsumer);

        isGraphEdgeLabelled = basicPattern.getConfig().isGraphEdgeLabelled();

        edges = createPatternEdgeArrayList(isGraphEdgeLabelled);

        patternEdgePool = PatternEdgePool.instance(isGraphEdgeLabelled);

        edges.ensureCapacity(basicPattern.edges.size());

        for (PatternEdge otherEdge : basicPattern.edges) {
            edges.add(createPatternEdge(otherEdge));
        }

        vertexPositions.putAll(basicPattern.vertexPositions);
    }

    @Override
    public void init(Configuration config) {
        configurationId = config.getId();

        configuration = config;

        isGraphEdgeLabelled = config.isGraphEdgeLabelled();

        if (edges == null) {
           edges = createPatternEdgeArrayList(isGraphEdgeLabelled);
        }

        if (patternEdgePool == null) {
           patternEdgePool = PatternEdgePool.instance(isGraphEdgeLabelled);
        }
    }

    @Override
    public void reset() {
        if (vertices != null) {
           vertices.clear();
        }

        if (patternEdgePool != null) {
           patternEdgePool.reclaimObjects(edges);
        }

        if (edges != null) {
           edges.clear();
        }

        if (vertexPositions != null) {
           vertexPositions.clear();
        }

        setDirty();

        resetIncremental();
    }

    @Override
    public Configuration getConfig() {
       //return Configuration.get(configurationId);
       return configuration;
    }

    private void resetIncremental() {
        numVerticesAddedFromPrevious = 0;
        numAddedEdgesFromPrevious = 0;
        if (previousWords != null) {
           previousWords.clear();
        }
    }

    @Override
    public void setSubgraph(Subgraph subgraph) {
        try {
            if (canDoIncremental(subgraph)) {
                setSubgraphIncremental(subgraph);
            } else {
                setSubgraphFromScratch(subgraph);
            }
        } catch (RuntimeException e) {
            LOG.error("subgraph: " + subgraph + " " + e);
            throw e;
        }
    }

    /**
     * Reset everything and do everything from scratch.
     *
     * @param subgraph
     */
    private void setSubgraphFromScratch(Subgraph subgraph) {
        reset();

        int numEdgesInSubgraph = subgraph.getNumEdges();
        int numVerticesInSubgraph = subgraph.getNumVertices();

        if (numEdgesInSubgraph == 0 && numVerticesInSubgraph == 0) {
            return;
        }

        ensureCanStoreNewVertices(numVerticesInSubgraph);
        ensureCanStoreNewEdges(numEdgesInSubgraph);

        IntArrayList SubgraphVertices = subgraph.getVertices();

        for (int i = 0; i < numVerticesInSubgraph; ++i) {
            addVertex(SubgraphVertices.getUnchecked(i));
        }

        numVerticesAddedFromPrevious = subgraph.numVerticesAdded();

        IntArrayList SubgraphEdges = subgraph.getEdges();

        for (int i = 0; i < numEdgesInSubgraph; ++i) {
            addEdge(SubgraphEdges.getUnchecked(i));
        }

        numAddedEdgesFromPrevious = subgraph.numEdgesAdded();

        updateUsedSubgraphFromScratch(subgraph);
    }

    private void updateUsedSubgraphFromScratch(Subgraph subgraph) {
        previousWords.clear();

        int SubgraphNumWords = subgraph.getNumWords();

        previousWords.ensureCapacity(SubgraphNumWords);

        IntArrayList words = subgraph.getWords();

        for (int i = 0; i < SubgraphNumWords; i++) {
            previousWords.add(words.getUnchecked(i));
        }
    }

    private void resetToPrevious() {
        removeLastNEdges(numAddedEdgesFromPrevious);
        removeLastNVertices(numVerticesAddedFromPrevious);
        setDirty();
    }

    private void removeLastNEdges(int n) {
        int targetI = edges.size() - n;

        for (int i = edges.size() - 1; i >= targetI; --i) {
            patternEdgePool.reclaimObject(edges.remove(i));
        }
    }

    private void removeLastNVertices(int n) {
        int targetI = vertices.size() - n;

        for (int i = vertices.size() - 1; i >= targetI; --i) {
            try {
                vertexPositions.remove(vertices.getUnchecked(i));
            } catch (IllegalArgumentException e) {
                System.err.println(e.toString());
                System.err.println("i=" + i);
                System.err.println("targetI=" + targetI);
                throw e;
            }
        }

        vertices.removeLast(n);
    }

    /**
     * Only the last word has changed, so skipped processing for the previous vertices.
     *
     * @param subgraph
     */
    private void setSubgraphIncremental(Subgraph subgraph) {
        resetToPrevious();

        numVerticesAddedFromPrevious = subgraph.numVerticesAdded();
        numAddedEdgesFromPrevious = subgraph.numEdgesAdded();

        ensureCanStoreNewVertices(numVerticesAddedFromPrevious);
        ensureCanStoreNewEdges(numAddedEdgesFromPrevious);

        IntArrayList SubgraphVertices = subgraph.getVertices();
        int numVerticesInSubgraph = subgraph.getNumVertices();
        for (int i = (numVerticesInSubgraph - numVerticesAddedFromPrevious); i < numVerticesInSubgraph; ++i) {
            addVertex(SubgraphVertices.getUnchecked(i));
        }

        IntArrayList SubgraphEdges = subgraph.getEdges();
        int numEdgesInSubgraph = subgraph.getNumEdges();
        for (int i = (numEdgesInSubgraph - numAddedEdgesFromPrevious); i < numEdgesInSubgraph; ++i) {
            addEdge(SubgraphEdges.getUnchecked(i));
        }

        updateUsedSubgraphIncremental(subgraph);
    }

    /**
     * By default only the last word changed.
     *
     * @param subgraph
     */
    private void updateUsedSubgraphIncremental(Subgraph subgraph) {
        previousWords.setUnchecked(previousWords.size() - 1, subgraph.getWords().getUnchecked(previousWords.size() - 1));
    }

    private void ensureCanStoreNewEdges(int numAddedEdgesFromPrevious) {
        int newNumEdges = edges.size() + numAddedEdgesFromPrevious;

        edges.ensureCapacity(newNumEdges);
    }

    private void ensureCanStoreNewVertices(int numVerticesAddedFromPrevious) {
        int newNumVertices = vertices.size() + numVerticesAddedFromPrevious;

        vertices.ensureCapacity(newNumVertices);
        vertexPositions.ensureCapacity(newNumVertices);
    }

    /**
     * Can do incremental only if the last word is different.
     *
     * @param subgraph
     * @return
     */
    private boolean canDoIncremental(Subgraph subgraph) {
        if (subgraph.getNumWords() == 0 || previousWords.size() != subgraph.getNumWords()) {
            return false;
        }

        // Maximum we want 1 change (which we know by default that it exists in the last position).
        // so we check
        final IntArrayList words = subgraph.getWords();
        for (int i = previousWords.size() - 2; i >= 0; i--) {
            if (words.getUnchecked(i) != previousWords.getUnchecked(i)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int getNumberOfVertices() {
        return vertices.getSize();
    }

    @Override
    public boolean addEdge(int edgeId) {
        Edge edge = getMainGraph().getEdge(edgeId);

        return addEdge(edge);
    }

    public boolean addEdge(Edge edge) {
        int srcId = edge.getSourceId();
        int srcPos = addVertex(srcId);
        int dstPos = addVertex(edge.getDestinationId());

        PatternEdge patternEdge = createPatternEdge(edge, srcPos, dstPos, srcId);

        return addEdge(patternEdge);
    }

    public boolean addEdge(PatternEdge edge) {
        // TODO: Remove when we have directed edges
        if (edge.getSrcPos() > edge.getDestPos()) {
            edge.invert();
        }

        edges.add(edge);

        setDirty();

        return true;
    }

    public int addVertex(int vertexId) {
        int pos = vertexPositions.get(vertexId);

        if (pos == -1) {
            pos = vertices.size();
            vertices.add(vertexId);
            vertexPositions.put(vertexId, pos);
            setDirty();
        }

        return pos;
    }

    protected void setDirty() {
        dirtyCanonicalLabelling = true;
        dirtyVertexPositionEquivalences = true;
        dirtyEdgePositionEquivalences = true;
    }

    @Override
    public int getNumberOfEdges() {
        return edges.size();
    }

    @Override
    public IntArrayList getVertices() {
        return vertices;
    }

    @Override
    public PatternEdgeArrayList getEdges() {
        return edges;
    }

    @Override
    public VertexPositionEquivalences getVertexPositionEquivalences() {
        return getVertexPositionEquivalences(null);
    }

    @Override
    public VertexPositionEquivalences getVertexPositionEquivalences(IntArrayList vertexLabels) {
        if (dirtyVertexPositionEquivalences) {
            synchronized (this) {
                if (dirtyVertexPositionEquivalences) {
                    if (vertexPositionEquivalences == null) {
                        vertexPositionEquivalences = new VertexPositionEquivalences();
                    }

                    vertexPositionEquivalences.setNumVertices(getNumberOfVertices());
                    vertexPositionEquivalences.clear();

                    fillVertexPositionEquivalences(vertexPositionEquivalences, vertexLabels);

                    dirtyVertexPositionEquivalences = false;
                }
            }
        }

        return vertexPositionEquivalences;
    }

    @Override
    public EdgePositionEquivalences getEdgePositionEquivalences() {
        return getEdgePositionEquivalences(null);
    }

    @Override
    public EdgePositionEquivalences getEdgePositionEquivalences(IntArrayList edgeLabels) {
       if (dirtyEdgePositionEquivalences) {
          synchronized (this) {
             if (dirtyEdgePositionEquivalences) {
                if (edgePositionEquivalences == null) {
                   edgePositionEquivalences = new EdgePositionEquivalences();
                }

                edgePositionEquivalences.setNumEdges(getNumberOfEdges());
                edgePositionEquivalences.clear();

                fillEdgePositionEquivalences(edgePositionEquivalences, edgeLabels);

                dirtyEdgePositionEquivalences = false;
             }
          }
       }

       return edgePositionEquivalences;
    }

    private int[][] vsymmetryBreakerMatrix() {
       int numVertices = getNumberOfVertices();
       int[][] symmetryBreaker = new int[numVertices][numVertices];
       IntArrayList vertexLabels = new IntArrayList(numVertices);

       IntCursor vertexCursor = vertices.cursor();
       while (vertexCursor.moveNext()) {
          vertexLabels.add(getConfig().getMainGraph().
                getVertex(vertexCursor.elem()).getVertexLabel());
       }

       vsymmetryBreakerRec(this.copy(), symmetryBreaker, vertexLabels, -1);

       StringBuffer sb = new StringBuffer();
       sb.append("symmetryBreaker {");
       for (int i = 0; i < numVertices; ++i) {
          for (int j = 0; j < numVertices; ++j) {
             sb.append(String.format("%2s ", symmetryBreaker[i][j]));
          }
          sb.append("\n");
       }
       sb.append("}");

       LOG.info(sb.toString());

       return symmetryBreaker;
    }

    public ObjArrayList<IntArrayList> vsymmetryBreaker() {
       if (vsymmetryBreaker == null) {
          synchronized (this) {
             if (vsymmetryBreaker == null) {
                int[][] symmetryBreaker = vsymmetryBreakerMatrix();

                vsymmetryBreaker = computeVsymmetryBreaker(symmetryBreaker);
             }
          }

          LOG.info("vsymmetryBreaker " + vsymmetryBreaker);
       }

       return vsymmetryBreaker;
    }
    
    public ObjArrayList<IntArrayList> computeVsymmetryBreaker(int[][] symmetryBreaker) {
       ObjArrayList<IntArrayList> vsymmetryBreaker = new ObjArrayList<IntArrayList>(
             symmetryBreaker.length);

       for (int i = 0; i < symmetryBreaker.length; ++i) {
          vsymmetryBreaker.add(new IntArrayList());
          for (int j = 0; j < i; ++j) {
             if (symmetryBreaker[i][j] == 1) {
                vsymmetryBreaker.get(i).add(j);
             }
          }
       }

       return vsymmetryBreaker;
    }

  @Override
    public boolean testSymmetryBreakerPos(Subgraph subgraph, int i) {
       ObjArrayList<IntArrayList> symmetryBreaker = vsymmetryBreaker();
       IntArrayList vertices = subgraph.getVertices();
       int targetVertex = vertices.get(i);
       IntCursor sbCur = symmetryBreaker.get(i).cursor();
       while (sbCur.moveNext()) {
          if (targetVertex < vertices.get(sbCur.elem())) {
             return false;
          }
       }
       return true;
    }

    @Override
    public boolean testSymmetryBreakerExt(
            Subgraph subgraph, int targetVertex) {
       ObjArrayList<IntArrayList> symmetryBreaker = vsymmetryBreaker();
       IntArrayList vertices = subgraph.getVertices();
       IntCursor sbCur = symmetryBreaker.get(subgraph.getNumVertices()).cursor();
       while (sbCur.moveNext()) {
          if (targetVertex < vertices.get(sbCur.elem())) {
             return false;
          }
       }
       return true;
    }

    @Override
    public int sbLowerBound(Subgraph subgraph, int pos) {
       IntArrayList conditions = vsymmetryBreaker().get(pos);
       IntArrayList vertices = subgraph.getVertices();
       int numConditions = conditions.size();
       int lowerBound = Integer.MIN_VALUE;
       for (int i = 0; i < numConditions; ++i) {
          lowerBound = Math.max(lowerBound, vertices.get(conditions.get(i)));
       }

       return lowerBound;
    }

    private static void vsymmetryBreakerRec(Pattern pattern, int[][] sbreaker,
          IntArrayList vertexLabels, int nextLabel) {
       int numVertices = sbreaker.length;
       VertexPositionEquivalences vertexPositionEquivalences =
          pattern.getVertexPositionEquivalences(vertexLabels);

       LOG.info(String.format(
                "symmetryBreakerRec{pattern=%s,vertices=%s,vertexLabels=%s," +
                "vertexEquivalences=%s,nextLabel=%s}\n",
                pattern, pattern.getVertices(), vertexLabels,
                vertexPositionEquivalences, nextLabel));

       IntSet equivalenceToBreakSet = null;
       for (int i = 0; i < numVertices; ++i) {
          IntSet eq = vertexPositionEquivalences.getEquivalences(i);
          if (equivalenceToBreakSet == null ||
                eq.size() > equivalenceToBreakSet.size()) {
             equivalenceToBreakSet = eq;
          }
       }

       if (equivalenceToBreakSet.size() > 1) {
          IntArrayList equivalenceToBreak = new IntArrayList(
                equivalenceToBreakSet.size());
          equivalenceToBreak.addAll(equivalenceToBreakSet);
          equivalenceToBreak.sort();
          IntCursor cur = equivalenceToBreak.cursor();
          cur.moveNext();
          int fixed = cur.elem();
          while (cur.moveNext()) {
             int elem = cur.elem();
             sbreaker[fixed][elem] = -1;
             sbreaker[elem][fixed] = 1;
          }

          vertexLabels.set(fixed, nextLabel);

          // recursive call
          vsymmetryBreakerRec(pattern.copy(), sbreaker, vertexLabels, --nextLabel);
       }
    }

    public int[][] esymmetryBreaker() {
       int numEdges = getNumberOfEdges();
       int[][] symmetryBreaker = new int[numEdges][numEdges];
       IntArrayList edgeLabels = new IntArrayList(numEdges);

       ObjCursor<PatternEdge> edgeCursor = edges.cursor();
       while (edgeCursor.moveNext()) {
          edgeLabels.add(edgeCursor.elem().getLabel());
       }
    
       esymmetryBreakerRec(this.copy(), symmetryBreaker, edgeLabels, -1);

       StringBuffer sb = new StringBuffer();
       sb.append("symmetryBreaker {");
       for (int i = 0; i < numEdges; ++i) {
          for (int j = 0; j < numEdges; ++j) {
             sb.append(String.format("%2s ", symmetryBreaker[i][j]));
          }
          sb.append("\n");
       }
       sb.append("}");

       LOG.info(sb.toString());

       return symmetryBreaker;
    }

    private static void esymmetryBreakerRec(Pattern pattern, int[][] sbreaker,
          IntArrayList edgeLabels, int nextLabel) {
       int numEdges = sbreaker.length;
       EdgePositionEquivalences edgePositionEquivalences =
          pattern.getEdgePositionEquivalences(edgeLabels);

       LOG.info(String.format(
                "symmetryBreakerRec{pattern=%s,vertices=%s,edgeLabels=%s," +
                "edgeEquivalences=%s,nextLabel=%s}\n",
                pattern, pattern.getVertices(), edgeLabels,
                edgePositionEquivalences, nextLabel));

       IntSet equivalenceToBreakSet = null;
       for (int i = 0; i < numEdges; ++i) {
          IntSet eq = edgePositionEquivalences.getEquivalences(i);
          if (equivalenceToBreakSet == null || eq.size() > equivalenceToBreakSet.size()) {
             equivalenceToBreakSet = eq;
          }
       }


       if (equivalenceToBreakSet.size() > 1) {
          IntArrayList equivalenceToBreak = new IntArrayList(
                equivalenceToBreakSet.size());
          equivalenceToBreak.addAll(equivalenceToBreakSet);
          equivalenceToBreak.sort();
          IntCursor cur = equivalenceToBreak.cursor();
          cur.moveNext();
          int fixed = cur.elem();
          while (cur.moveNext()) {
             int elem = cur.elem();
             sbreaker[fixed][elem] = -1;
             sbreaker[elem][fixed] = 1;
          }

          edgeLabels.set(fixed, nextLabel);

          // recursive call
          esymmetryBreakerRec(pattern.copy(), sbreaker, edgeLabels, --nextLabel);
       }
    }
    
    @Override
    public void readSymmetryBreakingConditions(String path) throws IOException {
       int[][] symmetryBreaker;
       try {
          symmetryBreaker = readFromFile(Paths.get(path));
       } catch (IOException e) {
          symmetryBreaker = readFromHdfs(new org.apache.hadoop.fs.Path(path));
       }

       vsymmetryBreaker = computeVsymmetryBreaker(symmetryBreaker);
    }

    private int[][] readFromFile(Path filePath) throws IOException {
       InputStream is = Files.newInputStream(filePath);
       int[][] sbreaker = readFromInputStream(is);
       is.close();
       return sbreaker;
    }

    private int[][] readFromHdfs(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
      org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(
            new org.apache.hadoop.conf.Configuration());
      InputStream is = fs.open(hdfsPath);
      int[][] sbreaker = readFromInputStream(is);
      is.close();
      return sbreaker;
    }

    private int[][] readFromInputStream(InputStream is) {
       int[][] sbreaker;
       try {
          BufferedReader reader = new BufferedReader(
                new InputStreamReader(new BOMInputStream(is)));

          String line = reader.readLine();         
          StringTokenizer tokenizer = new StringTokenizer(line);
          int numVertices = Integer.parseInt(tokenizer.nextToken());

          sbreaker = new int[numVertices][numVertices];

          line = reader.readLine();

          while (line != null) {
             tokenizer = new StringTokenizer(line);

             int pos1 = Integer.parseInt(tokenizer.nextToken());
             int pos2 = Integer.parseInt(tokenizer.nextToken());

             sbreaker[pos1][pos2] = -1;
             sbreaker[pos2][pos1] = 1;

             line = reader.readLine();
          }
       } catch (IOException e) {
          throw new RuntimeException(e);
       }

       return sbreaker;
    }

    protected abstract void fillVertexPositionEquivalences(
          VertexPositionEquivalences vertexPositionEquivalences,
          IntArrayList vertexLabels);
    
    protected abstract void fillEdgePositionEquivalences(
          EdgePositionEquivalences edgePositionEquivalences,
          IntArrayList edgeLabels);
    
    @Override
    public IntIntMap getCanonicalLabeling() {
        if (dirtyCanonicalLabelling) {
            synchronized (this) {
                if (dirtyCanonicalLabelling) {
                    if (canonicalLabelling == null) {
                        canonicalLabelling = HashIntIntMaps.newMutableMap(
                              getNumberOfVertices());
                    }

                    canonicalLabelling.clear();

                    fillCanonicalLabelling(canonicalLabelling);

                    dirtyCanonicalLabelling = false;
                }
            }
        }

        return canonicalLabelling;
    }

    protected abstract void fillCanonicalLabelling(IntIntMap canonicalLabelling);

    @Override
    public boolean turnCanonical() {
        resetIncremental();

        IntIntMap canonicalLabelling = getCanonicalLabeling();

        IntIntCursor canonicalLabellingCursor = canonicalLabelling.cursor();

        boolean allEqual = true;

        while (canonicalLabellingCursor.moveNext()) {
            int oldPos = canonicalLabellingCursor.key();
            int newPos = canonicalLabellingCursor.value();

            if (oldPos != newPos) {
                allEqual = false;
            }
        }

        if (allEqual) {
            edges.sort();
            return false;
        }

        IntArrayList oldVertices = new IntArrayList(vertices);

        for (int i = 0; i < vertices.size(); ++i) {
            int newPos = canonicalLabelling.get(i);

            // If position didn't change, do nothing
            if (newPos == i) {
                continue;
            }

            int vertexId = oldVertices.getUnchecked(i);
            vertices.setUnchecked(newPos, vertexId);

            vertexPositions.put(vertexId, newPos);
        }

        for (int i = 0; i < edges.size(); ++i) {
            PatternEdge edge = edges.get(i);

            int srcPos = edge.getSrcPos();
            int dstPos = edge.getDestPos();

            int convertedSrcPos = canonicalLabelling.get(srcPos);
            int convertedDstPos = canonicalLabelling.get(dstPos);

            if (convertedSrcPos < convertedDstPos) {
                edge.setSrcPos(convertedSrcPos);
                edge.setDestPos(convertedDstPos);
            } else {
                // If we changed the position of source and destination due to
                // relabel, we also have to change the labels to match this
                // change.
                int tmp = edge.getSrcLabel();
                edge.setSrcPos(convertedDstPos);
                edge.setSrcLabel(edge.getDestLabel());
                edge.setDestPos(convertedSrcPos);
                edge.setDestLabel(tmp);
            }
        }

        edges.sort();

        return true;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(isGraphEdgeLabelled);
        dataOutput.writeInt(configurationId);
        edges.write(dataOutput);
        vertices.write(dataOutput);

        if (vsymmetryBreaker != null) {
           dataOutput.writeBoolean(true);
           dataOutput.writeInt(vsymmetryBreaker.size());
           for (int i = 0; i < vsymmetryBreaker.size(); ++i) {
              vsymmetryBreaker.getUnchecked(i).write(dataOutput);
           }
        } else {
           dataOutput.writeBoolean(false);
        }
    }

    @Override
    public void writeExternal(ObjectOutput objOutput) throws IOException {
       write (objOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        reset();

        isGraphEdgeLabelled = dataInput.readBoolean();

        //init(Configuration.get(dataInput.readInt()));
        configurationId = dataInput.readInt();
        configuration = Configuration.get(configurationId);

        if (edges == null) {
           edges = createPatternEdgeArrayList(isGraphEdgeLabelled);
        }

        if (patternEdgePool == null) {
           patternEdgePool = PatternEdgePool.instance(isGraphEdgeLabelled);
        }

        edges.readFields(dataInput);
        vertices.readFields(dataInput);

        for (int i = 0; i < vertices.size(); ++i) {
            vertexPositions.put(vertices.getUnchecked(i), i);
        }

        boolean hasVsymmetryBreaker = dataInput.readBoolean();
        if (hasVsymmetryBreaker) {
           int size = dataInput.readInt();
           vsymmetryBreaker = new ObjArrayList<IntArrayList>(size);
           for (int i = 0; i < size; ++i) {
              IntArrayList conds = new IntArrayList();
              conds.readFields(dataInput);
              vsymmetryBreaker.add(conds);
           }
        }
    }

    @Override
    public void readExternal(ObjectInput objInput) throws IOException, ClassNotFoundException {
       readFields(objInput);
    }

    protected PatternEdgeArrayList createPatternEdgeArrayList(boolean areEdgesLabelled) {
        return new PatternEdgeArrayList(areEdgesLabelled);
    }

    protected PatternEdge createPatternEdge(Edge edge, int srcPos, int dstPos, int srcId) {
        PatternEdge patternEdge = patternEdgePool.createObject();

        patternEdge.setFromEdge(getConfig().getMainGraph(),
              edge, srcPos, dstPos, srcId);

        return patternEdge;
    }

    protected PatternEdge createPatternEdge(PatternEdge otherEdge) {
        PatternEdge patternEdge = patternEdgePool.createObject();

        patternEdge.setFromOther(otherEdge);

        return patternEdge;
    }

    @Override
    public String toString() {
        return toOutputString();
    }

    @Override
    public String toOutputString() {
        if (getNumberOfEdges() > 0) {
            return StringUtils.join(edges, ",");
        }
        else if (getNumberOfVertices() == 1) {
            Vertex vertex = getMainGraph().getVertex(vertices.getUnchecked(0));

            return "0(" + vertex.getVertexLabel() + ")";
        }
        else {
            return "";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BasicPattern that = (BasicPattern) o;

        return edges.equals(that.edges);

    }

    public boolean equals(Object o, int upTo) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BasicPattern that = (BasicPattern) o;

        if (this.getNumberOfEdges() < upTo || that.getNumberOfEdges() < upTo)
           return false;
        
        PatternEdgeArrayList otherEdges = that.getEdges();
        for (int i = 0; i < upTo; i++) {
           if (!edges.getUnchecked(i).equals (otherEdges.getUnchecked(i)))
              return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
       // TODO
        return //edges.isEmpty() ? mainGraph.getVertex(vertices.getUnchecked(0)).getVertexLabel() :
           edges.hashCode();
    }

    public MainGraph getMainGraph() {
        return getConfig().getMainGraph();
    }
}
