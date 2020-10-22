package br.ufmg.cs.systems.fractal.gmlib.clique;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.Utils;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayListView;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import com.koloboke.collect.map.IntObjCursor;
import com.koloboke.collect.map.IntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.koloboke.function.IntObjConsumer;
import org.apache.log4j.Logger;

public class MaximalCliquesEnumerator<S extends Subgraph> extends SubgraphEnumerator<S> {
   private static final Logger LOG = Logger.getLogger(MaximalCliquesEnumerator.class);

   private IntObjMap<IntArrayList> inducedGraph;
   private InducedGraphCleaner inducedGraphCleaner;

   protected IntArrayList cand;
   protected IntArrayList fini;
   private IntArrayList exts;
   private IntArrayListView neighborhood;
   private IntArrayList aux;

   @Override
   public void init(Configuration config, Computation<S> computation) {
      this.cand = new IntArrayList();
      this.fini = new IntArrayList();
      this.exts = new IntArrayList();
      this.neighborhood = new IntArrayListView();
      this.aux = new IntArrayList();
      this.inducedGraphCleaner = new InducedGraphCleaner();
   }

   @Override
   public void rebuildState() {
      if (subgraph.getNumVertices() == 0) return;
      MainGraph graph = computation.getConfig().getMainGraph();
      IntArrayList candTmp = IntArrayListPool.instance().createObject();
      IntArrayList finiTmp = IntArrayListPool.instance().createObject();

      bootstrap(graph);

      // compute extensions
      int pivot = generatePivot();
      IntArrayList uneighbors = inducedGraph.get(pivot);
      exts.clear();
      Utils.sdifference(cand, uneighbors, 0, cand.size(), 0,
              uneighbors.size(), exts);

      IntArrayList tmp;

      for (int i = 1; i < subgraph.getNumWords(); ++i) {
         // extend
         int u = subgraph.getVertices().getu(i);
         computeSets(u, candTmp, finiTmp);

         // swap
         tmp = candTmp;
         candTmp = cand;
         cand = tmp;
         tmp = finiTmp;
         finiTmp = fini;
         fini = tmp;

         // compute extensions
         pivot = generatePivot();
         graph.neighborhoodVertices(pivot, neighborhood);
         exts.clear();
         Utils.sdifference(cand, neighborhood, 0, cand.size(), 0,
                 neighborhood.size(), exts);
      }

      finiTmp.reclaim();
      candTmp.reclaim();
   }

   @Override
   public void computeExtensions() {
      int numVertices = subgraph.getNumVertices();
      if (numVertices == 0) {
         super.computeExtensions();
         return;
      }

      MainGraph graph = computation.getConfig().getMainGraph();

      if (numVertices == 1) {
         bootstrap(graph);
      }

      //LOG.warn("ComputeExtensions2 " + this);

      if (cand.isEmpty() && fini.isEmpty()) {
         exts.clear();
         newExtensions(exts);
         computation.getExecutionEngine().addValidSubgraphs(1);
         computation.lastComputation().process(subgraph);
         return;
      }

      int pivot = generatePivot();
      IntArrayList uneighbors = inducedGraph.get(pivot);
      exts.clear();
      Utils.sdifference(cand, uneighbors, 0, cand.size(), 0,
              uneighbors.size(), exts);

      newExtensions(exts);
   }

   private void bootstrap(MainGraph graph) {
      int u = subgraph.getVertices().getu(0);
      graph.neighborhoodVertices(u, neighborhood);

      // clear cand and fini sets
      fini.clear();
      cand.clear();

      // create induced subgraph vertices and fill cand and fini
      if (inducedGraph == null) {
         inducedGraph = HashIntObjMaps.newMutableMap(neighborhood.size() + 1);
      }
      inducedGraph.forEach(inducedGraphCleaner);
      inducedGraph.clear();
      inducedGraph.ensureCapacity(neighborhood.size() + 1);
      inducedGraph.put(u, IntArrayListPool.instance().createObject());
      cand.ensureCapacity(neighborhood.size());
      fini.ensureCapacity(neighborhood.size());
      for (int i = 0; i < neighborhood.size(); ++i) {
         int v = neighborhood.getu(i);
         inducedGraph.put(v, IntArrayListPool.instance().createObject());
         if (v > u) cand.add(v);
         else fini.add(v);
      }

      // select only edges between the vertices in this induced subgraph
      IntObjCursor<IntArrayList> cur = inducedGraph.cursor();
      while (cur.moveNext()) {
         int v = cur.key();
         IntArrayList vneighbors = cur.value();
         graph.neighborhoodVertices(v, neighborhood);
         vneighbors.ensureCapacity(neighborhood.size());
         for (int i = 0; i < neighborhood.size(); ++i) {
            int w = neighborhood.getu(i);
            if (inducedGraph.containsKey(w)) {
               vneighbors.add(w);
            }
         }
      }
   }

   private int generatePivot() {
      int pivot = -1, maxIntersectionSize = Integer.MIN_VALUE;
      for (int i = 0; i < cand.size(); ++i) {
         int u = cand.getu(i);
         IntArrayList uneighbors = inducedGraph.get(u);
         int intersectionSize = Utils.sintersectSize(cand, uneighbors, 0,
                 cand.size(), 0, uneighbors.size());
         if (intersectionSize > maxIntersectionSize) {
            maxIntersectionSize = intersectionSize;
            pivot = u;
         }
      }

      for (int i = 0; i < fini.size(); ++i) {
         int u = fini.getu(i);
         IntArrayList uneighbors = inducedGraph.get(u);
         int intersectionSize = Utils.sintersectSize(cand, uneighbors, 0,
                 cand.size(), 0, uneighbors.size());
         if (intersectionSize > maxIntersectionSize) {
            maxIntersectionSize = intersectionSize;
            pivot = u;
         }
      }

      return pivot;
   }

   @Override
   public boolean extend() {
      if (getPrefix().size() == 0) return super.extend();

      if (super.extend()) {
         MaximalCliquesEnumerator<S> nextEnumerator =
                 (MaximalCliquesEnumerator<S>) computation.nextComputation().getSubgraphEnumerator();
         int u = subgraph.getVertices().getLast();
         computeSets(u, nextEnumerator.cand, nextEnumerator.fini);
         nextEnumerator.inducedGraph = inducedGraph;
         return true;
      }
      return false;
   }

   private void computeSets(int u, IntArrayList nextCand,
                           IntArrayList nextFini) {
      IntArrayList uneighbors = inducedGraph.get(u);

      // cand
      int idx = exts.binarySearch(u);
      aux.clear();
      Utils.sdifference(cand, exts, 0, cand.size(), 0, idx, aux);
      nextCand.clear();
      Utils.sintersect(aux, uneighbors, 0,
              aux.size(), 0, uneighbors.size(), nextCand);

      // fini
      aux.clear();
      Utils.sunion(fini, exts, 0, fini.size(), 0, idx, aux);
      nextFini.clear();
      Utils.sintersect(aux, uneighbors, 0,
              aux.size(), 0, uneighbors.size(), nextFini);

   }

   @Override
   public String toString() {
      return cand + " " + fini + " " + exts + " " + inducedGraph +
              super.toString();
   }

   private class InducedGraphCleaner implements IntObjConsumer<IntArrayList> {
      @Override
      public void accept(int u, IntArrayList neighbors) {
         IntArrayListPool.instance().reclaimObject(neighbors);
      }
   }

}
