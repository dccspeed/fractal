package br.ufmg.cs.systems.fractal.gmlib.mcvc;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.pattern.PatternExplorationPlan;
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph;
import br.ufmg.cs.systems.fractal.util.Utils;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayListView;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;

public class MCVCEnumerator extends SubgraphEnumerator<PatternInducedSubgraph> {
   private MainGraph graph;
   private Pattern pattern;
   private PatternExplorationPlan explorationPlan;
   private int mcvcSize;
   protected ObjArrayList<IntArrayList> neighborhoods;
   private ObjArrayList<IntArrayListView> views;
   private ObjArrayList<IntArrayList> results;
   private ObjArrayList<IntArrayList> previouss;
   private IntArrayList diffResult;
   private IntArrayList diffPrevious;
   private IntArrayList validExtensionsRef;
   private IntArrayListView extensionsView;

   @Override
   public void init(Configuration config, Computation<PatternInducedSubgraph> computation) {
      this.graph = config.getMainGraph();
      this.pattern = computation.getPattern();
      this.explorationPlan = pattern.explorationPlan();
      this.mcvcSize = explorationPlan.mcvcSize();
      this.neighborhoods = new ObjArrayList<>();
      this.extensionsView = new IntArrayListView();
      this.views = new ObjArrayList<>(pattern.getNumberOfVertices());
      this.results = new ObjArrayList<>(pattern.getNumberOfVertices());
      this.previouss = new ObjArrayList<>(pattern.getNumberOfVertices());
      this.diffPrevious = new IntArrayList();
      this.diffResult = new IntArrayList();
      for (int i = 0; i < pattern.getNumberOfVertices(); ++i) {
         views.add(new IntArrayListView());
         results.add(new IntArrayList());
         previouss.add(new IntArrayList());
      }
   }

   @Override
   public boolean shouldRebuildState() {
      return true;
   }

   @Override
   public void computeExtensions_EXTENSION_PRIMITIVE() {
      int numVertices = subgraph.getNumVertices();
      if (numVertices < mcvcSize) {
         super.computeExtensions_EXTENSION_PRIMITIVE();
      } else if (numVertices == mcvcSize) {
         int numVerticesPattern = pattern.getNumberOfVertices();

         // compute extensions for remaining levels
         neighborhoods.clear();
         for (int i = 0; i < numVerticesPattern; ++i) {
            neighborhoods.add(null);
         }

         // intersections
         for (int pos = numVertices; pos < numVerticesPattern; ++pos) {
            ensureNeighborhoods(subgraph.getVertices(), pattern, pos);
         }

         // copy extensions to other enumerators
         int j = mcvcSize;
         validExtensionsRef = neighborhoods.getu(j++);
         SubgraphEnumerator<PatternInducedSubgraph> currEnumerator = nextEnumerator();
         while (currEnumerator != null) {
            if (currEnumerator instanceof MCVCEnumerator) {
               MCVCEnumerator mcvcEnumerator = (MCVCEnumerator) currEnumerator;
               mcvcEnumerator.neighborhoods.clear();
               for (int i = 0; i < neighborhoods.size(); ++i) {
                  mcvcEnumerator.neighborhoods.add(neighborhoods.getu(i));
               }
               mcvcEnumerator.validExtensionsRef = neighborhoods.getu(j++);
            }
            currEnumerator = currEnumerator.nextEnumerator();
         }

         IntArrayList validExtensions = validExtensionsRef;
         validExtensions = applyDifferences(pattern, numVertices,
                 validExtensions);
         int numExtensions = validExtensions.size();
         int lowerBound = pattern.sbLowerBound(subgraph, numVertices);
         int startIdx = validExtensions.binarySearch(lowerBound);
         startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx + 1;
         validExtensions.view(extensionsView, startIdx, numExtensions);
         newExtensions(extensionsView);
      } else {
         // extensions are already computed, just use it
         IntArrayList validExtensions = validExtensionsRef;
         validExtensions = applyDifferences(pattern, numVertices,
                 validExtensions);
         int numExtensions = validExtensions.size();
         int lowerBound = pattern.sbLowerBound(subgraph, numVertices);
         int startIdx = validExtensions.binarySearch(lowerBound);
         startIdx = (startIdx < 0) ? (-startIdx - 1) : startIdx + 1;
         validExtensions.view(extensionsView, startIdx, numExtensions);
         newExtensions(extensionsView);
      }

   }

   @Override
   public boolean extend_EXTENSION_PRIMITIVE() {
      IntArrayList vertices = subgraph.getVertices();
      while (true) {
         int word = nextExtension();

         // no more extensions left
         if (word == INVALID_EXTENSION) return false;

         // matching core, no duplicate is possible
         int numVertices = vertices.size();
         if (numVertices < mcvcSize) {
            subgraph.addWord(word);
            return true;
         }

         // matching non-core, check whether there is duplicates
         boolean valid = true;
         for (int i = 0; i < numVertices; ++i) {
            if (vertices.getu(i) == word) {
               valid = false;
               break;
            }
         }

         // valid non-core extension, add to subgraph and return
         if (valid) {
            subgraph.addWord(word);
            return true;
         }
      }
   }

   @Override
   public String asExtensionMethodString(Computation computation) {
      return "Mmcvc";
   }

   private void swapPreviousAndResult(int pos) {
      IntArrayList aux = previouss.getu(pos);
      previouss.setu(pos, results.getu(pos));
      results.setu(pos, aux);
   }

   private void ensureNeighborhoods(IntArrayList vertices, Pattern pattern,
                                    int pos) {
      if (neighborhoods.getu(pos) != null) return;
      /**
       * Single vertices: neighborhood view
       */
      if (pos < mcvcSize) {
         IntArrayListView view = views.getu(pos);
         graph.neighborhoodVertices(vertices.getu(pos), view);
         neighborhoods.setu(pos, view);
         return;
      }

      /**
       * Ensure dependencies are satisfied
       */
      PatternExplorationPlan explorationPlan = pattern.explorationPlan();
      IntArrayList intersections = explorationPlan.intersection(pos);
      for (int i = 0; i < intersections.size(); ++i) {
         ensureNeighborhoods(vertices, pattern, intersections.getu(i));
      }

      /**
       * In case intersections is single element, just get reference
       */
      if (intersections.size() == 1) {
         int dep = intersections.getu(0);
         neighborhoods.setu(pos, neighborhoods.getu(dep));
         return;
      }

      /**
       * In case intersections have two elements
       */
      if (intersections.size() == 2) {
         IntArrayList neighborhood1 = neighborhoods.getu(intersections.getu(0));
         IntArrayList neighborhood2 = neighborhoods.getu(intersections.getu(1));
         IntArrayList result = results.getu(pos);
         result.clear();
         Utils.sintersect(neighborhood1, neighborhood2, 0, neighborhood1.size(),
                 0, neighborhood2.size(), result);
         neighborhoods.setu(pos, result);
         return;
      }

      /**
       * In case intersections have more than two elements
       */
      IntArrayList result = results.getu(pos);
      result.clear();
      IntArrayList previous = previouss.getu(pos);
      previous.clear();
      IntArrayList neighborhood1 = neighborhoods.getu(intersections.getu(0));
      IntArrayList neighborhood2 = neighborhoods.getu(intersections.getu(1));
      Utils.sintersect(neighborhood1, neighborhood2, 0, neighborhood1.size(), 0,
              neighborhood2.size(), result);

      for (int i = 2; i < intersections.size(); ++i) {
         swapPreviousAndResult(pos);
         result = results.getu(pos);
         previous = previouss.getu(pos);
         neighborhood1 = neighborhoods.getu(intersections.getu(i));
         result.clear();
         Utils.sintersect(previous, neighborhood1, 0, previous.size(), 0,
                 neighborhood1.size(), result);
      }

      neighborhoods.setu(pos, result);
   }

   private IntArrayList applyDifferences(Pattern pattern,
                                         int pos,
                                         IntArrayList validExtensions) {
      int numVertices = subgraph.getNumVertices();
      IntArrayList differences = pattern.explorationPlan().difference(numVertices);
      if (differences.size() > 0) {
         int diffIdx = differences.getu(0);
         IntArrayList vertices = subgraph.getVertices();
         MainGraph graph = computation.getConfig().getMainGraph();
         IntArrayList result = diffResult;
         result.clear();
         IntArrayList previous = diffPrevious;
         previous.clear();
         IntArrayListView nextNeighborhood = views.getu(pos);
         graph.neighborhoodVertices(vertices.getu(diffIdx), nextNeighborhood);
         Utils.sdifference(validExtensions, nextNeighborhood, 0,
                 validExtensions.size(), 0, nextNeighborhood.size(), result);

         for (int i = 1; i < differences.size(); ++i) {
            diffIdx = differences.getu(i);
            IntArrayList aux = result;
            result = previous;
            previous = aux;
            graph.neighborhoodVertices(vertices.getu(diffIdx), nextNeighborhood);
            result.clear();
            Utils.sdifference(previous, nextNeighborhood, 0, previous.size(), 0,
                    nextNeighborhood.size(), result);
         }

         validExtensions = result;
      }

      return validExtensions;
   }
}
