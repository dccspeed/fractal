package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.graph.MainGraph;
import com.koloboke.function.IntIntPredicate;

import java.util.function.IntPredicate;

public class EdgePredicate implements IntPredicate {
   private int edgeLabel;
   private MainGraph graph;

   public void set(MainGraph graph, int edgeLabel) {
      this.edgeLabel = edgeLabel;
      this.graph = graph;
   }

   @Override
   public boolean test(int e) {
      return graph.edgeLabel(e) == edgeLabel;
   }
}

