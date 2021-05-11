package br.ufmg.cs.systems.fractal.graph;

public class UnlabeledMainGraph extends VELabeledMainGraph {

   public UnlabeledMainGraph() {

   }

   @Override
   public int vertexLabel(int u) {
      return 1;
   }

   @Override
   public int edgeLabel(int e) {
      return 0;
   }
}
