package br.ufmg.cs.systems.fractal.graph;

public class UnlabeledMainGraph extends VELabeledMainGraph {

   public UnlabeledMainGraph(String name, boolean isEdgeLabelled,
                            boolean isMultiGraph) {
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
