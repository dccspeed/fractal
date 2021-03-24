package br.ufmg.cs.systems.fractal.graph;

public class ELabeledMainGraph extends VELabeledMainGraph {

   public ELabeledMainGraph(String name, boolean isEdgeLabelled,
                            boolean isMultiGraph) {
   }

   @Override
   public int vertexLabel(int u) {
      return 1;
   }
}
