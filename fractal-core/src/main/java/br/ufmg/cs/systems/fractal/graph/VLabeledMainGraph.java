package br.ufmg.cs.systems.fractal.graph;

public class VLabeledMainGraph extends VELabeledMainGraph {

   public VLabeledMainGraph(String name, boolean isEdgeLabelled,
                            boolean isMultiGraph) {
   }

   @Override
   public int edgeLabel(int e) {
      return 0;
   }
}
