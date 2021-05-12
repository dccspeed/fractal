package br.ufmg.cs.systems.fractal.graph;

import java.io.InputStream;

public class UnlabeledMainGraph extends VELabeledMainGraph {

   public UnlabeledMainGraph() {
   }

   @Override
   protected void readVertexLabelsFromInputStream(InputStream is) {
   }

   @Override
   protected void readEdgeLabelsFromInputStream(InputStream is) {
   }

   @Override
   public int firstVertexLabel(int u) {
      return 1;
   }

   @Override
   public int firstEdgeLabel(int e) {
      return 0;
   }
}
