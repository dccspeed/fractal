package br.ufmg.cs.systems.fractal.graph;

import java.io.InputStream;

public class VLabeledMainGraph extends VELabeledMainGraph {

   public VLabeledMainGraph() {

   }

   @Override
   protected void readEdgeLabelsFromInputStream(InputStream is) {
   }

   @Override
   public int firstEdgeLabel(int e) {
      return 0;
   }
}
