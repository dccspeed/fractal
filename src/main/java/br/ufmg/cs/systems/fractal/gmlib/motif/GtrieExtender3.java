package br.ufmg.cs.systems.fractal.gmlib.motif;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;

public class GtrieExtender3 extends GtrieExtender {

   private static final IntArrayList patterns = new IntArrayList() {
      {
         add(0b00000000000000000000000000000000);
            add(0b00000000000000000000000000000010);
               add(0b00000000000000000000000000001110);
               add(0b00000000000000000000000000001010);
      }
   };

   // <pattern> <id> <depth> <mask> <parentpos> <nchildren> <childrenpos ...>
   private static final IntArrayList gtrie3 = new IntArrayList() {
      {
         add(patterns.get(0)); add(0); add(0); add(0); add(-1); add(1); add(7);
            add(patterns.get(1)); add(1); add(1); add(1); add(0); add(2); add(15); add(21);
               add(patterns.get(2)); add(2); add(2); add(2); add(7); add(0);
               add(patterns.get(3)); add(3); add(2); add(2); add(7); add(0);
      }
   };
   
   public GtrieExtender3() {
      super();
      this.gtrie = gtrie3;
   }

   @Override
   public boolean testSb(Subgraph e, int pattern, int v) {
      if (e.getNumVertices() + 1 < 3) {
         return true;
      }

      IntArrayList vertices = e.getVertices();
      if (pattern == patterns.get(2)) {
         return vertices.get(0) < vertices.get(1) && vertices.get(1) < v;
      } else if (pattern == patterns.get(3)) {
         return vertices.get(0) < v;
      } else {
         throw new RuntimeException("Not allowed");
      }
   }
}
