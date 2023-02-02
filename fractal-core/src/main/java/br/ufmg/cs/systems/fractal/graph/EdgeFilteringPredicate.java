package br.ufmg.cs.systems.fractal.graph;

import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayListView;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class EdgeFilteringPredicate implements Serializable {
   private static final AtomicInteger nextId = new AtomicInteger(0);
   private final int id = nextId.getAndIncrement();

   public int getId() {
      return id;
   }

   public boolean test(int u, IntArrayList uLabels,
                       int v, IntArrayList vLabels,
                       int e, IntArrayList eLabels) {
      return true;
   }
}
