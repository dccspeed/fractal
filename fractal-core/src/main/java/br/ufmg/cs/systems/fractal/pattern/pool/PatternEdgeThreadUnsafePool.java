package br.ufmg.cs.systems.fractal.pattern.pool;

import br.ufmg.cs.systems.fractal.pattern.LabelledPatternEdge;
import br.ufmg.cs.systems.fractal.pattern.PatternEdge;
import br.ufmg.cs.systems.fractal.util.BasicFactory;
import br.ufmg.cs.systems.fractal.util.Factory;
import br.ufmg.cs.systems.fractal.util.pool.ThreadUnsafePool;

public class PatternEdgeThreadUnsafePool extends ThreadUnsafePool<PatternEdge> {

   public static PatternEdgeThreadUnsafePool instance(boolean areEdgesLabelled, int maxsize) {
      if (!areEdgesLabelled) {
         return new PatternEdgeThreadUnsafePool(new PatternEdgeFactory(),
                 maxsize);
      } else {
         return new PatternEdgeThreadUnsafePool(new LabelledPatternEdgeFactory(), maxsize);
      }
   }

   public PatternEdgeThreadUnsafePool(Factory<PatternEdge> factory,
                                      int maxsize) {
      super(factory, maxsize);
   }

   private static class PatternEdgeFactory extends BasicFactory<PatternEdge> {
      @Override
      public PatternEdge createObject() {
         return new PatternEdge();
      }
   }

   private static class LabelledPatternEdgeFactory extends BasicFactory<PatternEdge> {
      @Override
      public PatternEdge createObject() {
         return new LabelledPatternEdge();
      }
   }
}
