package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.pattern.pool.PatternEdgeArrayListPool;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.pool.ThreadUnsafePool;

public class PatternEdgeArrayList extends ObjArrayList<PatternEdge> implements Comparable<PatternEdgeArrayList> {
   private boolean areEdgesLabelled;
   private ThreadUnsafePool<PatternEdge> patternEdgePool;

   public PatternEdgeArrayList(boolean areEdgesLabelled) {
      this.areEdgesLabelled = areEdgesLabelled;
   }

   public PatternEdgeArrayList(boolean areEdgesLabelled,
                               ThreadUnsafePool<PatternEdge> patternEdgePool) {
      this.areEdgesLabelled = areEdgesLabelled;
      this.patternEdgePool = patternEdgePool;
   }

   public PatternEdge createObject() {
      return patternEdgePool.createObject();
   }

   public void reclaim() {
      PatternEdgeArrayListPool.instance(areEdgesLabelled).reclaimObject(this);
   }

   public boolean areEdgesLabelled() {
      return areEdgesLabelled;
   }

   public int compareTo(PatternEdgeArrayList other) {
      int mySize = size();
      int otherSize = other.size();

      if (mySize == otherSize) {
         for (int i = 0; i < mySize; ++i) {
            PatternEdge myPatternEdge = get(i);
            PatternEdge otherPatternEdge = other.get(i);

            int comparisonResult = myPatternEdge.compareTo(otherPatternEdge);

            if (comparisonResult != 0) {
               return comparisonResult;
            }
         }
      }
      else {
         return mySize - otherSize;
      }

      return 0;
   }
}
