package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.pattern.pool.PatternEdgeArrayListPool;
import br.ufmg.cs.systems.fractal.pattern.pool.PatternEdgePool;
import br.ufmg.cs.systems.fractal.util.WritableObjArrayList;

import java.util.Collection;

public class PatternEdgeArrayList extends WritableObjArrayList<PatternEdge> implements Comparable<PatternEdgeArrayList> {
    private boolean areEdgesLabelled;

    public PatternEdgeArrayList(boolean areEdgesLabelled) {
        this.areEdgesLabelled = areEdgesLabelled;
    }

    public PatternEdgeArrayList(int capacity) {
        super(capacity);
    }

    public PatternEdgeArrayList(Collection<PatternEdge> elements) {
        super(elements);
    }

    @Override
    protected PatternEdge createObject() {
        return PatternEdgePool.instance(areEdgesLabelled).createObject();
    }

    @Override
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
