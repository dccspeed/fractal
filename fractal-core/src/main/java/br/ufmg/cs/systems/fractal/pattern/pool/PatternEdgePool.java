package br.ufmg.cs.systems.fractal.pattern.pool;

import br.ufmg.cs.systems.fractal.pattern.LabelledPatternEdge;
import br.ufmg.cs.systems.fractal.pattern.PatternEdge;
import br.ufmg.cs.systems.fractal.util.BasicFactory;
import br.ufmg.cs.systems.fractal.util.Factory;
import br.ufmg.cs.systems.fractal.util.pool.Pool;

public class PatternEdgePool extends Pool<PatternEdge> {

    public static PatternEdgePool instance(boolean areEdgesLabelled) {
        if (!areEdgesLabelled) {
            return PatternEdgePoolHolder.INSTANCE;
        } else {
            return PatternEdgePoolHolder.LBL_INSTANCE;
        }
    }
    
    public PatternEdgePool(Factory<PatternEdge> factory) {
        super(factory);
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

    /*
     * Delayed creation of PatternEdgePool. instance will only be instantiated when we call
     * the static method instance().
     *
     * This initialization is also guaranteed to be thread-safe.
     */
    private static class PatternEdgePoolHolder {
        static final PatternEdgePool INSTANCE =
           new PatternEdgePool(new PatternEdgeFactory());

        static final PatternEdgePool LBL_INSTANCE =
           new PatternEdgePool(new LabelledPatternEdgeFactory());
    }
}
