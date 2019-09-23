package br.ufmg.cs.systems.fractal.pattern.pool;

import br.ufmg.cs.systems.fractal.pattern.PatternEdgeArrayList;
import br.ufmg.cs.systems.fractal.util.BasicFactory;
import br.ufmg.cs.systems.fractal.util.Factory;
import br.ufmg.cs.systems.fractal.util.pool.CollectionPool;

public class PatternEdgeArrayListPool extends CollectionPool<PatternEdgeArrayList> {

    private static class PatternEdgeArrayListFactory extends BasicFactory<PatternEdgeArrayList> {
        @Override
        public PatternEdgeArrayList createObject() {
            return new PatternEdgeArrayList(false);
        }
    }

    private static class LabelledPatternEdgeArrayListFactory extends BasicFactory<PatternEdgeArrayList> {
        @Override
        public PatternEdgeArrayList createObject() {
            return new PatternEdgeArrayList(true);
        }
    }

    public static PatternEdgeArrayListPool instance(boolean areEdgesLabelled) {
        if (!areEdgesLabelled) {
            return PatternEdgeArrayListPoolHolder.INSTANCE;
        } else {
            return PatternEdgeArrayListPoolHolder.LBL_INSTANCE;
        }
    }

    public PatternEdgeArrayListPool(Factory<PatternEdgeArrayList> factory) {
        super(factory);
    }

    @Override
    public void reclaimObject(PatternEdgeArrayList object) {
        PatternEdgePool.instance(object.areEdgesLabelled()).
           reclaimObjects(object);
        super.reclaimObject(object);
    }

    /*
     * Delayed creation of IntArrayListPool. instance will only be instantiated when we call
     * the static method instance().
     *
     * This initialization is also guaranteed to be thread-safe.
     */
    private static class PatternEdgeArrayListPoolHolder {
        static final PatternEdgeArrayListPool INSTANCE =
           new PatternEdgeArrayListPool(new PatternEdgeArrayListFactory());

        static final PatternEdgeArrayListPool LBL_INSTANCE =
           new PatternEdgeArrayListPool(new LabelledPatternEdgeArrayListFactory());
    }
}
