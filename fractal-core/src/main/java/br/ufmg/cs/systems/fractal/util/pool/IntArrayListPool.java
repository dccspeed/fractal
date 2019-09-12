package br.ufmg.cs.systems.fractal.util.pool;

import br.ufmg.cs.systems.fractal.util.BasicFactory;
import br.ufmg.cs.systems.fractal.util.Factory;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;

public class IntArrayListPool extends CollectionPool<IntArrayList> {
    private static final Factory<IntArrayList> factory = new BasicFactory<IntArrayList>() {
        @Override
        public IntArrayList createObject() {
            return new IntArrayList();
        }
    };

    public static IntArrayListPool instance() {
        return IntArrayListPoolHolder.INSTANCE;
    }

    public IntArrayListPool() {
        super(factory);
    }

    /*
     * Delayed creation of IntArrayListPool. instance will only be instantiated when we call
     * the static method instance().
     *
     * This initialization is also guaranteed to be thread-safe.
     */
    private static class IntArrayListPoolHolder {
        static final IntArrayListPool INSTANCE = new IntArrayListPool();
    }
}
