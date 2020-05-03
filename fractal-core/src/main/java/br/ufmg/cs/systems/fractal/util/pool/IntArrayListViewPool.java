package br.ufmg.cs.systems.fractal.util.pool;

import br.ufmg.cs.systems.fractal.util.BasicFactory;
import br.ufmg.cs.systems.fractal.util.Factory;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayListView;

public class IntArrayListViewPool extends CollectionPool<IntArrayListView> {
    private static final Factory<IntArrayListView> factory = new BasicFactory<IntArrayListView>() {
        @Override
        public IntArrayListView createObject() {
            return new IntArrayListView();
        }
    };

    public static IntArrayListViewPool instance() {
        return IntArrayListViewPoolHolder.INSTANCE;
    }

    public IntArrayListViewPool() {
        super(factory);
    }

    /*
     * Delayed creation of IntArrayListPool. instance will only be instantiated when we call
     * the static method instance().
     *
     * This initialization is also guaranteed to be thread-safe.
     */
    private static class IntArrayListViewPoolHolder {
        static final IntArrayListViewPool INSTANCE = new IntArrayListViewPool();
    }
}
