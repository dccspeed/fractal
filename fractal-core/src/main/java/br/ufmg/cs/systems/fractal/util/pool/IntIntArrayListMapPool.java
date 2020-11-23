package br.ufmg.cs.systems.fractal.util.pool;

import br.ufmg.cs.systems.fractal.util.BasicFactory;
import br.ufmg.cs.systems.fractal.util.Factory;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;

public class IntIntArrayListMapPool extends ThreadSafePool<HashIntObjMap<IntArrayList>> {
    private static final Factory<HashIntObjMap<IntArrayList>> factory = new BasicFactory<HashIntObjMap<IntArrayList>>() {
        @Override
        public HashIntObjMap<IntArrayList> createObject() {
            return HashIntObjMaps.newMutableMap();
        }
    };

    public static IntIntArrayListMapPool instance() {
        return IntIntArrayListMapPoolHolder.INSTANCE;
    }

    public IntIntArrayListMapPool() {
        super(factory);
    }

    private class ObjCollectionReclaimer extends ObjReclaimer {
        @Override
        public void accept(HashIntObjMap<IntArrayList> o) {
            o.clear();
            super.accept(o);
        }
    }

    @Override
    protected ObjReclaimer createObjReclaimer() {
        return new ObjCollectionReclaimer();
    }

    /*
     * Delayed creation of IntArrayListPool. instance will only be instantiated when we call
     * the static method instance().
     *
     * This initialization is also guaranteed to be thread-safe.
     */
    private static class IntIntArrayListMapPoolHolder {
        static final IntIntArrayListMapPool INSTANCE = new IntIntArrayListMapPool();
    }
}
