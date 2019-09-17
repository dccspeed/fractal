package br.ufmg.cs.systems.fractal.util.pool;

import br.ufmg.cs.systems.fractal.util.BasicFactory;
import br.ufmg.cs.systems.fractal.util.Factory;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;

public class IntIntMapPool extends Pool<IntIntMap> {
    private static final Factory<IntIntMap> factory = new BasicFactory<IntIntMap>() {
        @Override
        public IntIntMap createObject() {
            return HashIntIntMaps.getDefaultFactory().
               withDefaultValue(-1).newMutableMap();
        }
    };

    public static IntIntMapPool instance() {
        return IntIntMapPoolHolder.INSTANCE;
    }

    public IntIntMapPool() {
        super(factory);
    }

    private class ObjCollectionReclaimer extends ObjReclaimer {
        @Override
        public void accept(IntIntMap o) {
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
    private static class IntIntMapPoolHolder {
        static final IntIntMapPool INSTANCE = new IntIntMapPool();
    }
}
