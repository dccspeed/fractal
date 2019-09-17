package br.ufmg.cs.systems.fractal.util.pool;

import br.ufmg.cs.systems.fractal.util.BasicFactory;
import br.ufmg.cs.systems.fractal.util.Factory;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;

public class HashIntSetPool extends CollectionPool<HashIntSet> {
    private static final Factory<HashIntSet> factory = new BasicFactory<HashIntSet>() {
        @Override
        public HashIntSet createObject() {
            return HashIntSets.newMutableSet();
        }
    };

    public static HashIntSetPool instance() {
        return HashIntSetPoolHolder.INSTANCE;
    }

    public HashIntSetPool() {
        super(factory);
    }

    /*
     * Delayed creation of IntArrayListPool. instance will only be instantiated when we call
     * the static method instance().
     *
     * This initialization is also guaranteed to be thread-safe.
     */
    private static class HashIntSetPoolHolder {
        static final HashIntSetPool INSTANCE = new HashIntSetPool();
    }
}
