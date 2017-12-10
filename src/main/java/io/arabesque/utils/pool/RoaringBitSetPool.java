package io.arabesque.utils.pool;

import io.arabesque.utils.BasicFactory;
import io.arabesque.utils.Factory;
import io.arabesque.utils.collection.RoaringBitSet;

public class RoaringBitSetPool extends CollectionPool<RoaringBitSet> {
    private static final Factory<RoaringBitSet> factory = new BasicFactory<RoaringBitSet>() {
        @Override
        public RoaringBitSet createObject() {
            return new RoaringBitSet();
        }
    };

    public static RoaringBitSetPool instance() {
        return RoaringBitSetPoolHolder.INSTANCE;
    }

    public RoaringBitSetPool() {
        super(factory);
    }

    /*
     * Delayed creation of RoaringBitSetPool. instance will only be instantiated when we call
     * the static method instance().
     *
     * This initialization is also guaranteed to be thread-safe.
     */
    private static class RoaringBitSetPoolHolder {
        static final RoaringBitSetPool INSTANCE = new RoaringBitSetPool();
    }
}
