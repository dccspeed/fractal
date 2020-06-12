package br.ufmg.cs.systems.fractal.util.pool;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class PoolRegistry {
    private Map<String, ThreadSafePool> poolMap;

    public PoolRegistry() {
        this.poolMap = new HashMap<>();
    }

    public synchronized void register(String poolId, ThreadSafePool pool) {
        poolMap.put(poolId, pool);
    }

    public synchronized Map<String, ThreadSafePool> getPoolMap() {
        return poolMap;
    }

    public synchronized Collection<ThreadSafePool> getPools() {
        return poolMap.values();
    }

    public static PoolRegistry instance() {
        return PoolRegistryHolder.INSTANCE;
    }

    /*
     * Delayed creation of PoolRegistry. instance will only be instantiated when we call
     * the static method instance().
     *
     * This initialization is also guaranteed to be thread-safe.
     */
    private static class PoolRegistryHolder {
        static final PoolRegistry INSTANCE = new PoolRegistry();
    }
}
