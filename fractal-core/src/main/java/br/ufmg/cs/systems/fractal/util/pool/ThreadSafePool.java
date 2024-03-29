package br.ufmg.cs.systems.fractal.util.pool;

import br.ufmg.cs.systems.fractal.util.Factory;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ReclaimableObjCollection;
import org.apache.log4j.Logger;

import java.util.function.Consumer;

public class ThreadSafePool<O> implements Pool<O> {
    private static final Logger LOG = Logger.getLogger(ThreadSafePool.class);
    private final static int MAX_SIZE_DEFAULT = 1000;

    private int maxSize;
    private Factory<O> objectFactory;
    protected PoolStorage poolStorage;
    private final ObjReclaimerStorage reclaimerStorage;

    public ThreadSafePool(Factory<O> objectFactory) {
        this(objectFactory, MAX_SIZE_DEFAULT);
    }

    public ThreadSafePool(Factory<O> objectFactory, int maxSize) {
        this.objectFactory = objectFactory;
        this.maxSize = maxSize;
        reclaimerStorage = new ObjReclaimerStorage();

        reset();

        PoolRegistry.instance().register(this.getClass().getSimpleName(), this);
    }

    public void reset() {
        objectFactory.reset();
        poolStorage = new PoolStorage();
    }

    @Override
    public O createObject() {
        ObjArrayList<O> pool = poolStorage.get();
        if (!pool.isEmpty()) {
            return pool.pop();
        } else {
            return objectFactory.createObject();
        }
    }

    @Override
    public void reclaimObject(O object) {
        reclaimerStorage.get().accept(object);
    }

    @Override
    public void reclaimObjects(ReclaimableObjCollection<O> objects) {
        objects.forEach(reclaimerStorage.get());
    }

    protected ObjReclaimer createObjReclaimer() {
        return new ObjReclaimer();
    }

    protected class ObjReclaimer implements Consumer<O> {
        private ObjArrayList<O> pool;

        public ObjReclaimer() {
            pool = poolStorage.get();
        }

        @Override
        public void accept(O o) {
            if (pool.size() < maxSize) {
                pool.add(o);
            }
        }
    }

    protected class PoolStorage extends ThreadLocal<ObjArrayList<O>> {
        @Override
        protected ObjArrayList<O> initialValue() {
            return new ObjArrayList<>(maxSize);
        }
    }

    private class ObjReclaimerStorage extends ThreadLocal<ObjReclaimer> {
        @Override
        protected ObjReclaimer initialValue() {
            return createObjReclaimer();
        }
    }
}
