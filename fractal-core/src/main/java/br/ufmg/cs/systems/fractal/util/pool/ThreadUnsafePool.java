package br.ufmg.cs.systems.fractal.util.pool;

import br.ufmg.cs.systems.fractal.util.Factory;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ReclaimableObjCollection;
import org.apache.log4j.Logger;

import java.util.function.Consumer;

public class ThreadUnsafePool<O> implements Pool<O> {
    private static final Logger LOG = Logger.getLogger(ThreadUnsafePool.class);
    private final static int MAX_SIZE_DEFAULT = 1000;

    private int maxSize;
    private Factory<O> objectFactory;
    private ObjArrayList<O> pool;
    public ObjReclaimer reclaimer;

    public ThreadUnsafePool(Factory<O> objectFactory) {
        this(objectFactory, MAX_SIZE_DEFAULT);
    }

    public ThreadUnsafePool(Factory<O> objectFactory, int maxSize) {
        this.objectFactory = objectFactory;
        this.maxSize = maxSize;
        this.pool = new ObjArrayList<O>(maxSize);
        this.reclaimer = new ObjReclaimer();
        reset();
    }

    public void reset() {
        objectFactory.reset();
    }

    @Override
    public O createObject() {
        if (!pool.isEmpty()) {
            return pool.pop();
        } else {
            return objectFactory.createObject();
        }
    }

    @Override
    public void reclaimObject(O object) {
        if (pool.size() < maxSize) {
            pool.add(object);
        }
    }

    @Override
    public void reclaimObjects(ReclaimableObjCollection<O> objects) {
        objects.forEach(reclaimer);
    }

    protected ObjReclaimer createObjReclaimer() {
        return new ObjReclaimer();
    }

    protected class ObjReclaimer implements Consumer<O> {
        @Override
        public void accept(O o) {
            if (pool.size() < maxSize) {
                pool.add(o);
            }
        }
    }

}
