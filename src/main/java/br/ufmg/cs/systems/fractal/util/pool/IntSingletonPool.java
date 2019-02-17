package br.ufmg.cs.systems.fractal.util.pool;

import br.ufmg.cs.systems.fractal.util.BasicFactory;
import br.ufmg.cs.systems.fractal.util.Factory;
import br.ufmg.cs.systems.fractal.util.collection.IntSingleton;

public class IntSingletonPool extends Pool<IntSingleton> {
    public static IntSingletonPool instance() {
        return IntSingletonPoolHolder.INSTANCE;
    }

    public IntSingletonPool(Factory<IntSingleton> factory) {
        super(factory);
    }


    public IntSingleton createObject(int value) {
        IntSingleton singleton = createObject();
        singleton.setValue(value);

        return singleton;
    }

    private static class IntSingletonFactory extends BasicFactory<IntSingleton> {
        @Override
        public IntSingleton createObject() {
            return new IntSingleton();
        }
    }

    /*
     * Delayed creation of IntSingletonPool. instance will only be instantiated when we call
     * the static method instance().
     *
     * This initialization is also guaranteed to be thread-safe.
     */
    private static class IntSingletonPoolHolder {
        static final IntSingletonPool INSTANCE = new IntSingletonPool(new IntSingletonFactory());
    }
}
