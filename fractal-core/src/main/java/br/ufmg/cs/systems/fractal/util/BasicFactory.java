package br.ufmg.cs.systems.fractal.util;

public abstract class BasicFactory<O> implements Factory<O> {
    public BasicFactory() {
        reset();
    }

    @Override
    public void reset() {
        // Empyt on purpose
    }
}
