package br.ufmg.cs.systems.fractal.util;

public interface Factory<O> {
    O createObject();
    void reset();
}
