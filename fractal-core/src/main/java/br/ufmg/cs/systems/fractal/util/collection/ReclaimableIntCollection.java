package br.ufmg.cs.systems.fractal.util.collection;

import com.koloboke.collect.IntCollection;

public interface ReclaimableIntCollection extends IntCollection {
    void reclaim();
}
