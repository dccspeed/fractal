package br.ufmg.cs.systems.fractal.util.collection;

public interface ReclaimableObjCollection<O> extends com.koloboke.collect.ObjCollection<O> {
    void reclaim();
}
