package br.ufmg.cs.systems.fractal.util.collection;

import com.koloboke.collect.map.IntIntMap;
import com.koloboke.function.IntIntConsumer;

public class IntIntMapAddConsumer implements IntIntConsumer {
    private IntIntMap map;

    public void setMap(IntIntMap map) {
        this.map = map;
    }

    @Override
    public void accept(int k, int v) {
        map.put(k, v);
    }
}
