package br.ufmg.cs.systems.fractal.util;

import com.koloboke.collect.map.hash.HashIntIntMap;
import java.util.function.IntConsumer;

public class ElementCounterConsumer implements IntConsumer {
    private HashIntIntMap map;

    public void setMap(HashIntIntMap map) {
        this.map = map;
    }

    public HashIntIntMap getMap() {
        return map;
    }

    @Override
    public void accept(int i) {
        map.addValue(i, 1);
    }
}
