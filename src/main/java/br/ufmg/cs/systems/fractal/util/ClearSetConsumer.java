package br.ufmg.cs.systems.fractal.util;

import com.koloboke.collect.set.hash.HashIntSet;
import java.util.function.IntConsumer;

public class ClearSetConsumer implements IntConsumer {
    private HashIntSet[] supportMatrix;

    public void setSupportMatrix(HashIntSet[] supportMatrix) {
        this.supportMatrix = supportMatrix;
    }

    @Override
    public void accept(int i) {
        HashIntSet domainSet = supportMatrix[i];

        if (domainSet != null) {
            domainSet.clear();
        }
    }
}
