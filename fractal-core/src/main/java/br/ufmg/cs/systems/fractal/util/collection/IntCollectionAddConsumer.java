package br.ufmg.cs.systems.fractal.util.collection;

import com.koloboke.collect.IntCollection;
import java.util.function.IntConsumer;

public class IntCollectionAddConsumer implements IntConsumer {
    private IntCollection collection;

    public void setCollection(IntCollection collection) {
        this.collection = collection;
    }

    @Override
    public void accept(int i) {
        collection.add(i);
    }
}
