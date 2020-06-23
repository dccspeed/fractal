package br.ufmg.cs.systems.fractal.util;

import java.io.DataOutput;
import java.io.IOException;
import java.util.function.IntConsumer;

public class WriterSetConsumer implements IntConsumer {
    DataOutput dataOutput;

    @Override
    public void accept(int i) {
        try {
            dataOutput.writeInt(i);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Writing failed");
        }
    }

    public void setOutput(DataOutput dataOutput) {
        this.dataOutput = dataOutput;
    }
}
