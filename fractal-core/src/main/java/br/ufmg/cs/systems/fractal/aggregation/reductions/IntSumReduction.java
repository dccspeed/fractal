package br.ufmg.cs.systems.fractal.aggregation.reductions;

import org.apache.hadoop.io.IntWritable;

public class IntSumReduction extends ReductionFunction<IntWritable> {
    @Override
    public IntWritable reduce(IntWritable k1, IntWritable k2) {
        if (k1 != null && k2 != null) {
            k1.set(k1.get() + k2.get());
        }

        return k1;
    }
}
