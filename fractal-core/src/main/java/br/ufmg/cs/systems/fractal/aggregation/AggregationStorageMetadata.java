package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.aggregation.reductions.ReductionFunction;
import org.apache.hadoop.io.Writable;

import java.io.Serializable;

public class AggregationStorageMetadata<K extends Writable, V extends Writable> implements Serializable {
    private Class<? extends AggregationStorage> aggStorageClass;
    private Class<K> keyClass;
    private Class<V> valueClass;
    private boolean persistent;
    private ReductionFunction<V> reductionFunction;
    private EndAggregationFunction<K, V> endAggregationFunction;
    private boolean isIncremental;

    public AggregationStorageMetadata(Class<? extends AggregationStorage> aggStorageClass,
            Class<K> keyClass, Class<V> valueClass,
            boolean persistent,
            ReductionFunction<V> reductionFunction,
            EndAggregationFunction<K, V> endAggregationFunction,
            boolean isIncremental) {
        this.aggStorageClass = aggStorageClass; 
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.persistent = persistent;
        this.reductionFunction = reductionFunction;
        this.endAggregationFunction = endAggregationFunction;
        this.isIncremental = isIncremental;
    }

    public Class<? extends AggregationStorage> getAggregationStorageClass() {
        return aggStorageClass;
    }

    public Class<K> getKeyClass() {
        return keyClass;
    }

    public Class<V> getValueClass() {
        return valueClass;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public boolean isIncremental() {
        return isIncremental;
    }

    public ReductionFunction<V> getReductionFunction() {
        return reductionFunction;
    }

    public EndAggregationFunction<K, V> getEndAggregationFunction() {
        return endAggregationFunction;
    }

    @Override
    public String toString() {
        return "AggregationStorageMetadata{" +
                "keyClass=" + keyClass +
                ", valueClass=" + valueClass +
                ", persistent=" + persistent +
                ", reductionFunction=" + reductionFunction +
                ", endAggregationFunction=" + endAggregationFunction +
                '}';
    }
}
