package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.aggregation.AggregationStorage;
import org.apache.hadoop.io.Writable;

public class MasterComputation {
    private CommonMasterExecutionEngine executionEngine;

    public void init() {
        // Do nothing by default
    }

    public void compute() {
        // Do nothing by default
    }

    public int getStep() {
        return (int) executionEngine.getStep();
    }

    public void setUnderlyingExecutionEngine(CommonMasterExecutionEngine executionEngine) {
        this.executionEngine = executionEngine;
    }
}
