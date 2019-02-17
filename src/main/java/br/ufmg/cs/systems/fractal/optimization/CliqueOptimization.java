package br.ufmg.cs.systems.fractal.optimization;

import br.ufmg.cs.systems.fractal.graph.MainGraph;
import org.apache.log4j.Logger;

public class CliqueOptimization extends BasicOptimization {
    private static final Logger LOG = Logger.getLogger(CliqueOptimization.class);

    @Override
    public void applyAfterGraphLoad() {
        MainGraph mainGraph = configuration.getMainGraph();

        configuration.setMainGraph(new BiggerNeighboursMainGraphDecorator(mainGraph));
        configuration.setSubgraphClass(CliqueVertexInducedSubgraph.class);
    }

    @Override
    public String toString() {
        return "CliqueOptimization";
    }
}
