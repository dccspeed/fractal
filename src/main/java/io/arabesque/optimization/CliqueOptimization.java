package io.arabesque.optimization;

import io.arabesque.conf.Configuration;
import io.arabesque.graph.MainGraph;
import org.apache.log4j.Logger;

public class CliqueOptimization extends BasicOptimization {
    private static final Logger LOG = Logger.getLogger(CliqueOptimization.class);

    @Override
    public void applyAfterGraphLoad() {
        MainGraph mainGraph = configuration.getMainGraph();

        configuration.setMainGraph(new BiggerNeighboursMainGraphDecorator(mainGraph));
        configuration.setEmbeddingClass(CliqueVertexInducedEmbedding.class);
    }

    @Override
    public String toString() {
        return "CliqueOptimization";
    }
}
