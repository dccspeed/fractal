package io.arabesque.optimization;

import io.arabesque.conf.Configuration;

public interface OptimizationSetDescriptor {
    OptimizationSet describe(Configuration config);
}
