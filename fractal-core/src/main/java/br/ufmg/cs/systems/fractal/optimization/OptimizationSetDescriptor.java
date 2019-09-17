package br.ufmg.cs.systems.fractal.optimization;

import br.ufmg.cs.systems.fractal.conf.Configuration;

public interface OptimizationSetDescriptor {
    OptimizationSet describe(Configuration config);
}
