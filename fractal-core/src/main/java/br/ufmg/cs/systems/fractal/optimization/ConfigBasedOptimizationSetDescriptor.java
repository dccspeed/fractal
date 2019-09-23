package br.ufmg.cs.systems.fractal.optimization;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.util.ReflectionUtils;

public class ConfigBasedOptimizationSetDescriptor implements OptimizationSetDescriptor {
    public static final String CONF_OPTIMIZATION_CLASSES = "fractal.optimizations";
    public static final Class[] CONF_OPTMIZATION_CLASSES_DEFAULT = {};

    @Override
    public OptimizationSet describe(Configuration configuration) {
        OptimizationSet optimizationSet = new OptimizationSet();

        Class[] optimizationClasses = configuration.getClasses(CONF_OPTIMIZATION_CLASSES);

        for (Class optimizationClass : optimizationClasses) {
            if (!Optimization.class.isAssignableFrom(optimizationClass)) {
                throw new RuntimeException("Class " + optimizationClass + " does not implement hte Optimization interface");
            }

            Optimization optimization = (Optimization) ReflectionUtils.newInstance(optimizationClass);
            optimization.init(configuration);
            optimizationSet.add(optimization);
        }

        return optimizationSet;
    }
}
