package br.ufmg.cs.systems.fractal.optimization;

import br.ufmg.cs.systems.fractal.conf.Configuration;

public interface Optimization {
    void init(Configuration config);
    void applyStartup();
    void applyAfterGraphLoad();
}
