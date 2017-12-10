package io.arabesque.optimization;

import io.arabesque.conf.Configuration;

public interface Optimization {
    void init(Configuration config);
    void applyStartup();
    void applyAfterGraphLoad();
}
