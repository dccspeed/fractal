package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.pattern.Pattern;

public class AggregationStorageFactory {
    
    private Configuration configuration;

    public AggregationStorageFactory(Configuration config) {
       this.configuration = config;
    }

    public AggregationStorage createAggregationStorage(String name) {
        AggregationStorageMetadata metadata = configuration.getAggregationMetadata(name);

        return createAggregationStorage(name, metadata);
    }

    public AggregationStorage createAggregationStorage(String name, AggregationStorageMetadata metadata) {
        if (metadata == null) {
            throw new RuntimeException(
                  "Attempted to create unregistered aggregation storage: " +
                  name);
        }

        Class<?> keyClass = metadata.getKeyClass();

        if (Pattern.class.isAssignableFrom(keyClass)) {
            return new PatternAggregationStorage(name, metadata);
        } else {
            AggregationStorage aggStorage = configuration.createAggregationStorage(name);
            aggStorage.init(name, metadata);
            return aggStorage;
        }
    }
}
