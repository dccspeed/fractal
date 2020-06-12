package br.ufmg.cs.systems.fractal.gmlib.fsm;

import br.ufmg.cs.systems.fractal.aggregation.AggregationStorage;
import br.ufmg.cs.systems.fractal.aggregation.EndAggregationFunction;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DomainSupportEndAggregationFunction
        implements EndAggregationFunction<Pattern, MinImageSupport> {
    private static final Logger LOG = Logger.getLogger(DomainSupportEndAggregationFunction.class);
    @Override
    public void endAggregation(AggregationStorage<Pattern, MinImageSupport> aggregationStorage) {
        Set<Pattern> patternsToRemove = new HashSet<>();

        Map<Pattern, MinImageSupport> finalMapping = aggregationStorage.getMapping();

        for (Map.Entry<Pattern, MinImageSupport> finalMappingEntry : finalMapping.entrySet()) {
            Pattern pattern = finalMappingEntry.getKey();
            MinImageSupport minImageSupport = finalMappingEntry.getValue();
            LOG.info(pattern + " " + minImageSupport);

            if (!minImageSupport.hasEnoughSupport()) {
                patternsToRemove.add(pattern);
            }
        }

        aggregationStorage.removeKeys(patternsToRemove);
    }
}
