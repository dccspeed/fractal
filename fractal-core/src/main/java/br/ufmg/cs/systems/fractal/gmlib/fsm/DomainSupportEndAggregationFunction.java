package br.ufmg.cs.systems.fractal.gmlib.fsm;

import br.ufmg.cs.systems.fractal.aggregation.AggregationStorage;
import br.ufmg.cs.systems.fractal.aggregation.EndAggregationFunction;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DomainSupportEndAggregationFunction
        implements EndAggregationFunction<Pattern, DomainSupport> {
    private static final Logger LOG = Logger.getLogger(DomainSupportEndAggregationFunction.class);
    @Override
    public void endAggregation(AggregationStorage<Pattern, DomainSupport> aggregationStorage) {
        Set<Pattern> patternsToRemove = new HashSet<>();

        Map<Pattern, DomainSupport> finalMapping = aggregationStorage.getMapping();

        for (Map.Entry<Pattern, DomainSupport> finalMappingEntry : finalMapping.entrySet()) {
            Pattern pattern = finalMappingEntry.getKey();
            DomainSupport domainSupport = finalMappingEntry.getValue();
            LOG.info(pattern + " " + domainSupport);

            if (!domainSupport.hasEnoughSupport()) {
                patternsToRemove.add(pattern);
            }
        }

        aggregationStorage.removeKeys(patternsToRemove);
    }
}
