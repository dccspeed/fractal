package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.pattern.Pattern;

public interface PatternAggregationAwareValue {
    public void handleConversionFromQuickToCanonical(Pattern quickPattern, Pattern canonicalPattern);
}
