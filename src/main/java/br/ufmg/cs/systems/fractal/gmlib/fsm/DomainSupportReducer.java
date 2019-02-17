package br.ufmg.cs.systems.fractal.gmlib.fsm;

import br.ufmg.cs.systems.fractal.aggregation.reductions.ReductionFunction;

public class DomainSupportReducer
   extends ReductionFunction<DomainSupport> {
    @Override
    public DomainSupport reduce(DomainSupport k1, DomainSupport k2) {
        k1.aggregate(k2);

        return k1;
    }
}
