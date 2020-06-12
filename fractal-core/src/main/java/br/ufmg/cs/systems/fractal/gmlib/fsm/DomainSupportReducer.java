package br.ufmg.cs.systems.fractal.gmlib.fsm;

import br.ufmg.cs.systems.fractal.aggregation.reductions.ReductionFunction;

public class DomainSupportReducer
   extends ReductionFunction<MinImageSupport> {
    @Override
    public MinImageSupport reduce(MinImageSupport k1, MinImageSupport k2) {
        k1.aggregate(k2);

        return k1;
    }
}
