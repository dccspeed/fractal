package br.ufmg.cs.systems.fractal.extender;

import br.ufmg.cs.systems.fractal.computation.Computation;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import com.koloboke.collect.IntCollection;

import java.io.Externalizable;

public abstract class Extender implements Externalizable {
   
   public abstract IntCollection extend(Subgraph e, Computation c);
   
}
