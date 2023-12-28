package br.ufmg.cs.systems.fractal.aggregation;

import br.ufmg.cs.systems.fractal.computation.ExecutionEngine;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.ProducerConsumerSignaling;
import com.koloboke.collect.set.ObjSet;
import com.koloboke.collect.set.hash.HashObjSets;
import org.apache.log4j.Logger;

import java.io.Serializable;

public abstract class ObjSubgraphAggregation
        <S extends Subgraph, K extends Serializable>
        extends ProducerConsumerSignaling
        implements SubgraphAggregation<S> {
   private static final Logger LOG = Logger.getLogger(ObjSubgraphAggregation.class);

   private static final int MAX_SIZE = 10000;

   private ObjSet<K> objSet;

   public final void init(Configuration configuration) {
      objSet = HashObjSets.newUpdatableSet();
   }

   public final void map(K obj) {
      objSet.add(obj);
      if (objSet.size() > MAX_SIZE) {
         // wait until map is consumed
         notifyWorkProduced();
         waitWorkConsumed();
         objSet.clear();
      }
   }

   public final ObjSet<K> getObjSet() {
      return objSet;
   }

   @Override
   public void report(ExecutionEngine<S> engine) {
      // empty by default
   }
}
