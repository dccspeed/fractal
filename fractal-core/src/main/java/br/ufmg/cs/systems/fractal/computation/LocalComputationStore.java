package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import com.koloboke.collect.map.IntObjCursor;
import com.koloboke.collect.map.IntObjMap;
import com.koloboke.collect.map.hash.HashByteObjMaps;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import net.sf.cglib.core.Local;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

public class LocalComputationStore {
   private static final Logger LOG = Logger.getLogger(LocalComputationStore.class);
   private static final AtomicInteger lastStageId = new AtomicInteger();
   private static final IntObjMap<ObjArrayList<Computation<? extends Subgraph>>>
           activeComputations =
           HashIntObjMaps.newMutableMap();

   private static void maybeUpdateLastStageId(SparkEngine<? extends Subgraph> engine) {
      int existing = lastStageId.get();
      if (existing < engine.stageId()) {
         lastStageId.compareAndSet(existing, engine.stageId());
      }
   }

   private static void cleanOldComputations() {
      int existing = lastStageId.get();

      synchronized (activeComputations) {
         IntObjCursor<ObjArrayList<Computation<? extends Subgraph>>> cur =
                 activeComputations.cursor();
         while (cur.moveNext()) {
            if (cur.key() < existing - 1) {
               LOG.info("CleaningStage stageId=" + cur.key() +
                       " computations=" + cur.value().size() +
                       " activeStages=" + activeComputations.size());
               cur.remove();
            }
         }
      }
   }

   public static ObjArrayList<Computation<? extends Subgraph>> localComputations(int stageId) {
      return activeComputations.get(stageId);
   }

   public static void createComputationsMap(SparkEngine<? extends Subgraph> engine) {
      int stageId = engine.stageId();
      maybeUpdateLastStageId(engine);
      ObjArrayList<Computation<? extends Subgraph>> computations =
              activeComputations.get(stageId);
      if (computations == null) {
         synchronized (activeComputations) {
            computations = activeComputations.get(stageId);
            if (computations == null) {
               activeComputations.put(stageId, new ObjArrayList<>());
            }
         }
      }
   }

   public static void registerComputation(Computation<? extends Subgraph> computation) {
      int stageId = computation.getExecutionEngine().getStageId();
      ObjArrayList<Computation<? extends Subgraph>> computations =
              activeComputations.get(stageId);

      synchronized (computations) {
         computations.add(computation);
      }
   }

   public static void unregisterComputation(SparkEngine<? extends Subgraph> engine) {
      if (engine.getPartitionId() == 0) {
         cleanOldComputations();
      }
   }
}
