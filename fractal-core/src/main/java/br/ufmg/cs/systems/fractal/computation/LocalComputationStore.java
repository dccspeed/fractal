package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import com.koloboke.collect.map.*;
import com.koloboke.collect.map.hash.HashIntLongMaps;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import com.koloboke.function.IntLongConsumer;
import org.apache.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.IntConsumer;

import static java.util.concurrent.TimeUnit.SECONDS;

public class LocalComputationStore {
   private static final Logger LOG = Logger.getLogger(LocalComputationStore.class);
   private static final long EXPIRE_TIME_MS = 5000;

   // active stage computations
   private static final IntObjMap<ObjArrayList<Computation<? extends Subgraph>>>
           activeStageComputations = HashIntObjMaps.newMutableMap();
   private static final IntLongMap stageFinishedTimes =
           HashIntLongMaps.newMutableMap();

   // internal clean-up scheduler
   private static final ScheduledExecutorService scheduler =
           Executors.newScheduledThreadPool(1);

   // static class initializer
   static {

      final IntSet expiredStages = HashIntSets.newUpdatableSet();
      final IntLongConsumer expireredStagesConsumer = (stageId, time) -> {
         long elapsed = System.currentTimeMillis() - time;
         if (elapsed >= EXPIRE_TIME_MS) expiredStages.add(stageId);
      };
      final IntConsumer finishedTimesCleaner = stageId -> {
         stageFinishedTimes.remove(stageId);
      };
      final IntConsumer activeComputationsCleaner = stageId -> {
         activeStageComputations.remove(stageId);
      };

      // clean-up task
      final Runnable expirerer = () -> {
         expiredStages.clear();
         stageFinishedTimes.forEach(expireredStagesConsumer);
         synchronized (stageFinishedTimes) {
            expiredStages.forEach(finishedTimesCleaner);
         }
         synchronized (activeStageComputations) {
            expiredStages.forEach(activeComputationsCleaner);
         }

         int numClearedStages = expiredStages.size();
         if (numClearedStages > 0) {
            LOG.info("ClearedStages" + " numStages=" + numClearedStages +
                    " activeComputations=" + activeStageComputations.size());
         }
      };

      scheduler.scheduleAtFixedRate(expirerer, 5, 5, SECONDS);
   }

   public static ObjArrayList<Computation<? extends Subgraph>> localComputations(
           int stageId) {
      return activeStageComputations.get(stageId);
   }

   public static void createComputationsMap(SparkEngine<? extends Subgraph> engine) {
      int stageId = engine.stageId();
      ObjArrayList<Computation<? extends Subgraph>> computations =
              activeStageComputations.get(stageId);
      if (computations == null) {
         synchronized (activeStageComputations) {
            computations = activeStageComputations.get(stageId);
            if (computations == null) {
               activeStageComputations.put(stageId, new ObjArrayList<>());
            }
         }
      }
   }

   public static void registerComputation(Computation<? extends Subgraph> computation) {
      int stageId = computation.getExecutionEngine().getStageId();
      ObjArrayList<Computation<? extends Subgraph>> computations =
              activeStageComputations.get(stageId);

      synchronized (computations) {
         computations.add(computation);
      }
   }

   public static void unregisterComputation(SparkEngine<? extends Subgraph> engine) {
      if (engine.getPartitionId() == 0) {
         int stageId = engine.getStageId();
         long currentTime = System.currentTimeMillis();

         synchronized (stageFinishedTimes) {
            stageFinishedTimes.put(stageId, currentTime);
         }
      }
   }

   public static void shutdown() {
      scheduler.shutdownNow();
   }
}
