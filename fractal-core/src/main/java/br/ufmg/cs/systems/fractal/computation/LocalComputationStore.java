package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import com.koloboke.collect.map.*;
import com.koloboke.collect.map.hash.HashIntLongMaps;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.koloboke.collect.map.hash.HashObjLongMaps;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.ObjSet;
import com.koloboke.collect.set.hash.HashIntSets;
import com.koloboke.collect.set.hash.HashObjSets;
import com.koloboke.function.IntLongConsumer;
import org.apache.log4j.Logger;
import scala.Int;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.ObjLongConsumer;

import static java.util.concurrent.TimeUnit.SECONDS;

public class LocalComputationStore {
   private static final Logger LOG = Logger.getLogger(LocalComputationStore.class);
   private static final long EXPIRE_TIME_MS = 1000;

   // active stage computations
   private static final ObjObjMap<IntArrayList, ObjArrayList<Computation<? extends Subgraph>>>
           activeComputations = HashObjObjMaps.newMutableMap();
   private static final ObjLongMap<IntArrayList> finishedTimes =
           HashObjLongMaps.newMutableMap();

   // active stage computations
   //private static final IntObjMap<ObjArrayList<Computation<? extends Subgraph>>>
   //        activeStageComputations = HashIntObjMaps.newMutableMap();
   //private static final IntLongMap stageFinishedTimes =
   //        HashIntLongMaps.newMutableMap();

   // internal clean-up scheduler
   private static final ScheduledExecutorService scheduler =
           Executors.newScheduledThreadPool(1);

   // static class initializer
   static {

      final ObjSet<IntArrayList> expiredStagesSteps = HashObjSets.newUpdatableSet();
      final ObjLongConsumer<IntArrayList> expireredStagesStepsConsumer = (key, time) -> {
         long elapsed = System.currentTimeMillis() - time;
         if (elapsed >= EXPIRE_TIME_MS) expiredStagesSteps.add(key);
      };

      //final IntSet expiredStages = HashIntSets.newUpdatableSet();
      //final IntLongConsumer expireredStagesConsumer = (stageId, time) -> {
      //   long elapsed = System.currentTimeMillis() - time;
      //   if (elapsed >= EXPIRE_TIME_MS) expiredStages.add(stageId);
      //};

      final Consumer<IntArrayList> finishedStagesStepsTimesCleaner = key -> {
         finishedTimes.removeAsLong(key);
      };
      final Consumer<IntArrayList> activeComputationsStagesStepsCleaner = key -> {
         activeComputations.remove(key);
      };
      final Consumer<IntArrayList> stagesStepsKeysReclaimer = key -> {
         IntArrayListPool.instance().reclaimObject(key);
      };

      //final IntConsumer finishedTimesCleaner = stageId -> {
      //   stageFinishedTimes.remove(stageId);
      //};
      //final IntConsumer activeComputationsCleaner = stageId -> {
      //   activeStageComputations.remove(stageId);
      //};

      //// clean-up task
      //final Runnable expirerer = () -> {
      //   expiredStages.clear();
      //   stageFinishedTimes.forEach(expireredStagesConsumer);
      //   synchronized (stageFinishedTimes) {
      //      expiredStages.forEach(finishedTimesCleaner);
      //   }
      //   synchronized (activeStageComputations) {
      //      expiredStages.forEach(activeComputationsCleaner);
      //   }

      //   int numClearedStages = expiredStages.size();
      //   if (numClearedStages > 0) {
      //      LOG.info("ClearedStages" + " numClearedStages=" + numClearedStages +
      //              " activeComputations=" + activeStageComputations.size());
      //   }
      //};

      // clean-up task
      final Runnable expirererStagesSteps = () -> {
         expiredStagesSteps.clear();
         finishedTimes.forEach(expireredStagesStepsConsumer);
         synchronized (finishedTimes) {
            expiredStagesSteps.forEach(finishedStagesStepsTimesCleaner);
         }
         synchronized (activeComputations) {
            expiredStagesSteps.forEach(activeComputationsStagesStepsCleaner);
         }

         expiredStagesSteps.forEach(stagesStepsKeysReclaimer);
         expiredStagesSteps.clear();

         int numClearedStagesSteps = expiredStagesSteps.size();
         if (numClearedStagesSteps > 0) {
            LOG.info("ClearedStagesSteps" + " numClearedStagesSteps=" + numClearedStagesSteps +
                    " activeComputations=" + activeComputations.size());
         }
      };

      //scheduler.scheduleAtFixedRate(expirerer, 1, 1, SECONDS);
      scheduler.scheduleAtFixedRate(expirererStagesSteps, 1, 1, SECONDS);
   }

   //public static ObjArrayList<Computation<? extends Subgraph>> localComputations(
   //        int stageId) {
   //   return activeStageComputations.get(stageId);
   //}

   public static ObjArrayList<Computation<? extends Subgraph>> localComputations(
           int stageId, int stepId) {
      IntArrayList key = IntArrayListPool.instance().createObject();
      key.add(stageId);
      key.add(stepId);
      ObjArrayList computations = activeComputations.get(key);
      IntArrayListPool.instance().reclaimObject(key);
      return computations;
   }

   public static void createComputationsMap(SparkEngine<? extends Subgraph> engine) {
      int stageId = engine.stageId();
      int stepId = engine.getStep();

      //ObjArrayList<Computation<? extends Subgraph>> computations =
      //        activeStageComputations.get(stageId);
      //if (computations == null) {
      //   synchronized (activeStageComputations) {
      //      computations = activeStageComputations.get(stageId);
      //      if (computations == null) {
      //         activeStageComputations.put(stageId, new ObjArrayList<>());
      //      }
      //   }
      //}

      IntArrayList key = IntArrayListPool.instance().createObject();
      key.add(stageId);
      key.add(stepId);
      ObjArrayList<Computation<? extends Subgraph>> computations =
              activeComputations.get(key);
      if (computations == null) {
         synchronized (activeComputations) {
            computations = activeComputations.get(key);
            if (computations == null) {
               activeComputations.put(key, new ObjArrayList<>());
            }
         }
      }


   }

   public static void registerComputation(Computation<? extends Subgraph> computation) {
      int stageId = computation.getExecutionEngine().getStageId();
      int stepId = computation.getExecutionEngine().getStep();

      //ObjArrayList<Computation<? extends Subgraph>> computations =
      //        activeStageComputations.get(stageId);

      //synchronized (computations) {
      //   computations.add(computation);
      //}

      IntArrayList key = IntArrayListPool.instance().createObject();
      key.add(stageId);
      key.add(stepId);
      ObjArrayList<Computation<? extends Subgraph>> computations =
              activeComputations.get(key);

      while (computations == null) {
         try {
            LOG.info("ActiveComputationsMapNotReady. Trying again.");
            Thread.sleep(100);
            computations = activeComputations.get(key);
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
      }

      synchronized (computations) {
         computations.add(computation);
      }

      IntArrayListPool.instance().reclaimObject(key);
   }

   public static void unregisterComputation(SparkEngine<? extends Subgraph> engine) {
      int stageId = engine.getStageId();
      int stepId = engine.getStep();

      //long existingTime = stageFinishedTimes.getOrDefault(stageId, -1);
      //if (existingTime == -1) {
      //   synchronized (stageFinishedTimes) {
      //      existingTime = stageFinishedTimes.getOrDefault(stageId, -1);
      //      if (existingTime == -1) {
      //         long currentTime = System.currentTimeMillis();
      //         stageFinishedTimes.put(stageId, currentTime);
      //      }
      //   }
      //}

      IntArrayList key = IntArrayListPool.instance().createObject();
      key.add(stageId);
      key.add(stepId);
      boolean reclaim = true;
      long existingTime = finishedTimes.getOrDefault(key, -1);
      if (existingTime == -1) {
         synchronized (finishedTimes) {
            existingTime = finishedTimes.getOrDefault(key, -1);
            if (existingTime == -1) {
               long currentTime = System.currentTimeMillis();
               finishedTimes.put(key, currentTime);
               reclaim = false;
            }
         }
      }

      if (reclaim) IntArrayListPool.instance().reclaimObject(key);
   }

   public static void shutdown() {
      scheduler.shutdownNow();
   }
}
