package br.ufmg.cs.systems.fractal.graph;

import com.koloboke.collect.map.ObjLongCursor;
import com.koloboke.collect.map.ObjLongMap;
import com.koloboke.collect.map.ObjObjMap;
import com.koloboke.collect.map.hash.HashObjLongMaps;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import org.apache.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Store holding main graphs in memory, used to prevent parsing at each
 * fractal step. Main graphs expire automatically after a fixed period, by
 * releasing object reference for GC.
 */
public class MainGraphStore {
   private static final Logger LOG = Logger.getLogger(MainGraphStore.class);

   // TODO: This class is holding every main graph for now, change this to
   //  prevent leak in the future
   private static final long EXPIRE_TIME_MS = Long.MAX_VALUE;

   // internal clean-up scheduler
   private static final ScheduledExecutorService scheduler =
           Executors.newScheduledThreadPool(1);

   // last time a graph were fetched
   private static final ObjLongMap<String> lastAccess =
           HashObjLongMaps.newMutableMap();

   // store: in-memory graphs
   private static final ObjObjMap<String,MainGraph> mainGraphs =
           HashObjObjMaps.newMutableMap();

   // static class initializer
   static {

      // clean-up task
      final Runnable expirerer = () -> {
         long currentTime = System.currentTimeMillis();
         synchronized (lastAccess) {
            ObjLongCursor<String> cur = lastAccess.cursor();
            while (cur.moveNext()) {
               // remove old main graph (expired)
               if (currentTime - cur.value() >= EXPIRE_TIME_MS) {
                  mainGraphs.remove(cur.key());
                  LOG.info("Removed graph " + cur.key());
                  cur.remove();
               }
            }
         }
      };

      scheduler.scheduleAtFixedRate(expirerer, 5, 5, SECONDS);

   }

   /**
    * Adds a main graph to this store
    * @param path file path representing this graph
    * @param graph main graph
    */
   public static void put(String path, MainGraph graph) {
      synchronized (lastAccess) {
         lastAccess.put(path, System.currentTimeMillis());
         mainGraphs.put(path, graph);
      }
   }

   /**
    * Retrieves a main graph given its path
    * @param path file path representing a graph
    * @return in-memory main graph or null if not found
    */
   public static MainGraph get(String path) {
      MainGraph graph;
      synchronized (lastAccess) {
         graph = mainGraphs.get(path);
         if (graph != null) {
            lastAccess.put(path, System.currentTimeMillis());
         }
      }
      return graph;
   }

   public static void shutdown() {
      scheduler.shutdownNow();
   }
}
