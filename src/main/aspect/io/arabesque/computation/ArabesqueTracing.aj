package io.arabesque.computation;

import io.arabesque.ArabesqueResult;
import io.arabesque.computation.*;
import io.arabesque.conf.*;
import io.arabesque.embedding.*;
import io.arabesque.utils.Logging$;

import java.lang.StringBuilder;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import org.slf4j.Logger;

public aspect ArabesqueTracing {
   private static Logger log = Logging$.MODULE$.getLogger("ArabesqueTracing");

   private static HashMap<String,Integer> counterIndexes =
      new HashMap<String,Integer>() {{
         put("canonicalFilter", 0);
         put("addWord", 1);
      }};
   
   private static ConcurrentHashMap<Integer,AtomicLongArray> counters =
      new ConcurrentHashMap<Integer,AtomicLongArray>() {{
         put(new Integer(-1), new AtomicLongArray(counterIndexes.size()));
      }};

   private long incAndGetCounter(Integer groupId, String counterName) {
      return counters.get(groupId).incrementAndGet(
            counterIndexes.get(counterName));
   }
   
   private long getCounter(Integer groupId, String counterName) {
      return counters.get(groupId).incrementAndGet(
            counterIndexes.get(counterName));
   }

   ///**
   // * Canonical filter counter
   // */
   //pointcut canonicalFilter():
   //   execution(
   //         * io.arabesque.computation.BasicComputation+.filter(Embedding,int));
   //
   //after() returning(boolean r): canonicalFilter() {
   //   Object[] arguments = thisJoinPoint.getArgs();

   //   BasicEmbedding embedding = (BasicEmbedding) arguments[0];
   //   int word = (int) arguments[1];

   //   incAndGetCounter(embedding.getConfig().getId(), "canonicalFilter");
   //}

   /**
    * Add word counter
    */
   pointcut eembeddingAddWord():
      execution(
            void io.arabesque.embedding.EdgeInducedEmbedding.addWord(int));
   
   pointcut vembeddingAddWord():
      execution(
            void io.arabesque.embedding.VertexInducedEmbedding.addWord(int));


   after(): eembeddingAddWord() || vembeddingAddWord() {
      // BasicEmbedding e = (BasicEmbedding) thisJoinPoint.getThis();
      // incAndGetCounter(e.getConfig().getId(), "addWord");
      incAndGetCounter(-1, "addWord");
   }

   /**
    * Counter grouping
    */

   pointcut gtagCompute():
      execution(* io.arabesque.computation.SparkGtagEngine.compute(..));
   
   pointcut odagCompute():
      execution(* io.arabesque.computation.ODAGEngine.compute(..));
   
   pointcut embeddingCompute():
      execution(* io.arabesque.computation.SparkEmbeddingEngine.compute(..));

   before(): gtagCompute() || odagCompute() || embeddingCompute() {
      SparkEngine e = (SparkEngine) thisJoinPoint.getThis();
      SparkConfiguration config = e.getConfig();
      synchronized (config) {
         if (!counters.containsKey(config.getId())) {
            counters.put(new Integer(config.getId()),
                  new AtomicLongArray(counterIndexes.size()));
         }
      }
   }

   after(): gtagCompute() || odagCompute() || embeddingCompute() {
      SparkEngine e = (SparkEngine) thisJoinPoint.getThis();
      SparkConfiguration config = e.getConfig();
      for (String counterName: counterIndexes.keySet()) {
         long accum = getCounter(-1, counterName);
         if (accum > 0) {
            log.info(thisJoinPoint.getSignature() +
                  " (step=" + e.getSuperstep() + ",partitionId=" +
                  e.getPartitionId() + "," + counterName + "=" + accum + ")");
         }
      }
   }
}
