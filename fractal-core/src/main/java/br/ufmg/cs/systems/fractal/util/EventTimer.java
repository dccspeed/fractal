package br.ufmg.cs.systems.fractal.util;

/**
 * Timer used to measure incrementally periods of predetermined events.
 * Access to instances of this class should be through 'workerInstance' or
 * 'masterInstance' calls. This timer automatically prints its stats at JVM
 * shutdown via a configured hook during object instantiation.
 */
public class EventTimer {

   /**
    * This flag indicates to the compiler whether code in this project should
    * be instrumented by this timer. We leverage a JVM behavior that tests on
    * final static variables can be optimized at compile time.
    */
   public static final boolean ENABLED = true;

   /**
    * This private class is used for lazy initialization of static fields for
    * master instances of this timer
    */
   private static class EventTimerHolderMaster {
      private static final int CAPACITY = 64;
      static EventTimer[] INSTANCES = new EventTimer[CAPACITY];
      static {
         for (int i = 0; i < CAPACITY; ++i) {
            INSTANCES[i] = new EventTimer(
                    "EventTimerMaster[" + i + "]");
         }
      }
   }

   /**
    * This private class is used for lazy initialization of static fields
    */
   private static class EventTimerHolderWorker {
      private static final int CAPACITY = 64;
      static EventTimer[] INSTANCES = new EventTimer[CAPACITY];
      static {
         for (int i = 0; i < CAPACITY; ++i) {
            INSTANCES[i] = new EventTimer(
                    "EventTimerWorker[" + i + "]");
         }
      }
   }

   /**
    * Events to be timed
    */
   public static final int INITIALIZATION = 0;
   public static final int ENUMERATION_FILTERING = 1;
   public static final int AGGREGATION = 2;
   private static final int NONE = 3;
   private static final String[] EVENT_NAMES = {
           "initialization",
           "enumeration_filtering",
           "aggregation",
           "none"
   };

   /**
    * Auxiliary fields for controlling the state of this timer
    */
   private int ongoingEvents;
   private long creationTime;

   /**
    * let it fail: indicates whether this timer is being used correctly
    */
   private boolean inconsistent;

   /**
    * Time vector for each event (elapsed time)
    */
   private long[] eventElapsedTimes;
   private long[] eventLastTimes;

   public EventTimer(String name) {
      int numEvents = EVENT_NAMES.length;
      this.eventElapsedTimes = new long[numEvents];
      this.eventLastTimes = new long[numEvents];
      this.ongoingEvents = 0;
      this.creationTime = System.nanoTime();
      this.inconsistent = false;

      // start 'none' event
      startNoneEvent();

      // shutdown hook to print all event times
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {

         inconsistent = inconsistent || ongoingEvents != 0;

         // last event to finish must be the none event
         finishNoneEvent();

         // we append 'Inconsistent' to this timer name in case it is
         // inconsistent
         String timerName = inconsistent ? "Inconsistent" + name : name;

         // iterate over real events (not none), printing stats if they are
         // non-empty
         boolean nonEmptyEvent = false;
         long totalElapsedFromCreationTime = System.nanoTime() - creationTime;
         long totalElapsedFromEvents = 0;
         for (int event = 0; event < numEvents; ++event) {
            if (event == NONE) continue;

            long timeNano = eventElapsedTimes[event];
            if (timeNano == 0) continue;

            totalElapsedFromEvents += timeNano;
            long timeMs = (long) (timeNano * 1e-6);
            System.out.printf("%s %s %d (ms)\n",
                    timerName, EVENT_NAMES[event], timeMs);

            // found non-empty event that is not 'none'
            nonEmptyEvent = true;
         }

         // maybe print none events and total
         if (nonEmptyEvent) {
            // print none event
            long timeNano = eventElapsedTimes[NONE];
            totalElapsedFromEvents += timeNano;
            long timeMs = (long) (timeNano * 1e-6);
            System.out.printf("%s %s %d (ms)\n",
                    timerName, EVENT_NAMES[NONE], timeMs);

            long elapsed1 = (long) (totalElapsedFromCreationTime * 1e-6);
            long elapsed2 = (long) (totalElapsedFromEvents * 1e-6);
            System.out.printf("%s total %d %d (ms)\n",
                    timerName, elapsed1, elapsed2);
         }
      }));
   }

   /**
    * Worker instance referenced by this index
    * @param index unique identifier of worker
    * @return timer
    */
   public static EventTimer workerInstance(int index) {
      return EventTimerHolderWorker.INSTANCES[index];
   }

   /**
    * Master instance referenced by this index
    * @param index unique identifier of master
    * @return timer
    */
   public static EventTimer masterInstance(int index) {
      return EventTimerHolderMaster.INSTANCES[index];
   }

   /**
    * Used by this class to indicate time windows where no real event is
    * active (ongoing events)
    */
   private void startNoneEvent() {
      inconsistent = inconsistent || eventLastTimes[NONE] != 0;
      eventLastTimes[NONE] = System.nanoTime();
   }

   /**
    * Used by this class to indicate the end of a 'none' event
    */
   public void finishNoneEvent() {
      long lastEventTime = eventLastTimes[NONE];
      inconsistent = inconsistent || lastEventTime == 0;
      eventElapsedTimes[NONE] += System.nanoTime() - lastEventTime;
      eventLastTimes[NONE] = 0;
   }

   /**
    * Marks the start of an available event (see static fields in this class)
    * @param event event unique index
    */
   public void start(int event) {
      if (ongoingEvents == 0) {
         finishNoneEvent();
      }

      inconsistent = inconsistent || eventLastTimes[event] != 0;
      eventLastTimes[event] = System.nanoTime();

      ++ongoingEvents;
   }

   /**
    * Marks the end of an available event (see static fields in this class)
    * @param event event unique index
    */
   public void finish(int event) {
      long lastEventTime = eventLastTimes[event];
      inconsistent = inconsistent || lastEventTime == 0;
      eventElapsedTimes[event] += System.nanoTime() - lastEventTime;
      eventLastTimes[event] = 0;

      if (--ongoingEvents == 0) {
         startNoneEvent();
      }
   }

   /**
    * Marks the transition between an event to another, without gaps between
    * them (i.e., without a 'none' event period)
    * @param finishEvent finishing event
    * @param startEvent starting event
    */
   public void finishAndStart(int finishEvent, int startEvent) {
      long currentTime = System.nanoTime();

      // finish event at currentTime
      long lastEventTime = eventLastTimes[finishEvent];
      inconsistent = inconsistent || lastEventTime == 0;
      eventElapsedTimes[finishEvent] += currentTime - lastEventTime;
      eventLastTimes[finishEvent] = 0;

      // start event at currentTime
      inconsistent = inconsistent || eventLastTimes[startEvent] != 0;
      eventLastTimes[startEvent] = currentTime;
   }

}
