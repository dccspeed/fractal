package br.ufmg.cs.systems.fractal.gmlib

import java.io.Serializable

import br.ufmg.cs.systems.fractal.FractalGraph
import br.ufmg.cs.systems.fractal.util.{EventTimer, Logging}

trait BuiltInApplication[T] extends Serializable with Logging {
   def apply(fg: FractalGraph): T

   /* INSTRUMENTATION {{ */

   def initializationStart(): Unit = if (EventTimer.ENABLED) {
      EventTimer.masterInstance(0).start(EventTimer.INITIALIZATION)
   }

   def aggregationFinishInitializationStart(): Unit = if (EventTimer.ENABLED) {
      val instance = EventTimer.masterInstance(0)

      synchronized {
         while (!instance.eventIsActive(EventTimer.AGGREGATION)) {
            wait(10)
         }
      }
      instance.finishAndStart(
         EventTimer.AGGREGATION, EventTimer.INITIALIZATION
      )
   }

   def initializationFinish(): Unit = if (EventTimer.ENABLED) {
      EventTimer.masterInstance(0).finish(EventTimer.INITIALIZATION)
   }

   /* }} INSTRUMENTATION */
}
