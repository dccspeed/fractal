package br.ufmg.cs.systems.fractal.util

import org.apache.log4j.{Level, LogManager, Logger}

/**
 * Logging utility used primary in scala classes.
 */
trait Logging {
   protected def logName = this.getClass.getSimpleName

   protected def log = Logger.getLogger (logName)

   /** client functions are called by name in order to avoid unecessary string
    *  building **/

   protected def logInfo(msg: => String): Unit = if (log.isInfoEnabled) {
      log.info(msg)
   }

   protected def logWarn(msg: => String): Unit = if (log.isEnabledFor(Level.WARN)) {
      log.warn(msg)
   }

   protected def logDebug(msg: => String): Unit = if (log.isDebugEnabled) {
      log.debug(msg)
   }

   protected def logError(msg: => String): Unit = if (log.isEnabledFor(Level.ERROR)) {
      log.error(msg)
   }

   protected def logApp(msg: => String): Unit = if (log.isEnabledFor(FractalAppLogLevel.APP)) {
      log.log(FractalAppLogLevel.APP, msg)
   }

   /** **/

   protected def setLogLevel(level: String): Unit = {
      val logLevel = Level.toLevel(level.toUpperCase, FractalAppLogLevel.APP)
      LogManager.getRootLogger.setLevel(logLevel)
   }
}

object Logging {
   private val log = getLogger("Logging")
   def getLogger(name: String) = Logger.getLogger(name)
   def logApp(msg: String) : Unit = {
      if (log.isEnabledFor(FractalAppLogLevel.APP)) {
         log.log(FractalAppLogLevel.APP, msg)
      }
   }

   def logWarn(msg: String) : Unit = {
      if (log.isEnabledFor(Level.WARN)) {
         log.log(Level.WARN, msg)
      }
   }

}
