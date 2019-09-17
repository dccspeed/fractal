package br.ufmg.cs.systems.fractal

import java.util.UUID

import br.ufmg.cs.systems.fractal.computation.ActorMessageSystem
import br.ufmg.cs.systems.fractal.conf.Configuration
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

/**
  * Starting point for Fractal execution engine (currently Spark)
  * @param sc Spark context
  * @param logLevel log level: "error", "warn", "info"
  * @param tmpDir temporary directory to store fractal data/metadata
  */
class FractalContext(sc: SparkContext, logLevel: String = "info",
                     tmpDir: String = Configuration.CONF_TMP_DIR_DEFAULT)
    extends Logging {

  private val uuid: UUID = UUID.randomUUID

  def tmpPath: String = s"${tmpDir}-${uuid}" // TODO: base dir as config

  def sparkContext: SparkContext = sc

  /**
    * Read graph from text file
    * @param path
    * @param graphClass specifies how the graph is read (default adj. lists)
    * @param local specifies whether this path is in the local fs or not
    * @return fractal graph
    */
  def textFile (path: String,
      graphClass: String = Configuration.CONF_MAINGRAPH_CLASS_DEFAULT,
      local: Boolean = false): FractalGraph = {
    new FractalGraph(path, graphClass, this, logLevel)
  }

  /**
    * Stop this context, cleaning the temporary directory
    */
  def stop() = {
    val fs = FileSystem.get (sc.hadoopConfiguration)
    val res = fs.delete (new Path(tmpPath), true)
    ActorMessageSystem.shutdown()
    logInfo (s"Removing fractal temp directory: ${tmpPath} (exists=${res})")
  }
}
