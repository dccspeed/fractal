package br.ufmg.cs.systems.fractal

import java.util.UUID

import br.ufmg.cs.systems.fractal.computation.ActorMessageSystem
import br.ufmg.cs.systems.fractal.conf.Configuration
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

/**
  * Creates an fractal Context from a Spark Context
  *
  * Example of usage:
  * {{{
  * import br.ufmg.cs.systems.fractal.fractalContext
  * val arab = new fractalContext(sc)
  * arab: br.ufmg.cs.systems.fractal.fractalContext = br.ufmg.cs.systems.fractal.fractalContext@3a996bbc
  * }}}
  *
  * @param sc a [[SparkContext]] instance
  * @return an [[br.ufmg.cs.systems.fractal.FractalContext]]
  *
 */
class FractalContext(sc: SparkContext, logLevel: String = "info",
                     tmpDir: String = "/tmp/fractal")
    extends Logging {

  private val uuid: UUID = UUID.randomUUID

  def tmpPath: String = s"${tmpDir}-${uuid}" // TODO: base dir as config

  def sparkContext: SparkContext = sc

  /**
    *  Indicates the path for input graph
    *
    * {{{
    *  val file_path = "~/input.graph" // Set the input file path
    *  graph = arab.textFile(file_path)
    *  graph: br.ufmg.cs.systems.fractal.fractalGraph = br.ufmg.cs.systems.fractal.fractalGraph@2310a619
    * }}}
    *
    * @param path a string indicating the path for input graph
    * @param local TODO: Describe local variable
    * @return an [[br.ufmg.cs.systems.fractal.FractalGraph]]
    * @see [[https://github.com/viniciusvdias/fractal/blob/master/README.md]]
    * for how to prepare the input file
    */
  def textFile (path: String,
      graphClass: String = Configuration.CONF_MAINGRAPH_CLASS_DEFAULT,
      local: Boolean = false): FractalGraph = {
    new FractalGraph(path, graphClass, this, logLevel)
  }

  /**
    * Stops an [[br.ufmg.cs.systems.fractal.FractalContext]] application
    * {{{
    * arab.stop()
    * }}}
    */
  def stop() = {
    val fs = FileSystem.get (sc.hadoopConfiguration)
    val res = fs.delete (new Path(tmpPath), true)
    ActorMessageSystem.shutdown()
    logInfo (s"Removing fractal temp directory: ${tmpPath} (exists=${res})")
  }
}
