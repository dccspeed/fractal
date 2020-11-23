package br.ufmg.cs.systems.fractal.gmlib

import java.io.Serializable

import br.ufmg.cs.systems.fractal.FractalGraph
import br.ufmg.cs.systems.fractal.util.Logging

trait BuiltInApplication[T] extends Serializable with Logging {
   def apply(fg: FractalGraph): T
}
