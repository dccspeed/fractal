package br.ufmg.cs.systems.fractal.gmlib.motifs

import br.ufmg.cs.systems.fractal.FractalGraph
import br.ufmg.cs.systems.fractal.gmlib.BuiltInApplication
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.rdd.RDD

class MotifsSF(numVertices: Int)
   extends BuiltInApplication[RDD[(Pattern,Long)]] {

   override def apply(fg: FractalGraph): RDD[(Pattern, Long)] = {
      fg.vfractoid.expand(numVertices).aggregationCanonicalPatternLong(
         s => {
            s.quickPattern()
         }, 0, _ => 1L, _ + _)
   }
}
