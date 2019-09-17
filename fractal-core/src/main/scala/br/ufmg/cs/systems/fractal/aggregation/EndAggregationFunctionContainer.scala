package br.ufmg.cs.systems.fractal.aggregation

import org.apache.hadoop.io.Writable

case class EndAggregationFunctionContainer[K <: Writable, V <: Writable] (
    func: (AggregationStorage[K,V]) => Unit) extends EndAggregationFunction[K,V] {
  def endAggregation(aggStorage: AggregationStorage[K,V]): Unit = func (aggStorage)
}
