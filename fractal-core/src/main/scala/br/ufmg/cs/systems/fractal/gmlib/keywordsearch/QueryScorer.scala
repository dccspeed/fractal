package br.ufmg.cs.systems.fractal.gmlib.keywordsearch

import br.ufmg.cs.systems.fractal.subgraph.EdgeInducedSubgraph
import br.ufmg.cs.systems.fractal.util.collection.InvertedIndexMap
import org.apache.spark.broadcast.Broadcast

class QueryScorer(
    val keywordIndexBc: Broadcast[Array[Int]],
    val invIdxsBc: Broadcast[Array[InvertedIndexMap]],
    val totalInvIdxBc: Broadcast[InvertedIndexMap],
    val invPredicatesBc: Broadcast[Array[InvertedIndexMap]],
    val totalInvPredicateBc: Broadcast[InvertedIndexMap],
    val totalBc: Broadcast[Array[Double]],
    val totalFreq: Long,
    val alpha: Double,
    val beta: Double
  ) extends Serializable {

  def score(e: EdgeInducedSubgraph): Double = {
    var total = 1.0
    var qi = 0
    while (qi < invIdxsBc.value.length) {
      total *= pqiG(qi, e)
      qi += 1
    }
    total
  }

  // equation 2
  private def pqiG(qi: Int, e: EdgeInducedSubgraph): Double = {
    val n = e.getNumWords()
    val words = e.getWords()
    var total = 0.0
    var i = 0
    while (i < n) {
      val dj = words.getUnchecked(i)
      val rj = e.labelledEdge(dj).getEdgeLabel()
      total += (1 / n.toDouble) * pqiDjrj(qi, dj, rj)
      i += 1
    }
    total
  }

  // equation 5
  private def pqiDjrj(qi: Int, dj: Int, rj: Int): Double = {
    val qiDj = pqiDj(qi, dj)
    (beta) * qiDj * pRjqi (rj, qi) + (1 - beta) * qiDj
  }

  // equation 7
  private def pRjqi(rj: Int, qi: Int): Double = {
    val part = pqiRj(qi, rj) * pR(rj)
    part / totalBc.value(qi)
  }

  // equation 6
  private def pqiDj(qi: Int, dj: Int): Double = {
    (alpha) *
    (invIdxsBc.value(qi).getFreq(dj) / totalInvIdxBc.value.getFreq(dj).toDouble) +
    (1 - alpha) *
    (invPredicatesBc.value(qi).getTotalFreq() / totalFreq.toDouble)
  }

  // equation 6
  private def pqiRj(qi: Int, rj: Int): Double = {
    (alpha) *
    (invPredicatesBc.value(qi).getFreq(rj) / totalInvPredicateBc.value.getFreq(rj).toDouble) +
    (1 - alpha) *
    (invPredicatesBc.value(qi).getTotalFreq() / totalFreq.toDouble)
  }

  private def pR(r: Int): Double = {
    totalInvPredicateBc.value.getFreq(r) / totalFreq.toDouble
  }

}
