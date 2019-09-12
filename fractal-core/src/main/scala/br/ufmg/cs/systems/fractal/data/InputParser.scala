package br.ufmg.cs.systems.fractal.data

import org.apache.spark.{SparkConf, SparkContext}

object InputParser {
  def time[R](block: => R): (R, Long) = { 
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    (result, t1 - t0)
  }
  
  def adjLists2edgeList(
      sc: SparkContext,
      inputFile: String,
      outputFile: String): Unit = {

    sc.textFile(inputFile).
      filter (line => !line.startsWith("#")).
      flatMap { line =>
        val arr = line.trim().split("\\s+").map(_.toLong)
        var res = List[(Long,Long)]()
        var i = 2
        while (i < arr.length) {
          res = (arr(0), arr(i)) :: res
          i += 1
        }
        res
      }.
      sortByKey().
      map {case (v1,v2) => s"${v1}\t${v2}"}.
      saveAsTextFile(outputFile)
  }

  def edgeList2adjLists(
      sc: SparkContext,
      inputFile: String,
      outputFile: String): Unit = {

    val verticeMap = sc.textFile(inputFile).
      filter (line => !line.startsWith("#")).
      flatMap { line =>
        val arr = line.trim().split("\\s+").map(_.toLong)
        Iterator(arr(0), arr(1))
      }.
      distinct().
      zipWithIndex().
      collectAsMap()

    val verticeMapBc = sc.broadcast(verticeMap)

    val edges = sc.textFile(inputFile).
      filter (line => !line.startsWith("#")).
      flatMap { line =>
        val arr = line.trim().split("\\s+").map(_.toLong)
        val verticeMap = verticeMapBc.value
        Iterator(
          (verticeMap(arr(0)), verticeMap(arr(1))),
          (verticeMap(arr(1)), verticeMap(arr(0))))
      }.
      distinct().
      groupByKey().
      sortByKey().
      map { case (from,tos) =>
        s"${from} 1 ${tos.mkString(" ")}"
      }.
      saveAsTextFile(outputFile)
  }

  def main(args: Array[String]) {
    // args
    var i = 0
    val option = args(i)
    i += 1
    val inputFile = args(i)
    i += 1
    val outputFile = args(i)

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    option match {
      case "edgeList2adjLists" =>
        edgeList2adjLists(sc, inputFile, outputFile)
      case "adjLists2edgeList" =>
        adjLists2edgeList(sc, inputFile, outputFile)
      case other =>
        throw new RuntimeException(s"Unknown option: ${other}")
    }
    
    sc.stop()
  }
}
