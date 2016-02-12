package pt.tecnico.spark.graph

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by dikei on 2/12/16.
  */
object PageRank {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: ")
      println("spark-submit --class pt.tecnico.spark.graph.PageRankSimple [jar] [input] [output] [#iteration]")
      System.exit(0)
    }

    val input = args(0)
    val output = args(1)
    val iteration = if (args.length > 2) args(2).toInt else 10

    val conf = new SparkConf().setAppName("PageRankGraphx")
    conf.set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, input)

    // Run page rank algorithm and save the result
    graph.staticPageRank(iteration).vertices.saveAsTextFile(output)
  }
}
