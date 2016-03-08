package pt.tecnico.spark.graph

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}
import pt.tecnico.spark.util.StageRuntimeReportListener

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
    val statisticDir = args(3)

    val conf = new SparkConf().setAppName("PageRankGraph")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
//    conf.set("spark.scheduler.removeStageBarrier", "true")

    val sc = new SparkContext(conf)
    val reportListener = new StageRuntimeReportListener(statisticDir)
    sc.addSparkListener(reportListener)

    val graph = GraphLoader.edgeListFile(sc, input)

    // Run page rank algorithm and save the result
    graph.staticPageRank(iteration).vertices.saveAsTextFile(output)
  }
}
