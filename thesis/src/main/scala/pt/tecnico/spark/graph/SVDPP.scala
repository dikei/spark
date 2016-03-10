package pt.tecnico.spark.graph

import org.apache.spark.graphx.lib.SVDPlusPlus.Conf
import org.apache.spark.graphx.{Edge, GraphLoader}
import org.apache.spark.graphx.lib.SVDPlusPlus
import org.apache.spark.{SparkContext, SparkConf}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Created by dikei on 2/12/16.
  */
object SVDPP {

  def main(args: Array[String]): Unit = {
    if (args.length < 9) {
      println("Usage: ")
      println("spark-submit --class pt.tecnico.spark.graph.SVDPP " +
        "[jar] [input] [output] [rank] [#iteration] [minVal] [maxVal] [gamma1] [gamma2] [gamma6] [gamma7] [statsDir]")
      System.exit(0)
    }

    val input = args(0)
    val output = args(1)
    val rank = args(2).toInt
    val iteration = args(3).toInt
    val minVal = args(4).toDouble
    val maxVal = args(5).toDouble
    val gamma1 = args(6).toDouble
    val gamma2 = args(7).toDouble
    val gamma6 = args(8).toDouble
    val gamma7 = args(9).toDouble
    val statsDir = if (args.length > 10) args(10) else "stats"

    val conf = new SparkConf().setAppName("SVD++")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    sc.addSparkListener(new StageRuntimeReportListener(statsDir))

    val graph = GraphLoader.edgeListFile(sc, input)
    val edges = graph.edges.map( e => new Edge(e.srcId, e.dstId, e.attr.toDouble))

    val svdconf = new Conf(rank, iteration, minVal, maxVal, gamma1, gamma2, gamma6, gamma7)

    val (result, _) = SVDPlusPlus.run(edges, svdconf)

    // Force execution
    val count = result.triplets.count()

    println(s"Triplets: $count")
  }
}
