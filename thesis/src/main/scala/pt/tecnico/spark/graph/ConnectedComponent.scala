package pt.tecnico.spark.graph

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by dikei on 2/12/16.
  */
object ConnectedComponent {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Usage: ")
      println("spark-submit --class pt.tecnico.spark.graph.ConnectedComponent [jar] [input] [output]")
      System.exit(0)
    }

    val input = args(0)
    val output = args(1)
    val conf = new SparkConf().setAppName("ConnectedComponent")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val graph = GraphLoader.edgeListFile(sc, input)

    // Calculate and save the connected components
    graph.connectedComponents().vertices.saveAsTextFile(output)
  }
}
