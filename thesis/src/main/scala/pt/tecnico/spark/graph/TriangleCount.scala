package pt.tecnico.spark.graph

import org.apache.spark.graphx.{PartitionStrategy, GraphLoader}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Counting triangle in the graph
  */
object TriangleCount {

  def main(args: Array[String]): Unit = {

    val input = args(0)
    val output = args(1)

    val conf = new SparkConf().setAppName("TriangleCount")
    conf.set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)
    val graph = GraphLoader.edgeListFile(sc, input, canonicalOrientation = true)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    // Find the triangle count for each vertex
    graph.triangleCount().vertices.saveAsTextFile(output)
  }
}
