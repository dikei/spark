package pt.tecnico.spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Count the K-most frequent word
  */
object TopKCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TopKCount")
    conf.set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)

    val inputFile = args(0)
    val k = if (args.length == 2) args(1).toInt else 10
    // Do the count and save output
    val result = sc.textFile(inputFile)
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b, 4)
      .sortBy[Int](t => t._2, ascending = false)
      .take(k)
    println(s"Most $k frequent word: ")
    result.foreach(println)
  }
}
