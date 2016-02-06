package pt.tecnico.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Word count program to test Spark
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    conf.set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)

    val inputFile = args(0)
    val outputFile = args(1)
    // Do the word count and save output
    sc.textFile(inputFile)
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b, 4)
      .saveAsTextFile(outputFile)
  }
}
