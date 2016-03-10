package pt.tecnico.spark

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Word count program to test Spark
  */
object WordCountTimeout {

  def main(args: Array[String]): Unit = {
    val inputFile = args(0)
    val outputFile = args(1)
    val statsDir = if (args.length > 2) args(2) else "stats"

    val conf = new SparkConf().setAppName("WordCountTimeout")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    sc.addSparkListener(new StageRuntimeReportListener(statsDir))

    val createCombiner = (v: Int) => v
    val mergeValue = (a: Int, b: Int) => a + b
    val mergeCombiners = (a: Int, b: Int) => {
      Thread.sleep(100)
      a + b
    }
    // Do the word count and save output
    val partial = sc.textFile(inputFile)
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .combineByKeyWithTimeout[Int](
        createCombiner,
        mergeValue,
        mergeCombiners,
        new HashPartitioner(4),
        timeout = 15
      )

      // Save partial result
      partial.saveAsTextFile(outputFile)
  }
}
