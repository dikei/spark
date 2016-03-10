package pt.tecnico.spark

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Word count program to test Spark
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
//    conf.set("spark.scheduler.removeStageBarrier", "true")

    val sc = new SparkContext(conf)

    val inputFile = args(0)
    val outputFile = args(1)
    val statisticDir = if (args.length > 2) args(2) else "stats"

    // Do the word count and save output
    val createCombiner = (v: Int) => v
    val mergeValue = (a: Int, b: Int) => a + b
    val mergeCombiners = (a: Int, b: Int) => {
      a + b
    }

    val reportListener = new StageRuntimeReportListener(statisticDir)
    sc.addSparkListener(reportListener)

    val out = sc.textFile(inputFile)
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .combineByKey(
        createCombiner,
        mergeValue,
        mergeCombiners,
        new HashPartitioner(4)
      )
      .reduceByKey((a, b) => a + b)
      .saveAsTextFile(outputFile)

//      .filter(t => t._2 > 20000)
//      .take(50)
//    println("50 words with more than 20000 occurances")
//    out.foreach(println)


  }
}
