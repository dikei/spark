package pt.tecnico.spark

import java.io.{File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.scheduler.{StatsReportListener, SparkListenerApplicationStart}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.supercsv.io.{CsvBeanWriter, CsvListWriter}
import org.supercsv.prefs.CsvPreference
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
    val statisticDir = args(2)
    // Do the word count and save output
    val createCombiner = (v: Int) => v
    val mergeValue = (a: Int, b: Int) => a + b
    val mergeCombiners = (a: Int, b: Int) => {
      a + b
    }

    val now = Calendar.getInstance()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    val timeStamp = dateFormat.format(now.getTime)
    val fileName = s"stats-$timeStamp.txt"

    val headers = Array (
      "StageId", "Average", "Fastest", "Slowest", "StandardDeviation"
    )

    val csvWriter = new CsvBeanWriter(new FileWriter(new File(statisticDir, fileName)), CsvPreference.STANDARD_PREFERENCE)
    csvWriter.writeHeader(headers:_*)

    val reportListener = new StageRuntimeReportListener(csvWriter, headers)
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

    csvWriter.close()

//      .filter(t => t._2 > 20000)
//      .take(50)
//    println("50 words with more than 20000 occurances")
//    out.foreach(println)


  }
}
