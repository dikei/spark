package pt.tecnico.spark

import org.apache.spark.{SparkContext, SparkConf}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Created by dikei on 3/10/16.
  */
object Sort {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: ")
      println("spark-submit --class pt.tecnico.spark.Sort [jar] [input] [output] [statsDir]")
      System.exit(0)
    }

    val input = args(0)
    val output = args(1)
    val statsDir = if (args.length > 2) args(2) else "stats"

    val conf = new SparkConf().setAppName("Sort")
    conf.set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)
    sc.addSparkListener(new StageRuntimeReportListener(statsDir))

    sc.textFile(input).sortBy(s => s).saveAsTextFile(output)
  }

}
