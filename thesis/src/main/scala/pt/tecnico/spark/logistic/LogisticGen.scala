package pt.tecnico.spark.logistic

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.util.LogisticRegressionDataGenerator

/**
  * Data generator for Logistic Regression
  */
object LogisticGen {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("Usage: ")
      println("spark-submit --class pt.tecnico.spark.LogisticGen [jar] [output] [#examples] [#feature] [eps] [#part] [prob1]")
      System.exit(0)
    }

    val output = args(0)
    val noExamples = args(1).toInt
    val noFeatures = args(2).toInt
    val eps = args(3).toDouble
    val noPart = if (args.length >= 5) args(4).toInt else 2
    val probOne = if (args.length >= 6) args(5).toDouble else 0.5

    val conf = new SparkConf().setAppName("LogisticRegressionDataGenerator")
    conf.set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)

    LogisticRegressionDataGenerator
      .generateLogisticRDD(sc, noExamples, noFeatures, eps, noPart, probOne)
      .saveAsTextFile(output)
  }
}
