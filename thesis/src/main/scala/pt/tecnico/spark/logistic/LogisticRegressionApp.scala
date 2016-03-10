package pt.tecnico.spark.logistic

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.util.MLUtils

import org.apache.spark.{SparkConf, SparkContext}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Created by dikei on 2/10/16.
  */
object LogisticRegressionApp {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: ")
      println("spark-submit --class pt.tecnico.spark.logistic.LogisticRegressionApp [jar] [input] [output] [statsDir]")
      System.exit(0)
    }

    val input = args(0)
    val output = args(1)
    val statsDir = if (args.length > 2) args(2) else "stats"

    val conf = new SparkConf().setAppName("LogisticRegression")
    conf.set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)
    sc.addSparkListener(new StageRuntimeReportListener(statsDir))

    val data = MLUtils.loadLabeledPoints(sc, input)
    val splits = data.randomSplit(Array(0.6, 0.4))

    val training = splits(0)
    // Cache training data
    training.cache()

    val model = new LogisticRegressionWithLBFGS()
      .run(training)

    val validation = splits(1)
    val predictionResult = validation.map { point =>
      val prediction = model.predict(point.features)
      (prediction, point.label)
    }

    // Cache the result
    predictionResult.cache()

    // Save the result
    predictionResult.saveAsTextFile(output)

    // Print analytics of the result

    val metrics = new MulticlassMetrics(predictionResult)
    println("Precision = " + metrics.precision)
    println("Recall = " + metrics.recall)
  }
}
