package pt.tecnico.spark.svm

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.{SparkContext, SparkConf}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Created by dikei on 2/10/16.
  */
object SvmApp {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: ")
      println("spark-submit --class pt.tecnico.spark.svm.SvmApp [jar] [input] [output] [#iterations] [statsDir]")
      System.exit(0)
    }

    val input = args(0)
    val output = args(1)
    val noIteration = if (args.length > 2) args(2).toInt else 100
    val statsDir = if (args.length > 3) args(3) else "stats"
    val conf = new SparkConf().setAppName("SvmApp")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    sc.addSparkListener(new StageRuntimeReportListener(statsDir))

    val data = MLUtils.loadLabeledPoints(sc, input)
    val splits = data.randomSplit(Array(0.6, 0.4), 13290)

    val training = splits(0)
    training.cache()

    val model = SVMWithSGD.train(training, noIteration)

    val validation = splits(1)
    val predictionResult = validation.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
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
