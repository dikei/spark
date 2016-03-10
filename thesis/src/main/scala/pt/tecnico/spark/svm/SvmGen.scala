package pt.tecnico.spark.svm

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import pt.tecnico.spark.util.StageRuntimeReportListener
import scala.util.Random

/**
  * Data generator for Logistic Regression
  */
object SvmGen {

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: ")
      println("spark-submit pt.tecnico.spark.svm.SvmGen [output] [#examples] [#features] [#partitions] [statsDir]")
      System.exit(1)
    }

    val outputPath = args(0)
    val nexamples = if (args.length > 1) args(1).toInt else 1000
    val nfeatures = if (args.length > 2) args(2).toInt else 2
    val parts = if (args.length > 3) args(3).toInt else 2
    val statsDir = if (args.length > 4) args(4) else "stats"

    val conf = new SparkConf().setAppName("SVMGenerator")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    sc.addSparkListener(new StageRuntimeReportListener(statsDir))

    val globalRnd = new Random(94720)
    val trueWeights = Array.fill[Double](nfeatures)(globalRnd.nextGaussian())

    val data: RDD[LabeledPoint] = sc.parallelize(0 until nexamples, parts).map { idx =>
      val rnd = new Random(42 + idx)

      val x = Array.fill[Double](nfeatures) {
        rnd.nextDouble() * 2.0 - 1.0
      }
      val yD = blas.ddot(nfeatures, x, 1, trueWeights, 1) + rnd.nextGaussian() * 0.1
      val y = if (yD < 0) 0.0 else 1.0
      LabeledPoint(y, Vectors.dense(x))
    }

    data.saveAsTextFile(outputPath)

    sc.stop()
  }
}