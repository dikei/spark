package pt.tecnico.spark.matrix

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Matrix factorization app
  */
object MatrixFactorApp {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("Usage: ")
      println("spark-submit --class pt.tecnico.spark.matrix.MatrixFactorApp " +
        "[jar] [input] [output] [rank] [iteration] [statsDir]")
      System.exit(0)
    }

    val input = args(0)
    val output = args(1)
    val rank = args(2).toInt
    val iteration = args(3).toInt
    val statsDir = if (args.length > 4) args(4) else "stats"

    val conf = new SparkConf().setAppName("Matrix Factorization")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
//    conf.set("spark.scheduler.removeStageBarrier", "true")

    val sc = new SparkContext(conf)
    sc.addSparkListener(new StageRuntimeReportListener(statsDir))

    val ratings = sc.textFile(input)
      .map { line =>
        val t = line.split(',')
        new Rating(t(0).toInt, t(1).toInt, t(2).toDouble)
      }

    val model = ALS.train(ratings, rank, iteration)

    val usersProducts = ratings.map(rating => (rating.user, rating.product))

    val predictions = model.predict(usersProducts).map { p =>
      ((p.user, p.product), p.rating)
    }
    val ratesAndPreds = ratings.map { r =>
      ((r.user, r.product), r.rating)
    }.join(predictions)

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()
    println("Mean Squared Error = " + MSE)

  }
}
