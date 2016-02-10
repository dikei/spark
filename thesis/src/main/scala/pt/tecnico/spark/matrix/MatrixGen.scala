package pt.tecnico.spark.matrix

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import scala.util.Random

/**
  * Matrix factorization generator
  */
object MatrixGen {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: ")
      println("spark-submit pt.tecnico.spark.matrix.MatrixGen " +
        "[output] [m] [n] [rank] [trainSampFact] [noise] [sigma] [test] [testSampFact]")
      System.exit(0)
    }

    val outputPath = args(0)
    val m = if (args.length > 1) args(1).toInt else 100
    val n = if (args.length > 2) args(2).toInt else 100
    val rank = if (args.length > 3) args(3).toInt else 10
    val trainSampFact = if (args.length > 4) args(4).toDouble else 1.0
    val noise = if (args.length > 5) args(5).toBoolean else false
    val sigma = if (args.length > 6) args(6).toDouble else 0.1
    val test = if (args.length > 7) args(7).toBoolean else false
    val testSampFact = if (args.length > 8) args(8).toDouble else 0.1

    val conf = new SparkConf().setAppName("MFDataGenerator")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val random = new java.util.Random(42L)

    val A = DenseMatrix.randn(m, rank, random)
    val B = DenseMatrix.randn(rank, n, random)
    val z = 1 / math.sqrt(rank)
    val fullData = DenseMatrix.zeros(m, n)
    gemm(z, A, B, 1.0, fullData)

    val df = rank * (m + n - rank)
    val sampSize = math.min(math.round(trainSampFact * df), math.round(.99 * m * n)).toInt
    val rand = new Random()
    val mn = m * n
    val shuffled = rand.shuffle((0 until mn).toList)

    val omega = shuffled.slice(0, sampSize)
    val ordered = omega.sortWith(_ < _).toArray
    val trainData: RDD[(Int, Int, Double)] = sc.parallelize(ordered)
      .map(x => (x % m, x / m, fullData.values(x)))

    // optionally add gaussian noise
    if (noise) {
      trainData.map(x => (x._1, x._2, x._3 + rand.nextGaussian * sigma))
    }

    trainData.map(x => x._1 + "," + x._2 + "," + x._3).saveAsTextFile(outputPath)

    // optionally generate testing data
    if (test) {
      val testSampSize = math.min(
        math.round(sampSize * testSampFact), math.round(mn - sampSize)).toInt
      val testOmega = shuffled.slice(sampSize, sampSize + testSampSize)
      val testOrdered = testOmega.sortWith(_ < _).toArray
      val testData: RDD[(Int, Int, Double)] = sc.parallelize(testOrdered)
        .map(x => (x % m, x / m, fullData.values(x)))
      testData.map(x => x._1 + "," + x._2 + "," + x._3).saveAsTextFile(outputPath)
    }

    sc.stop()

  }

  /**
    * C := alpha * A * B + beta * C
    * For `DenseMatrix` A.
    */
  private def gemm(
      alpha: Double,
      A: DenseMatrix,
      B: DenseMatrix,
      beta: Double,
      C: DenseMatrix): Unit = {
    val tAstr = "N"
    val tBstr = "N"
    val lda = A.numRows
    val ldb = B.numRows

    blas.dgemm(tAstr, tBstr, A.numRows, B.numCols, A.numCols, alpha, A.values, lda,
      B.values, ldb, beta, C.values, C.numRows)
  }
}
