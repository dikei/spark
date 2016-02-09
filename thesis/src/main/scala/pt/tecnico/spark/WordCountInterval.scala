package pt.tecnico.spark

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Word count program to test Spark
  */
object WordCountInterval {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountInterval")
    conf.set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)

    val inputFile = args(0)
    val outputFile = args(1)
    val createCombiner = (v: Int) => v
    val mergeValue = (a: Int, b: Int) => a + b
    val mergeCombiners = (a: Int, b: Int) => {
      Thread.sleep(1)
      a + b
    }
    // Do the word count and perform aggregate every 60s
    val ret = sc.textFile(inputFile)
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .combineByKeyWithInterval[Int](
        createCombiner,
        mergeValue,
        mergeCombiners,
        new HashPartitioner(4),
        interval = 60
      )

//      val words = ret.filter(t => t._2 > 20000).take(50)
//      println("50 words with more than 20000 occurrences")
//      words.foreach(println)

      // Combine the interval and output the final value
      ret.reduceByKey((a, b) => if (a < b ) b else a)
        .saveAsTextFile(outputFile)
  }
}
