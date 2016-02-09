package pt.tecnico.spark

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Word count program to test Spark
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    conf.set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)

    val inputFile = args(0)
    val outputFile = args(1)
    // Do the word count and save output
    val createCombiner = (v: Int) => v
    val mergeValue = (a: Int, b: Int) => a + b
    val mergeCombiners = (a: Int, b: Int) => {
      Thread.sleep(1)
      a + b
    }
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
