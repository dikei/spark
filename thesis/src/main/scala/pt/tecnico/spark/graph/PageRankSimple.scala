/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
Copy from org.apache.spark.examples.SparkPageRank
 */
package pt.tecnico.spark.graph

import org.apache.spark.{SparkConf, SparkContext}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Computes the PageRank of URLs from an input file. Input file should
  * be in format of:
  * URL         neighbor URL
  * URL         neighbor URL
  * URL         neighbor URL
  * ...
  * where URL and their neighbors are separated by space(s).
  *
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * please refer to org.apache.spark.graphx.lib.PageRank
  */
object PageRankSimple {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: spark-submit --class pt.tecnico.spark.graph.PageRankSimple [jar] [input] [output] [iteration] [statsDir]")
      System.exit(0)
    }

    val input = args(0)
    val output = args(1)
    val iters = if (args.length > 2) args(2).toInt else 10
    val statsDir = if (args.length > 3) args(3) else "stats"

    val sparkConf = new SparkConf().setAppName("PageRankSimple")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false")
//    sparkConf.set("spark.scheduler.removeStageBarrier", "true")

    val ctx = new SparkContext(sparkConf)
    ctx.addSparkListener(new StageRuntimeReportListener(statsDir))

    val lines = ctx.textFile(input, 1)
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    ranks.saveAsTextFile(output)

    ctx.stop()
  }
}