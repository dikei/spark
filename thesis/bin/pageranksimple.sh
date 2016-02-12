#!/bin/bash

spark-submit --class pt.tecnico.spark.graph.PageRankSimple \
    target/scala-2.10/thesis_2.10-1.6.0.jar \
    /home/dikei/Tools/tmp/spark-testing/data/web-Google.txt \
    /home/dikei/Tools/tmp/spark-testing/out/pageranksimple \
    10
