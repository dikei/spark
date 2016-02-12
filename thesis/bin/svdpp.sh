#!/bin/bash

spark-submit --class pt.tecnico.spark.graph.SVDPP \
    target/scala-2.10/thesis_2.10-1.6.0.jar \
    /home/dikei/Tools/tmp/spark-testing/data/graph \
    /home/dikei/Tools/tmp/spark-testing/out/svdpp \
    1 \
    50 \
    0.0 \
    5.0 \
    0.007 \
    0.007 \
    0.007 \
    0.015