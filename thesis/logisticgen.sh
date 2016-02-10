#!/bin/bash

spark-submit --class pt.tecnico.spark.logistic.LogisticGen \
    target/scala-2.10/thesis_2.10-1.6.0.jar \
    /home/dikei/Tools/tmp/spark-testing/data/logistic \
    5000000 \
    10 \
    0.5 \
    4 \
    0.4