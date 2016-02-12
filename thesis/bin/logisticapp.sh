#!/bin/bash

spark-submit --class pt.tecnico.spark.logistic.LogisticRegressionApp \
    target/scala-2.10/thesis_2.10-1.6.0.jar \
    /home/dikei/Tools/tmp/spark-testing/data/logistic \
    /home/dikei/Tools/tmp/spark-testing/out/logistic