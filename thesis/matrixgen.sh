#!/bin/bash

spark-submit --class pt.tecnico.spark.matrix.MatrixGen \
    target/scala-2.10/thesis_2.10-1.6.0.jar \
    /home/dikei/Tools/tmp/spark-testing/data/matrix \
    3500 \
    3000 \
    500 \
    0.9 \
    false \
    0.1 \
    false \
    0.1