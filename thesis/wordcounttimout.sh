#!/bin/bash

spark-submit --class pt.tecnico.spark.WordCountTimeout target/scala-2.10/thesis_2.10-1.6.0.jar /home/dikei/Tools/tmp/spark-testing/data/pride.txt /home/dikei/Tools/tmp/spark-testing/out/pride.timeout
