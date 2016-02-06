#!/bin/bash

spark-submit --class pt.tecnico.spark.TopKCount target/scala-2.10/thesis_2.10-1.6.0.jar /home/dikei/Tools/tmp/spark-testing/data/pride.txt 10
