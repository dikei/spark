#!/bin/bash

spark-submit --class pt.tecnico.spark.terasort.TeraSort target/scala-2.10/thesis_2.10-1.6.0.jar /home/dikei/Tools/tmp/spark-testing/data/tera /home/dikei/Tools/tmp/spark-testing/out/tera
