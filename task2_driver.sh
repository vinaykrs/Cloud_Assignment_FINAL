#!/bin/bash

spark-submit \
    --master local[4] \
    Task2_Spark.py \
    --input file:///home/hadoop/data/ \
    --output file:///home/hadoop/task2_out/
