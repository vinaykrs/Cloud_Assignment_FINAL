#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Invalid number of parameters!"
    echo "Usage: ./task1_driver.sh [input_location] [output_location]"
    exit 1
fi

hadoop jar /usr/lib/hadoop/hadoop-streaming-2.8.5-amzn-2.jar \
-D mapreduce.job.name='Workload For Task 1' \
-D mapred.reduce.tasks=1 \
-file task1_mapper.py \
-mapper task1_mapper.py \
-file task1_reducer.py \
-reducer task1_reducer.py \
-input $1 \
-output $2
