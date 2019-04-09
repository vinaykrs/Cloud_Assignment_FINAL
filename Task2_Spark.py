"""
   This program will identify TOP10 controversial trending videos - where the DISLIKE growoth rate is higher than the LIKES growth rate 
   INPUT: 
   OUTPUT: "BEePFpC9qG8", 366556, "Film & Animation", "DE"
           "RmZ3DPJQo2k", 334594, "Music", "KR"
   PACKAGES: my_utils.py - user defined functions/utilities to achieve the desired output
             heapq - is used to find the smallest
   Note: Some parts of code is referred from Internet mostly Stackoverflow  
"""

from my_utils import *
from pyspark import SparkContext
from heapq import nsmallest
import argparse


if __name__ == "__main__":
    sc = SparkContext(appName="Workload 2 Task 2")
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="the input path",
                        default='~/data/')
    parser.add_argument("--output", help="the output path", 
                        default='task2_out') 
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output
    #Reading the input file
    youtube = sc.textFile(input_path+'changed.csv')
    header = youtube.first() #extract header
    youtube = youtube.filter(lambda row: row != header)#Filtering the header record


    test_youtube = youtube.map(extractVideo)#Extract the required data columns by extractVideo function call
    rddTop2 = test_youtube.groupBy(lambda x: x[0]+x[5]).flatMap(lambda g: nsmallest(2, g[1], key=lambda x: x[1]))#GroupBY based on VideoId and Country and take first 2 records for each group based on trending date
    rddKeyValue = rddTop2.map(extractKeyValue)#Construct Key Value Pairs
    rddReduce = rddKeyValue.reduceByKey(lambda x,y: (y[0],y[1],y[3]-x[3],y[2]-x[2],x[4]+y[4])) ## reduce by key to get difference between within dislikes and within likes for each key
    rddFilter = rddReduce.filter(lambda x: x[1][4] > 1)# Filter all keys having only one record
    rddReduceMap = rddFilter.map(lambda x: (x[0], x[1][0], x[1][1],x[1][2]-x[1][3]))# Calculate the dislike growth rate
    rddSortByDislikes = rddReduceMap.sortBy(lambda x: x[3],ascending=False)#Order the records based on the dislike growth rate in descending order
    rddTop10 = sc.parallelize(rddSortByDislikes.take(10))# Take TOP 10 and convert it to RDD
    rddTop10.map(recordFormat).coalesce(1).saveAsTextFile(output_path)# Create an OUTPUT file for TOP 10 Dislike Growth records
