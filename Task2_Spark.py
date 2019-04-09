from my_utils import *
from pyspark import SparkContext
from heapq import nsmallest
import argparse


if __name__ == "__main__":
    sc = SparkContext(appName="Average Rating per Genre")
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="the input path",
                        default='~/comp5349/lab_commons/week5/')
    parser.add_argument("--output", help="the output path", 
                        default='rating_out_script') 
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output
    youtube = sc.textFile(input_path+'changed.csv')
    header = youtube.first() #extract header
    youtube = youtube.filter(lambda row: row != header)


    test_youtube = youtube.map(extractVideo)
    rddTop2 = test_youtube.groupBy(lambda x: x[0]+x[5]).flatMap(lambda g: nsmallest(2, g[1], key=lambda x: x[1]))

    rddKeyValue = rddTop2.map(extractKeyValue)

    rddReduce = rddKeyValue.reduceByKey(lambda x,y: (y[0],y[1],y[3]-x[3],y[2]-x[2],x[4]+y[4])) ## reduce by key to get difference between dislikes
    rddFilter = rddReduce.filter(lambda x: x[1][4] > 1)

    rddReduceMap = rddFilter.map(lambda x: (x[0], x[1][0], x[1][1],x[1][2]-x[1][3]))

    rddSortByDislikes = rddReduceMap.sortBy(lambda x: x[3],ascending=False)

    rddTop10 = sc.parallelize(rddSortByDislikes.take(10))

    rddTop10.map(recordFormat).repartition(1).saveAsTextFile(output_path)
