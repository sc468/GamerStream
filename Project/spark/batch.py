############################################################
#Spark Batch Job
############################################################
#!usr/bin/python3

import os
# add dependency to use spark with kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell'

import numpy as np
# Spark
import pyspark
from pyspark import SparkContext
# Spark Streaming
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
# Kafka
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
import json, math, datetime
from kafka.consumer import SimpleConsumer

from operator import add
# rethinkDB
#import rethinkdb as r

def extractKills(v):
    try:
        output = (int(v.split(',')[3]), int(v.split(',')[2]), int(v.split(',')[4]))
        return output
    except:
        return (None, None, None)

###################################################
##                     Main                      ## 
###################################################

def main():
    conf = SparkConf()
    sc = SparkContext()
    sc.setLogLevel("WARN")

    sortedTime = sc.textFile("s3a://steve-dota2/KillStream.txt")\
        .map(extractKills)\
        .sortBy(lambda v: v[0])
    
    result = sortedTime.collect()  
    with open("SortedKillTimes.txt", "w+") as outputFile:
        for line in result:
            outputFile.write(str(line)+ '\n')
    
    return


if __name__ == '__main__':
    main()
