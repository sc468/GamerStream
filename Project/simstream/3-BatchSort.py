#HOW TO USE IN COMMAND LINE: 
#python3 3-BatchSort.py <Number of players to simulate>

############################################################
#Spark Batch Job
#Goal: Sort kill events to form simulated stream of kills 
############################################################
#!usr/bin/python3
import sys
#print (sys.argv)
NumPlayers = int(sys.argv[1])
#NumPlayers = 50000
InputFile = "s3a://steve-dota2/3-UnsortedKillStream"+ str(NumPlayers) + ".txt"
OutputFile = '4-SortedKillStream' + str(NumPlayers) + '.txt'
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

#Place time of kill as key (first element)
#Afterwards store player_id, player_hero, and victim
def extractKills(v):
    try:
        output = (int(v.split(',')[3]), int(v.split(',')[1]),   int(v.split(',')[2]), int(v.split(',')[4]))
        return output
    except:
        return (None, None, None, None)

###################################################
##                     Main                      ## 
###################################################

def main():
   # InputFile = "s3a://steve-dota2/KillStream.txt"

    conf = SparkConf()
    sc = SparkContext()
    sc.setLogLevel("WARN")

    sortedTime = sc.textFile(InputFile)\
        .map(extractKills)\
        .filter(lambda v: not v[0] is None)\
        .filter(lambda v: v[0]>=0)\
        .sortBy(lambda v: v[0])
    
    result = sortedTime.collect()  
    with open(OutputFile, "w+") as outputFile:
        for line in result:
            outputFile.write(str(line)+ '\n')
    
    print ('3- Created Sorted Kill Feed')
    print ('Filename : ' , OutputFile)
    return


if __name__ == '__main__':
    main()
