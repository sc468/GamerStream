############################################################
# This python script is the main script for spark streaming. 
#
############################################################


import os
# add dependency to use spark with kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell'

import numpy as np
# Spark
from pyspark import SparkContext
# Spark Streaming
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

# cassandra
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
#from cassandra.cqlengine.query import BatchType
from cassandra.query import BatchType
import time

# configuration file
#import config


###################################################
##                   Functions                   ## 
###################################################

def sendCassandra(iter):
    print("send to cassandra")
#    cluster = Cluster(['54.214.213.178', '52.88.247.214', '54.190.18.13', '52.41.141.29'])
    cluster = Cluster(['52.11.210.69', '50.112.90.110', '54.149.158.21'])
    session = cluster.connect()
    session.execute('USE ' + "PlayerKills")

    insert_statement = session.prepare("UPDATE killerstats SET kills = kills + ? WHERE time = ? AND killerhero = ? AND victimhero = ? ")
    insert_statement2 = session.prepare("UPDATE victimstats SET kills = kills + ? WHERE time = ? AND killerhero = ? AND victimhero = ? ")
    count = 0

    # batch insert into cassandra database
#    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    batch = BatchStatement( batch_type=BatchType.COUNTER)
    
    for record in iter:
        batch.add(insert_statement,(record[1][3], record[1][0], record[1][1], record[1][2]))
        batch.add(insert_statement2,(record[1][3], record[1][0], record[1][1], record[1][2]))
 # split the batch, so that the batch will not exceed the size limit
        count += 1
        if count % 250 == 0:
            startTime = time.time()
            session.execute(batch)
            elapsedTime= time.time()-startTime
            with open ('/home/ubuntu/outputWriteTime.txt','a+') as outputFile:
                print ('250 counts')
               # outputFile.write(str(count))
               # outputFile.write(', ')
               # outputFile.write(str(elapsedTime))
               # outputFile.write('\n')
            batch = BatchStatement( batch_type=BatchType.COUNTER)
    # send the batch that is less than 500            
    startTime = time.time()

    session.execute(batch)

    elapsedTime= time.time()-startTime
    with open ('/home/ubuntu/outputWriteTime.txt','a+') as outputFile:
        print ('Sending batch')
        #outputFile.write(str(count))
        #outputFile.write(', ')
        #outputFile.write(str(elapsedTime))
        #outputFile.write('\n')
    session.shutdown()

def extractKiller(v):
    try:
        key = (int(v[0]), int(v[2]), int(v[3]))
        return(key, (int(v[0]), int(v[2]), int(v[3]), 1))
    except:
        return (None, (None, None, None))

def extractKiller2(v):
    try:
        key = v[0]
        return(key, (int(v[0]), 0, 0, 1))
    except:
        return (None, (None, None, None))

###################################################
##                     Main                      ## 
###################################################

def main():


    sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
    sc.setLogLevel("WARN")
    
    # set microbatch interval seconds
    ssc = StreamingContext(sc, 2)
    
    # create a direct stream from kafka without using receiver
    kafkaStream = KafkaUtils.createDirectStream(ssc, ['data'], {"metadata.broker.list":"localhost:9092"})
    
    # Transform player name into key
    prekiller = kafkaStream.map(lambda v:v[1][1:-2].split(','))\

    killer = prekiller\
                .map(extractKiller)\
                .reduceByKey(lambda x, y: (x[0], x[1], x[2], x[3]+y[3]) )

###################################################################
#SERIAL Map Reduce job
#    # Transform time into key, aggregate all kills

    killer.cache()
    totalkills = killer\
                .map(lambda x: (x[1][0], (x[1][0], 0, 0, x[1][3])))\
                .reduceByKey(lambda x, y: (x[0], x[1], x[2], x[3]+y[3]) )
###########################################   

###################################################################
# PARALLEL Map Reduce Job
#    # Transform time into key, aggregate all kills
#    totalkills = prekiller.map(extractKiller2)\
#                .map(lambda x: (x[1][0], (x[1][0], 0, 0, x[1][3])))\
#                .reduceByKey(lambda x, y: (x[0], x[1], x[2], x[3]+y[3]) )

###########################################   
    # Send data to cassandra    
    totalkills.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandra))   

    # Send data to cassandra    
    killer.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandra))
    #resultSimple_ds.foreachRDD(sendRethink)
    killer.foreachRDD(lambda rdd: rdd.unpersist())
    
    ssc.start()
    ssc.awaitTermination()
    return


if __name__ == '__main__':
    main()



