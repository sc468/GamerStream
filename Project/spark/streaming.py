############################################################
# This python script is the main script for spark streaming. 
# Here is the JSON format of the data from kafka:
#
# {"userid": text, 
#  "time": timestamp, 
#  "acc": float}
# The "acc" column is the acceleration of the user.
#
# The main tasks of thise script is the following:
#
# 1. Receive streaming data from kafka as a Dstream object 
# 2. Take the original Dstream, calculate the window-average,
#    and window-standard-deviation for each user and window,
#    and produce a aggregated Dstream.
# 3. Join the original Dstream with the aggregated Dstream 
#    as a new Dstream
# 4. Using the window-avg and window-std from the aggregated Dstream,
#    label each record from the original Dstream as 'safe' or 'danger'
# 5. Send the list of (userid, status) to rethinkDB
# 6. Send all of the data to cassandra
#
# The parameters
# config.KAFKA_SERVERS: public DNS and port of kafka servers
# config.CHECKPOINT_DIR: check point folder for window process  
# config.ANOMALY_CRITERIA: if abs(data - avg) > config.ANOMALY_CRITERIA * std,
#                          then data is an anomaly
# config.RETHINKDB_SERVER: public DNS of the rethinkDB server
# config.RETHINKDB_DB: name of the database in rethinkDB
# config.RETHINKDB_TABLE: name of the table in rethinkDB
# config.CASSANDRA_SERVERS: public DNS and port of cassandra servers
# config.CASSANDRA_NAMESPACE: namespace for cassandra

# were written in a separate "config.py".
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

# configuration file
#import config


###################################################
##                   Functions                   ## 
###################################################

def getSquared(tuples):

    key = tuples[0]
    val = float(tuples[1][0])

    return (key, (val, val*val, 1))


def getAvgStd(tuples):
    num = tuples[1][0]
    num2 = tuples[1][1]
    n = tuples[1][2]
    std = math.sqrt( (num2/n) - ((num / n) ** 2) )
    avg = num / n
    return (tuples[0], (avg, std))


def labelAnomaly(tuples):
    key = int(tuples[0])
    val = float(tuples[1][0][0])
    time = datetime.datetime.strptime(tuples[1][0][1], "%Y-%m-%d %H:%M:%S %f")
    avg = float(tuples[1][1][0])
    std = float(tuples[1][1][1])
    return 0
#    if np.abs(val - avg) > config.ANOMALY_CRITERIA * np.abs(std):
#        return (key, time, val, avg, std, 'danger')
#    else:
#        return (key, time, val, avg, std, 'safe')
   

def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def getStatusList(rdd):

    # get the singleton instance of SparkSession
    spark = getSparkSessionInstance(rdd.context.getConf())

    # convert RDD[String] to RDD[Row] to DataFrame
    rowRdd = rdd.map(lambda w: Row(userid=w[0], time=w[1], status=w[2]))
    complete_df = spark.createDataFrame(rowRdd)
    complete_df.show()

    # get the dataframe with status 'danger'
 #   danger_df = complete_df.filter(complete_df.status == 'danger')

    # get the latest 'danger' record for each user
 #   danger_window = Window.partitionBy(danger_df['userid'])\
  #                        .orderBy(danger_df['time'].desc())
  #  dangerLatest_df = danger_df.select('*', rank().over(danger_window).alias('rank')) \
   #                            .filter(col('rank') <= 1) 

    # get the complete list of user ID in this rdd
 #   completeID_list = [row.userid for row in complete_df.select('userid').distinct().collect()]
    # get the list of danger and safe user ID in this rdd
   # dangerID_list   = [row.userid for row in dangerLatest_df.select('userid').distinct().collect()]
   # safeID_list = list(set(completeID_list) - set(dangerID_list))
  
    # get the status list of (userid, status, time) tuple from each group (safe and danger)
    # The timestamps for the safe users are not needed
    #dangerStatus_list = [(row.userid, row.status, row.time.strftime("%Y-%m-%d %H:%M:%S %f")) for row in dangerLatest_df.collect()]
    #safeStatus_list = [(ID, 'safe', 'None') for ID in safeID_list]
    #status_list = dangerStatus_list + safeStatus_list  

    #return status_list


def sendCassandra(iter):
    print("send to cassandra")
    cluster = Cluster(['54.214.213.178', '52.88.247.214', '54.190.18.13', '52.41.141.29'])
    session = cluster.connect()
    session.execute('USE ' + "PlayerKills")

    insert_statement = session.prepare("INSERT INTO data (userid, kills) VALUES (?, ?)")

    count = 0

    # batch insert into cassandra database
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    
    for record in iter:
        batch.add(insert_statement,( record[0], record[1]))


        # split the batch, so that the batch will not exceed the size limit
        count += 1
        if count % 500 == 0:
            session.execute(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    # send the batch that is less than 500            
    session.execute(batch)
    session.shutdown()

def mapFunction1(v):
    if v is None or v[1][2] is None or v[1][4] is None:
        return (None, None)
    else:
        return(int(v[1].split(',')[2]),int(v[1].split(',')[4]) )
 

###################################################
##                     Main                      ## 
###################################################

def main():

    sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
    sc.setLogLevel("WARN")
    
    # set microbatch interval seconds
    ssc = StreamingContext(sc, 20)
  #  ssc.checkpoint(config.CHECKPOINT_DIR)
    
    # create a direct stream from kafka without using receiver
    kafkaStream = KafkaUtils.createDirectStream(ssc, ['data'], {"metadata.broker.list":"localhost:9092"})
    
    # parse each record string as json
  #  data_ds = kafkaStream.map(lambda v: json.loads(v[1]))
  #  data_ds.count().map(lambda x:'Records in this batch: %s' % x)\
 #                  .union(data_ds).pprint()
    
    # Transform player name into key
    line = kafkaStream.map(mapFunction1) \
                .reduceByKey(add)
###    line = kafkaStream.map(lambda v: (int(v[1].split(',')[0]),int(v[1].split(',')[0]) )) \
###		.reduceByKey(add)
#    line = kafkaStream.map(lambda v: v)
    line.pprint()
   
    # use the window function to group the data by window
    #dataWindow_ds = data_ds.map(lambda x: (x['userid'], (x['acc'], x['time']))).window(10,10)
    
    '''    

    '''
    #dataWindowAvgStd_ds = dataWindow_ds\
    #       .map(getSquared)\
    #       .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))\
    #       .map(getAvgStd)
    
    # join the original Dstream with individual record and the aggregated Dstream with window-avg and window-std 
    #joined_ds = dataWindow_ds.join(dataWindowAvgStd_ds)

    # label each record 'safe' or 'danger' by comparing the data with the window-avg and window-std    
    #result_ds = joined_ds.map(labelAnomaly)
    #resultSimple_ds = result_ds.map(lambda x: (x[0], x[1], x[5]))

    # Send the status table to rethinkDB and all data to cassandra    
    line.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandra))
    #resultSimple_ds.foreachRDD(sendRethink)
    
    ssc.start()
    ssc.awaitTermination()
    return


if __name__ == '__main__':
    main()



