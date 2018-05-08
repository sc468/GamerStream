############################################################
# This python script is a producer for kafka. It creates 
# random user data and send to kafka. The data is in JSON
# format. Here is the schema:
#
# {"userid": text, 
#  "time": timestamp, 
#  "acc": float}
# The "acc" column is the acceleration of the user.

# To send it to kafka, each record is first converted to 
# string then to bytes using str.encode('utf-8') method.
#
# The parameters
# config.KAFKA_SERVERS: public DNS and port of the servers
# config.ANOMALY_PERIOD: How often to create an outlier  
# config.ANOMALY_VALUE: The value to add to create an outlier
# were written in a separate "config.py".
############################################################


import random
import sys
import datetime
import numpy as np
from kafka import KafkaProducer
import time


def main():
    NumPlayers = 1000000
    InputFile = '/home/ubuntu/GamerStream/Project/simstream/4-SortedKillStream' + str(NumPlayers) + '.txt'
    KafkaProducerServers = "localhost:9092"

    producer = KafkaProducer(bootstrap_servers = KafkaProducerServers)

    #Variables to control kafka producer speed    
    batchSize = 100
    waitTime = 1 
    count = 0

    prevTime =0 
#    with open('SortedKillTimes.txt', 'r') as KillStream:
    with open(InputFile, 'r') as KillStream:
        for line in KillStream:
            #print ('Printing line:', line)
            currentTime = int(line.split(',')[0][1:])
            if currentTime == prevTime:
                producer.send('data', line.encode('utf-8'))
                print (line)
            else:
                prevTime = currentTime
                time.sleep(waitTime)
                producer.send('data', line.encode('utf-8'))
    
    # block until all async messages are sent
    producer.flush()
    
    # configure multiple retries
    producer = KafkaProducer(retries=5)

    return


if __name__ == '__main__':
    main()

