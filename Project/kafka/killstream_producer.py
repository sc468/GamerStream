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
    
    producer = KafkaProducer(bootstrap_servers = "localhost:9092")

    #Variables to control kafka producer speed    
    batchSize = 100
    waitTime = 1 
    count = 0

    with open('KillStream.txt', 'r') as KillStream:
        for line in KillStream:
            #print ('Printing line:', line)
            count += 1
            producer.send('data', line.encode('utf-8'))
            if (count%batchSize ==0):
                time.sleep(waitTime)
    
    # block until all async messages are sent
    producer.flush()
    
    # configure multiple retries
    producer = KafkaProducer(retries=5)

    return


if __name__ == '__main__':
    main()

