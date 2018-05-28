############################################################
# This python script is a producer for kafka. 
############################################################


import random
import sys
import datetime
import numpy as np
from kafka import KafkaProducer
import time


def main():
    NumPlayers = 1000001
    InputFile = '/home/ubuntu/GamerStream/Project/simstream/4-SortedKillStream' + str(NumPlayers) + '.txt'
#    InputFile = '/home/ubuntu/GamerStream/Project/simstream/5-ShortenedTestStream.txt'
    KafkaProducerServers = "localhost:9092"

    producer = KafkaProducer(bootstrap_servers = KafkaProducerServers)

    #Variables to control kafka producer speed    
    batchSize = 100
    waitTime = 1 
    count = 0

    prevTime =0 
    with open(InputFile, 'r') as KillStream:
        for line in KillStream:
            currentTime = int(line.split(',')[0][1:])
            if currentTime == prevTime:
                producer.send('data', line.encode('utf-8'))
                #print (line)
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

