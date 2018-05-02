# https://github.com/PatrickCallaghan/datastax-kafka-python/blob/master/Consumer.py
# https://kafka-python.readthedocs.io/en/v0.9.4/usage.html

from kafka import KafkaClient
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import config

cluster = Cluster(config.CASSANDRA_SERVER)
session = cluster.connect()
session.execute('USE ' + config.CASSANDRA_NAMESPACE)
kafka = KafkaClient("localhost:9092")
 
print("After connecting to kafka")

#Create Kafka consumer (topic, group, server)
consumer = KafkaConsumer('data',
                         group_id='my_group',
                         bootstrap_servers=['localhost:9092'])

for message in consumer:
    # message value is raw byte string -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                         message.offset, message.key,
                                         message.value))
