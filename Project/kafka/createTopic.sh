#######################################################
# This is the script to create a topic in Kafka       #
#######################################################


/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 2 --topic data

