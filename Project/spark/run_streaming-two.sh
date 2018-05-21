###########################################################
# This is the bash script to submit streaming-two.py to spark #
###########################################################
#Clear old Cassandra table
#python3 /home/ubuntu/GamerStream/Project/cassandra/createTable.py 

#Run Spark, Kafka, Cassandra Interfaces
/usr/local/spark/bin/spark-submit --master spark://ec2-54-203-27-201.us-west-2.compute.amazonaws.com:7077 \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 \
--num-executors 4 \
--executor-cores 3 \
--executor-memory 2G \
--conf spark.cassandra.output.batch.size.bytes=100M \
--conf spark.cassandra.output.throughput_mb_per_sec=100 \
~/GamerStream/Project/spark/streaming-two.py 

#--conf spark.cleaner.ttl=30000000000 \
#--conf spark.streaming.unpersist=false \
#--conf spark.default.parallelism=2 \



