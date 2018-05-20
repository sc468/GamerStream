###########################################################
# This is the bash script to submit streaming.py to spark #
###########################################################
#Clear old Cassandra table
python3 /home/ubuntu/GamerStream/Project/cassandra/createTable.py 

#Run Spark, Kafka, Cassandra Interfaces
/usr/local/spark/bin/spark-submit --master spark://ec2-54-218-203-72.us-west-2.compute.amazonaws.com:7077 \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 \
--num-executors 4 \
--executor-cores 3 \
--executor-memory 2G \
--conf spark.default.parallelism=2 \
~/GamerStream/Project/spark/streaming.py 

#--conf spark.scheduler.mode=FAIR \
