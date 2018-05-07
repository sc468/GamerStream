###########################################################
# This is the bash script to submit streaming.py to spark #
###########################################################
#Clear old Cassandra table
python3 /home/ubuntu/GamerStream/Project/cassandra/createTable.py 

#Run Spark, Kafka, Cassandra Interfaces
/usr/local/spark/bin/spark-submit --master spark://ec2-54-214-213-178.us-west-2.compute.amazonaws.com:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 ~/GamerStream/Project/spark/streaming.py 

