###########################################################
# This is the bash script to submit streaming-two.py to spark #
###########################################################
#Clear old Cassandra table
#python3 /home/ubuntu/GamerStream/Project/cassandra/createTable.py 

#Run Spark, Kafka, Cassandra Interfaces
/usr/local/spark/bin/spark-submit --master spark://ec2-52-13-193-15.us-west-2.compute.amazonaws.com:7077 \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 \
--num-executors 4 \
--executor-cores 3 \
--executor-memory 2G \
--conf spark.default.parallelism=2 \
--conf spark.cleaner.ttl=30000000000 \
--conf spark.streaming.unpersist=false \
~/GamerStream/Project/spark/streaming-two.py 

#--conf spark.executor.cores=1 \
#--conf spark.executor.memory=6G \
#--conf  spark.executor.instances=1 \



#Run Spark, Kafka, Cassandra Interfaces
#/usr/local/spark/bin/spark-submit --master spark://ec2-54-214-158-7.us-west-2.compute.amazonaws.com:7077 \
#--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 \
#--conf spark.default.parallelism=2 \
#~/GamerStream/Project/spark/streaming.py 





#/usr/local/spark/bin/spark-submit --master spark://ec2-54-214-213-178.us-west-2.compute.amazonaws.com:7077 \
#--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 \
#~/GamerStream/Project/spark/streaming.py 


#/usr/local/spark/bin/spark-submit --master spark://ec2-54-214-213-178.us-west-2.compute.amazonaws.com:7077 \
#--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 \
#--conf spark.executor.cores=1 \
#--conf spark.executor.memory=6G \
#--conf  spark.executor.instances=1 \
#~/GamerStream/Project/spark/streaming.py 


#/usr/local/spark/bin/spark-submit --master spark://ec2-54-214-213-178.us-west-2.compute.amazonaws.com:7077 \
#--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 \
#--num-executors 1 --executor-cores 1 --executor-memory 6G \
#~/GamerStream/Project/spark/streaming.py 
