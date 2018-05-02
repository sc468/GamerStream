###########################################################
# This is the bash script to submit streaming.py to spark #
###########################################################

/usr/local/spark/bin/spark-submit --master spark://<master-hostname>:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 --py-files config.py streaming.py


