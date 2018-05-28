#!/bin/sh

#GOAL: Take historical match data and extract stream of kille events
#A random time offset is added to each player. This is to simualte players starting at different times.

NUMPLAYERS=2000001
echo $NUMPLAYERS

python3 1-CleanPlayerData.py $NUMPLAYERS
python3 2-ExtractKillStream.py $NUMPLAYERS
python3 2-5-UploadToS3.py $NUMPLAYERS
/usr/local/spark/bin/spark-submit --master spark://ec2-54-214-213-178.us-west-2.compute.amazonaws.com:7077 ~/GamerStream/Project/simstream/3-BatchSort.py $NUMPLAYERS
