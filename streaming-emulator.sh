#!/bin/bash

SRC="/user/ubuntu/split-dataset"
DEST="/user/ubuntu/stream-dataset"

#for a in `seq 1 1127` ;
#for a in `seq 900 990` ;
#do
#hdfs dfs -mv ${DEST}/${a}.csv ${SRC}/${a}.csv
#done
~      

#hdfs dfs -rm ${DEST}/*


echo "Start" 
for a in `seq 1 1197` ;
do
hdfs dfs -mv ${SRC}/${a}.csv ${DEST}/${a}.csv
sleep 5
done
