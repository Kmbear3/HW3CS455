#! /bin/bash

clear

gradle build

pack_name=output_q4_$(date +%s)

$HADOOP_HOME/bin/hadoop jar build/libs/Q4-1.0-SNAPSHOT.jar Question4 /hw3_input /"$pack_name"

$HADOOP_HOME/bin/hadoop fs -get /"$pack_name" ./"$pack_name"