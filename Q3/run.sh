#! /bin/bash

clear

gradle build

pack_name=output_q3_$(date +%s)

$HADOOP_HOME/bin/hadoop jar build/libs/Q3-1.0-SNAPSHOT.jar Question3 /hw3_input /"$pack_name"

$HADOOP_HOME/bin/hadoop fs -get /"$pack_name" ./"$pack_name"