#! /bin/bash

gradle build

pack_name=output_q2_$(date +%s)

$HADOOP_HOME/bin/hadoop jar build/libs/Q2-1.0-SNAPSHOT.jar Question2 /parsed.csv /"$pack_name"

$HADOOP_HOME/bin/hadoop fs -get /"$pack_name" ./"$pack_name"