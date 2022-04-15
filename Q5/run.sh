#! /bin/bash
OUTPUT_PATH=/hw3_q5_output_pt1
FINAL_OUTPUT=/hw3_q5_output_final

gradle build

$HADOOP_HOME/bin/hadoop fs -test -d $OUTPUT_PATH
if [ $? == 0 ];then
    echo "deleting $OUTPUT_PATH before running job."
    $HADOOP_HOME/bin/hadoop fs -rm -r $OUTPUT_PATH
fi

$HADOOP_HOME/bin/hadoop fs -test -d $FINAL_OUTPUT
if [ $? == 0 ];then
    echo "deleting $FINAL_OUTPUT before running job."
    $HADOOP_HOME/bin/hadoop fs -rm -r $FINAL_OUTPUT
fi

$HADOOP_HOME/bin/hadoop jar build/libs/Q5-1.0-SNAPSHOT.jar Question5 /hw3_input $OUTPUT_PATH $FINAL_OUTPUT

$HADOOP_HOME/bin/hadoop fs -cat $FINAL_OUTPUT/* | head -100