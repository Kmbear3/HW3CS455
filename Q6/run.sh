#! /bin/bash
FINAL_OUTPUT=/hw3_q6_output_final

gradle build


$HADOOP_HOME/bin/hadoop fs -test -d $FINAL_OUTPUT
if [ $? == 0 ];then
    echo "deleting $FINAL_OUTPUT before running job."
    $HADOOP_HOME/bin/hadoop fs -rm -r $FINAL_OUTPUT
fi

$HADOOP_HOME/bin/hadoop jar build/libs/Q6-1.0-SNAPSHOT.jar Question6 /hw3_input $FINAL_OUTPUT

$HADOOP_HOME/bin/hadoop fs -cat $FINAL_OUTPUT/* | head -100