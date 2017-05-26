#!/bin/bash
echo Run Hadoop

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
echo $HADOOP_HOME_DIST
${HADOOP_HOME_DIST}/bin/hadoop fs -rmr /output
${HADOOP_HOME_DIST}/bin/hadoop fs -rmr /input
${HADOOP_HOME_DIST}/bin/hadoop fs -put input /input
#${HADOOP_HOME_DIST}/bin/hadoop fs -cat /input/* | more

${HADOOP_HOME_DIST}/bin/hadoop jar target/redditanalytics-1.0-SNAPSHOT.jar heigvd.bda.labs.redditanalytics.RedditAnalytics $1 /input /output

rm -r output
${HADOOP_HOME_DIST}/bin/hadoop fs -get /output output

cat output/* | more
