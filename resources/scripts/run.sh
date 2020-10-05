#!/bin/bash
# business-data/mongo/adb/2020-08-11/accepted-data.checkAutoCalcPendingData.0001.json.encryption.json

export HADOOP_CLASSPATH=$(find /usr/lib/hbase/ -type f -name '*.jar' \! -name 'slf4j-log4j12*' | xargs | tr ' ' ':'):/etc/hbase/conf:./historic-data-loader-1.0-SNAPSHOT-all.jar
export HBASE_TABLE='calculator:calculationParts'
export MAP_REDUCE_OUTPUT_DIRECTORY=/user/hadoop/import/$(uuidgen)
export S3_BUCKET=danc-nifi-stub
export S3_PREFIX=business-data/mongo/adb/2020-08-11/accepted-data.checkAutoCalcPendingData
export TOPIC_NAME=accepted-data.checkAutoCalcPendingData

hadoop jar ./historic-data-loader-1.0-SNAPSHOT-all.jar 1 2 3 4 5
