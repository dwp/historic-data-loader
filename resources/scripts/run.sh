#!/bin/bash
# business-data/mongo/adb/2020-08-11/accepted-data.checkAutoCalcPendingData.0001.json.encryption.json
export S3_BUCKET=${1:?Usage $0: bucket prefix}
export S3_PREFIX=${2:-business-data/mongo/danielchicot_generate_historic_data_dev_100/}

export HADOOP_CLASSPATH=$(find /usr/lib/hbase/ -type f -name '*.jar' \! -name 'slf4j-log4j12*' | xargs | tr ' ' ':'):/etc/hbase/conf:./historic-data-loader-1.0-SNAPSHOT-all.jar
export HBASE_TABLE=automatedtests:danielchicot_generate_historic_data_dev_100_1
export MAP_REDUCE_OUTPUT_DIRECTORY=/user/hadoop/import/$(uuidgen)
export TOPIC_NAME=automatedtests.danielchicot_generate_historic_data_dev_100_1

hadoop jar ./historic-data-loader-1.0-SNAPSHOT-all.jar
