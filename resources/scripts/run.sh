#!/bin/bash

export S3_BUCKET=${1:?Usage $0: bucket prefix}
export S3_PREFIX=${2:-business-data/mongo/danielchicot_generate_historic_data_dev_100/}

export HADOOP_CLASSPATH=$(find /usr/lib/hbase/ -type f -name '*.jar' \! -name 'slf4j-log4j12*' | xargs | tr ' ' ':'):/etc/hbase/conf:./historic-data-loader-1.0-SNAPSHOT-all.jar
export HBASE_TABLE=automatedtests:danielchicot_generate_historic_data_dev_100_1
export MAP_REDUCE_OUTPUT_DIRECTORY=/user/hadoop/import/$(uuidgen)
export TOPIC_NAME=automatedtests.danielchicot_generate_historic_data_dev_100_1
aws s3 cp s3://danc-nifi-stub/historic-data-loader-all.jar .
hadoop jar ./historic-data-loader-all.jar
