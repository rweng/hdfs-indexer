#!/usr/bin/env bash

SAMPLE_FILE="build/sampleFile"
SAMPLE_FILE_HDFS="/test/sampleFile"

scripts/setupHdfs.sh

hadoop jar build/libs/hdfs-indexer-0.01.jar "${SAMPLE_FILE_HDFS}Small"

# make sure indexer was successfull
if [ $? -ne 0 ];then
	echo "hdfs-indexer failed!"
	exit
fi

# output the results
hadoop fs -cat /csv_output/*



