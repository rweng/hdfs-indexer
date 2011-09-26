#!/usr/bin/env bash

SAMPLE_FILE="build/sampleFile"
SAMPLE_FILE_HDFS="/test/sampleFile"

scripts/setupHdfs.sh

# used * instead of the concrete name to 1. be version independent and 2. so that also jars generated in the
# vagrant env work (they are generated as vagrant.*.jar)
hadoop jar build/libs/*.jar com.freshbourne.hdfs.index.run.Main "${SAMPLE_FILE_HDFS}Small"

# make sure indexer was successfull
if [ $? -ne 0 ];then
	echo "hdfs-indexer failed!"
	exit
fi

# output the results
hadoop fs -cat /csv_output/*



