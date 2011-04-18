#!/usr/bin/env bash

SAMPLE_FILE="build/sampleFile"
SAMPLE_FILE_HDFS="/test/sampleFile"

# make sure we are in the right directory for the relative paths in the rest of the script to work
if [ ! -f build.gradle ];then
	echo "please start the script from the root directory of the project"
	exit
fi

# download the sample file, if it doesn't exist already
if [ ! -f build/sampleFile ]; then
  curl http://dl.dropbox.com/u/456244/user-ct-test-collection-01.txt -o $SAMPLE_FILE
  head -100 $SAMPLE_FILE > "${SAMPLE_FILE}Small"
fi

# run the indexer 
hadoop fs -rmr /csv_output
rm -rf "/tmp${SAMPLE_FILE_HDFS}"
hadoop fs -put $SAMPLE_FILE "$SAMPLE_FILE_HDFS"
hadoop fs -put "${SAMPLE_FILE}Small" "${SAMPLE_FILE_HDFS}Small"
hadoop jar build/libs/hdfs-indexer-0.01.jar "${SAMPLE_FILE_HDFS}Small"

# make sure indexer was successfull
if [ $? -ne 0 ];then
	echo "hdfs-indexer failed!"
	exit
fi

# output the results
hadoop fs -cat /csv_output/*



