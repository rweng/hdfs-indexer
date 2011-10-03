#!/usr/bin/env bash

SAMPLE_FILE="build/lineitem.tbl"
SAMPLE_FILE_HDFS="/test/lineitem.tbl"

# make sure we are in the right directory for the relative paths in the rest of the script to work
if [ ! -f build.gradle ];then
	echo "please start the script from the root directory of the project"
	exit
fi

# download the sample file, if it doesn't exist already
if [ ! -f build/sampleFile ]; then
  echo "sample file missing. Please generate and put it to: " + $SAMPLE_FILE
else
	hadoop fs -rmr /csv_output
	hadoop fs -put $SAMPLE_FILE "$SAMPLE_FILE_HDFS"
fi