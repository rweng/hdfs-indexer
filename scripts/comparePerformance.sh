#!/usr/bin/env bash

SAMPLE_FILE="build/sampleFile"
SAMPLE_FILE_HDFS="/test/sampleFile"
PERF_SUMMARY="build/perf_summary.csv"

scripts/setupHdfs.sh
START=$(date +%s)
hadoop jar build/libs/hdfs-indexer-0.01.jar com.freshbourne.hdfs.index.run.PerformanceMain "${SAMPLE_FILE_HDFS}" false -Dmapred.child.java.opts=-Xmx1024M
END=$(date +%s)
DIFF_NO_INDEX=$(( $END - $START ))
echo "LARGE,false,0,${DIFF_NO_INDEX}" >> $PERF_SUMMARY


rm -rf /tmp/index
for i in {1..5}; do
	sleep 5
	scripts/setupHdfs.sh
	START=$(date +%s)
	hadoop jar build/libs/hdfs-indexer-0.01.jar com.freshbourne.hdfs.index.run.PerformanceMain "${SAMPLE_FILE_HDFS}" true -Dmapred.child.java.opts=-Xmx1024M
	END=$(date +%s)
	DIFF_INDEX=$(( $END - $START ))
	echo "LARGE,true,${i},${DIFF_INDEX}" >> $PERF_SUMMARY

	DIFF=$(( $DIFF_INDEX - $DIFF_NO_INDEX ))
	echo "INDEX was ${DIFF} seconds faster"
done


