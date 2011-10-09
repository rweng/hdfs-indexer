package com.freshbourne.hdfs.index;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.Serializable;

public interface IndexBuilder extends Serializable {
	Index create(InputSplit genericSplit, TaskAttemptContext context);
}
