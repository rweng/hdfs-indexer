package com.freshbourne.hdfs.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * this class is only used to create indexes. Maybe we should make the Index interface an
 * abstract class and put this method in there?
 */
public class IndexBuilder {
    public static Index create(InputSplit genericSplit, TaskAttemptContext context) {
        Configuration conf = context.getConfiguration();
        Class<?> indexClass = conf.getClass("Index", null);
        try {
            return (Index) indexClass.getConstructor().newInstance();
        } catch (Exception e) {
            return null;
        }
    }

}
