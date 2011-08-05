package com.freshbourne.hdfs.index;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;

public class IndexedRecordReader extends
		LineRecordReader {
	private static final Log LOG = LogFactory.getLog(IndexedRecordReader.class);

	private ThreadShared shared;
	private RecordReaderIndexExtension indexExtension;
	private boolean doneReadingFromIndex = false;

	public void initialize(InputSplit genericSplit,
						   TaskAttemptContext context) throws IOException {
		this.initialize(genericSplit, context);
		indexExtension = new RecordReaderIndexExtension(genericSplit, context);

		// adjust position from where we start reading, based on the value in the index
		// ...
	}

	public boolean nextKeyValue() throws IOException {
		// get next value from index as long as we have
          if(!doneReadingFromIndex){
               LOG.debug("READING FROM INDEX");
               String next = indexExtension.getNextFromIndex();
               if (next != null) {
                    value.set(next);
                    return true;
               }
          }

		pos = indexExtension.getPos();
		boolean result = super.nextKeyValue();
		// this.getCurrentKey();
		// this.getCurrentValue();
		return result;
	}
}
