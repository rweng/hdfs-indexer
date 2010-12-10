package com.freshbourne.hdfs.index.run;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import edu.umd.cloud9.io.ArrayListWritableComparable;



public class CSVFileInputFormat extends FileInputFormat<LongWritable, ArrayList<String>> {

	@Override
	public RecordReader<LongWritable, ArrayList<String>> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new CSVRecordReader();
	}
	
	@Override
	  protected boolean isSplitable(JobContext context, Path file) {
	    CompressionCodec codec = 
	      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
	    return codec == null;
	  }


}
