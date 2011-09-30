/**
 * Copyright (C) 2011 Robin Wenglewski <robin@wenglewski.de>
 *
 * This work is licensed under a Creative Commons Attribution-NonCommercial 3.0 Unported License:
 * http://creativecommons.org/licenses/by-nc/3.0/
 * For alternative conditions contact the author. 
 */
package com.freshbourne.hdfs.index.run;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.freshbourne.hdfs.index.IndexedInputFormat;

public class PerformanceMain extends Configured implements Tool {

	public static class Map extends	Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private static final Log LOG = LogFactory.getLog(Map.class);

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			LOG.debug("Line: " + line);
			String[] splits = line.split("(\t| +)");
			one.set(1);
			if(splits.length >= 3)
				context.write(new Text(splits[2]), one);
		}
	}

	public static class Reduce extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	private boolean useIndex;
	
	private void printUsage() {
		System.out.println("Usage : .jar <input_file>");
	}

	private int runJob(String name, Class<? extends Map> map, Class<? extends Reduce> reduce,
			String input, String output) throws Exception {
		// configuration
		Configuration conf = getConf();
		conf.setClass("Filter", Filter.class, Serializable.class);
		conf.set("indexSavePath", "/tmp/index/");
		setConf(conf);
		
		Job job = new Job(conf, name);
		job.setJarByClass(PerformanceMain.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		
		job.setMapperClass(map);
		job.setReducerClass(reduce);
		
		job.setInputFormatClass(useIndex ? IndexedInputFormat.class : TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length < 3) {
			printUsage();
			return 2;
		}

		String input = args[0];
		useIndex = Boolean.parseBoolean(args[1]);
		return runJob("CSV", Map.class, Reduce.class, input,"/csv_output");
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new PerformanceMain(), args);
		System.exit(ret);
	}
}
