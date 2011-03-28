package com.freshbourne.hdfs.index.run;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.freshbourne.hdfs.index.IndexedInputFormat;

/**
 * @author Robin Wenglewski <robin@wenglewski.de>
 */
public class Main extends Configured implements Tool {

	public static class Map extends	Mapper<LongWritable, ArrayList<String>, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, ArrayList<String> value, Context context)
				throws IOException, InterruptedException {
			if(value.size() < 2)
				return;
			String line = value.get(0);
			
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				one.set(Integer.parseInt(value.get(1)));
				context.write(word, one);
			}
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

	private void printUsage() {
		System.out.println("Usage : .jar <input_file>");
	}

	private int runJob(String name, Class<? extends Map> map, Class<? extends Reduce> reduce,
			String input, String output) throws Exception {

		// configuration
		Configuration conf = getConf();
		conf.setClass("Index", Col1Index.class, Serializable.class);
		conf.set("indexSavePath", "/tmp/c1Index");
		setConf(conf);
		
		Job job = new Job(conf, name);
		job.setJarByClass(Main.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		
		job.setMapperClass(map);
		job.setReducerClass(reduce);
		
		job.setInputFormatClass(IndexedInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length < 1) {
			printUsage();
			return 1;
		}

		String input = args[0];
		return runJob("CSV", Map.class, Reduce.class, input,"/csv_output");
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Main(), args);
		System.exit(ret);
	}
}
