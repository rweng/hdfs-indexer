package thesis;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.umd.cloud9.io.ArrayListWritableComparable;

/**
 * @author Robin Wenglewski <robin@wenglewski.de>
 */
public class CSV extends Configured implements Tool {

	public static class Map extends	Mapper<LongWritable, ArrayListWritableComparable<Text>, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, ArrayListWritableComparable<Text> value, Context context)
				throws IOException, InterruptedException {
			String line = value.get(0).toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
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

	private int runJob(String name, Class map, Class reduce,
			String input, String output) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, name);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(map);
		job.setReducerClass(reduce);
		
		
		
		class SelectOver25 extends Index implements Selectable{
			
			protected int COLUMN = 1;
			
			@Override
			public boolean select(String[] o) {
				if(o.length >= 2){
					return Integer.parseInt(o[COLUMN]) > 25 ? true : false;
				}
				
				return false;
			}
			
		}

		SelectOver25 s = new SelectOver25();
		CSVRecordReader.setDelimiter(" ");
		CSVRecordReader.setPredicate(s);
		CSVRecordReader.setIndex(s);
		
		job.setInputFormatClass(CSVFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public int run(String[] args) throws Exception {

		if (args.length < 1) {
			printUsage();
			return 1;
		}

		String input = args[0];
		return runJob("CSV", Map.class, Reduce.class, input,"/csv_output");
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new CSV(), args);
		System.exit(ret);
	}
}
