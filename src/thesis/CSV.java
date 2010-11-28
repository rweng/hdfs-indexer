package thesis;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Robin Wenglewski <robin@wenglewski.de>
 */
public class CSV extends Configured implements Tool {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	private void printUsage() {
		System.out.println("Usage : .jar <input_file>");
	}

	private RunningJob runJob(String name, Class map, Class reduce,
			String input, String output) throws Exception {
		JobConf job = new JobConf(CSV.class);
		job.setJobName(name);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(map);
		job.setReducerClass(reduce);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output));

		return JobClient.runJob(job);
	}

	public int run(String[] args) throws Exception {

		if (args.length < 1) {
			printUsage();
			return 1;
		}

		String input = args[0];

		RunningJob concatJob = runJob("CSV", Map.class, Reduce.class, input,
				"/csv_output");
		concatJob.waitForCompletion();

		return 0;

	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new CSV(), args);
		System.exit(ret);
	}
}
