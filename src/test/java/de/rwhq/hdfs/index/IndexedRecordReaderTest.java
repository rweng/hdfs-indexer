package de.rwhq.hdfs.index;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import de.rwhq.btree.Range;
import de.rwhq.comparator.IntegerComparator;
import de.rwhq.hdfs.index.extractor.IntegerCSVExtractor;
import de.rwhq.serializer.IntegerSerializer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public abstract class IndexedRecordReaderTest {
	private static final String TEST_ROOT_DIR = "/tmp/IndexedRecordReaderTest";
	private static final Log    LOG           = LogFactory.getLog(IndexedRecordReaderTest.class);

	private static FileSystem localFs;
	public static final Path   OUTPUT          = new Path(TEST_ROOT_DIR, "out");
	public static final Path   INPUT           = new Path(TEST_ROOT_DIR, "in");
	public static final File   INDEX           = new File(TEST_ROOT_DIR.toString() + "/index");
	public static final String INPUT_FILE_PATH = INPUT + "/testfile.csv";
	private ArrayList<String> lines;


	@BeforeClass
	public static void setUpClass() {
		try {
			localFs = FileSystem.getLocal(new Configuration());
		} catch (IOException io) {
			throw new RuntimeException("problem getting local fs", io);
		}

		// make sure the log folder exists,
		// otherwise the test fill fail
		new File(TEST_ROOT_DIR.toString() + "/test-logs").mkdirs();
		System.setProperty("hadoop.log.dir", TEST_ROOT_DIR.toString() + "/test-logs");

	}

	public static Path writeFile(String name, String data) throws IOException {
		Path file = new Path(name);
		localFs.delete(file, false);
		DataOutputStream f = localFs.create(file);
		f.write(data.getBytes());
		f.close();
		return file;
	}

	public static String readFile(String name) throws IOException {
		DataInputStream f = localFs.open(new Path(name));
		BufferedReader b = new BufferedReader(new InputStreamReader(f));
		StringBuilder result = new StringBuilder();
		String line = b.readLine();
		while (line != null) {
			result.append(line);
			result.append('\n');
			line = b.readLine();
		}
		b.close();
		return result.toString();
	}

	private MiniMRCluster cluster;

	@Before
	public void setUp() throws IOException {
		FileUtils.deleteDirectory(new File(TEST_ROOT_DIR.toString()));

		new File(INDEX.toString()).mkdirs();
		new File(INPUT.toString()).mkdirs();

		cluster = new MiniMRCluster(2, "file:///", 3);
		SpyBuilder.instances.clear();
		resetLines();
	}

	private void resetLines() {
		lines = Lists.newArrayList();
		for (int i = 0; i < 5; i++)
			lines.add("0,A,25");

		for (int i = 0; i < 5; i++)
			lines.add("1,B,25");


		for (int i = 0; i < 5; i++)
			lines.add("2,C,25");

		for (int i = 0; i < 7; i++)
			lines.add("3,D,25");

		for (int i = 0; i < 5; i++)
			lines.add("4,F,25");

		for (int i = 0; i < 5; i++)
			lines.add("5,G,25");

		for (int i = 0; i < 5; i++)
			lines.add("6,H,25");
	}

	@After
	public void tearDown() throws IOException {
		cluster.shutdown();
		cluster = null;
	}


	@Test
	public void createIndex() throws IOException, ClassNotFoundException, InterruptedException {

		// prepare for test
		createTextInputFile();

		assertThat(createJob().waitForCompletion(false)).isTrue();

		String out = readFile(OUTPUT + "/part-r-00000");

		LOG.info(out);

		assertThat(out).isEqualTo("1\t5\n2\t5\n3\t7\n4\t5\n");
		assertThat(new File(INDEX.getAbsolutePath() + INPUT_FILE_PATH)).isDirectory();
		assertThat(new File(INDEX.getAbsolutePath() + INPUT_FILE_PATH + "/properties")).isFile();
		assertThat(new File(INDEX.getAbsolutePath() + INPUT_FILE_PATH).list().length).isGreaterThan(1);
		assertThat(SpyBuilder.instances).hasSize(1);
		verify(SpyBuilder.instances.get(0), atLeastOnce()).addLine(anyString(), anyLong(), anyLong());
	}

	@Test
	public void useIndex() throws ClassNotFoundException, IOException, InterruptedException {
		createIndex();
		SpyBuilder.instances.clear();

		FileUtils.deleteDirectory(new File(OUTPUT.toString()));

		assertThat(createJob().waitForCompletion(false)).isTrue();

		String out = readFile(OUTPUT + "/part-r-00000");

		LOG.info(out);

		assertThat(out).isEqualTo("1\t5\n2\t5\n3\t7\n4\t5\n");
		assertThat(SpyBuilder.instances).hasSize(1);
		verify(SpyBuilder.instances.get(0), never()).addLine(anyString(), anyLong(), anyLong());
	}


	@Test
	public void partialIndex() throws ClassNotFoundException, IOException, InterruptedException {
		createIndex();

		SpyBuilder.instances.clear();
		FileUtils.deleteDirectory(new File(OUTPUT.toString()));

		for (int i = 0; i < 10; i++)
			lines.add("10,Z,10");

		createTextInputFile();


		assertThat(createJob().waitForCompletion(false)).isTrue();

		String out = readFile(OUTPUT + "/part-r-00000");

		LOG.info(out);

		assertThat(out).isEqualTo("1\t5\n10\t10\n2\t5\n3\t7\n4\t5\n");
		assertThat(SpyBuilder.instances).hasSize(1);
		verify(SpyBuilder.instances.get(0), atLeastOnce()).addLine(anyString(), anyLong(), anyLong());
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one  = new IntWritable(1);
		private              Text        word = new Text();


		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			LOG.info("map recieved: " + value);
			String oId = value.toString().split(",")[0];
			int orderId;
			try {
				orderId = Integer.parseInt(oId);
			} catch (Exception e) {
				LOG.warn("coundn't parse '" + oId + "', which is the fist part of line\n" + value, e);
				return;
			}

			word.set("" + orderId);
			context.write(word, one);
		}
	}

	private Job createJob() throws IOException {
		Configuration conf = new Configuration();
		conf.setClass("indexBuilder", getBuilderClass(), IndexBuilder.class);

		Job job = new Job(conf, "IndexedRecordReaderTest");

		job.setJarByClass(IndexedRecordReaderTest.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setReducerClass(IntSumReducer.class);
		job.setMapperClass(Map.class);

		job.setInputFormatClass(IndexedInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, INPUT);
		FileOutputFormat.setOutputPath(job, OUTPUT);

		return job;
	}

	public static abstract class SpyBuilder extends AbstractIndexBuilder {
		private static List<Index> instances = Lists.newArrayList();

		@Override
		public Index build() {
			Index instance = spy(super.build());
			instances.add(instance);
			return instance;
		}


		@Override
		public MFIBuilder configure(MFIBuilder bTreeIndexBuilder) {
			return configure2(bTreeIndexBuilder
					.indexFolder(IndexedRecordReaderTest.INDEX)
					.addDefaultRange(new Range(1, 4))
					.addDefaultRange(new Range(10, 10))
					.cacheSize(10)
					.keySerializer(IntegerSerializer.INSTANCE)
					.keyExtractor(new IntegerCSVExtractor(0, ","))
					.treePageSize(4 * 1024)
					.comparator(IntegerComparator.INSTANCE));
		}

		protected abstract MFIBuilder configure2(MFIBuilder b);
	}

	protected abstract Class<? extends SpyBuilder> getBuilderClass();


	private void createTextInputFile() throws IOException {
		OutputStream os = localFs.create(new Path(INPUT_FILE_PATH));
		Writer wr = new OutputStreamWriter(os);

		wr.write(Joiner.on("\n").join(lines));

		wr.close();
	}
}
