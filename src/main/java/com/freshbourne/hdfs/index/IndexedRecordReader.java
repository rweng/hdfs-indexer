package com.freshbourne.hdfs.index;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.LinkedList;

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
import org.apache.hadoop.util.LineReader;

import com.freshbourne.hdfs.index.IndexedRecordReader.Shared;

public class IndexedRecordReader extends
		RecordReader<LongWritable, ArrayList<String>> {
	private static final Log LOG = LogFactory.getLog(IndexedRecordReader.class);

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;
	private LongWritable key = null;
	private ArrayList<String> value = null;
	private Text tmpInputLine = new Text();
	private static Select selectable;
	private static String delimiter = " \t";
	private static Index<String, String> index;
	private String[] splits;
	private Configuration conf;

	private Shared shared;

	public static void setDelimiter(String d) {
		delimiter = d;
	}

	public static void setIndex(Index<String, String> i) {
		index = i;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {

		// lets assume its a file split

		LOG.info("in RecordReader.initialize()");

		FileSplit split = (FileSplit) inputSplit;
		Configuration job = context.getConfiguration();
		this.maxLineLength = job.getInt("mapred.csvrecordreader.maxlinelength",
				Integer.MAX_VALUE);
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();

		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		boolean skipFirstLine = false;
		if (codec != null) {
			in = new LineReader(codec.createInputStream(fileIn), job);
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				skipFirstLine = true;
				--start;
				fileIn.seek(start);
			}
			in = new LineReader(fileIn, job);
		}
		if (skipFirstLine) { // skip first line and re-establish "start".
			start += in.readLine(new Text(), 0,
					(int) Math.min(Integer.MAX_VALUE, end - start));
		}
		this.pos = start;

		conf = context.getConfiguration();

		Class<?> c = conf.getClass("Index", null);
		if (c == null)
			throw new IllegalArgumentException(
					"Index class must be set in config");

		try {

			index = (Index<String, String>) (c.getConstructor().newInstance());

			// try to load the index
			String savePath = generateIndexPath(conf.get("indexSavePath"),
					inputSplit, index);
			index.load(savePath);

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		shared = new Shared(index);
		IndexWriterThread thread = new IndexWriterThread(shared);
		// thread.run();
		LOG.info("Index set!");

		LOG.debug("delimiter: " + delimiter);
	}

	/**
	 * @param string
	 * @param inputSplit
	 * @return
	 */
	private String generateIndexPath(String folder, InputSplit inputSplit,
			Index index) {
		FileSplit split;
		try {
			split = (FileSplit) inputSplit;
		} catch (Exception e) {
			throw new IllegalArgumentException(
					"InputSplit must be an instance of FileSplit");
		}

		File folderFile = (new File(folder));
		if (!(folderFile.isDirectory() || !folderFile.exists()))
			throw new IllegalArgumentException("savePath must be a folder: "
					+ folderFile.getAbsolutePath());

		if (!split.getPath().toString().startsWith("hdfs://")) {
			throw new IllegalArgumentException(
					"The File for the Index must be in the hdfs");
		}

		String path = split.getPath().toString()
				.replaceFirst("hdfs://[^\\/]+", "");
		LOG.debug("path: " + path);
		String path2 = folderFile.getAbsolutePath() + path;
		LOG.debug("path2: " + path2);
		String path3 = path2 + "_" + index.getIdentifier() + "_"
				+ split.getStart();
		LOG.debug("FILE NAME: " + path3);

		(new File(path3).getParentFile()).mkdirs();

		// TODO Auto-generated method stub
		return path3;
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		LOG.info("in Recorder.nextKeyValue()");
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new ArrayList<String>();
		}

		int newSize = 0;
		value.clear();
		boolean fromIndex = false;

		//TODO: how to know where the index ended
		
		// Iterator<String> iterator = index.getIterator("2006-03-17",
		// "2006-03-17");

		// iterator.hasNext()

		// if(iterator != null){
		// if(iterator.hasNext()){
		// pos = iterator.next().getValue();
		// fromIndex = true;
		// LOG.info("Using index for pos" + pos);
		// } else {
		// pos = iterator.getHighestOffset(); // this one is read double
		// iterator = null;
		// }
		// }
		//
		// we almost always break from this loop, it is only for making sure
		// that we are in maxLineLength
		
		while (pos < end) {

			newSize = in.readLine(tmpInputLine, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
							maxLineLength));

			LOG.info("READING LINE: " + tmpInputLine);

			this.splits = tmpInputLine.toString().split(delimiter);
			LOG.info("Splitsize: " + splits.length);

			pos += newSize;
			if (newSize == 0 || newSize < maxLineLength) {
				break;
			}

			// line too long. try again
			LOG.info("Skipped line of size " + newSize + " at pos "
					+ (pos - newSize));
		}

		// return false if we didnt read anything, end of input
		if (newSize == 0) {
			String sp = conf.get("indexSavePath");
			if (index != null && sp != null)
				index.save(null);

			key = null;
			value = null;
			return false;
		}

		// put it in the Index, if already there it just returns
		if (index != null) {
			index.add(splits, tmpInputLine.toString());
		}

		if (this.splits == null)
			LOG.info("splits are null");

		// if the predicate is matched, return, otherwise return nextKeyValue();
		if (fromIndex || selectable == null || selectable.select(this.splits)) {
			for (String s : this.splits) {
				LOG.info("adding to arraylist: " + s);
				value.add(s);
			}
			return true;
		} else {
			return nextKeyValue();
		}

	}

	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	@Override
	public ArrayList<String> getCurrentValue() {
		return value;
	}

	/**
	 * Get the progress within the split
	 */
	@Override
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public synchronized void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}
	
	class Shared {
		private Index index;
		private LinkedList<String[]> splitsList = new LinkedList<String[]>();
		private LinkedList<String> valueList = new LinkedList<String>();
		
		public void add(String[] splits, String string) {
			if(getSplitsList().size() > 1000)
				return;
			
			getSplitsList().add(splits);
			getValueList().add(string);
		}
		
		public void save(){};

		Shared(Index index){
			this.setIndex(index);
		}

		/**
		 * @param index the index to set
		 */
		public void setIndex(Index index) {
			this.index = index;
		}

		/**
		 * @return the index
		 */
		public Index getIndex() {
			return index;
		}

		/**
		 * @param splitsList the splitsList to set
		 */
		public void setSplitsList(LinkedList<String[]> splitsList) {
			this.splitsList = splitsList;
		}

		/**
		 * @return the splitsList
		 */
		public LinkedList<String[]> getSplitsList() {
			return splitsList;
		}

		/**
		 * @param valueList the valueList to set
		 */
		public void setValueList(LinkedList<String> valueList) {
			this.valueList = valueList;
		}

		/**
		 * @return the valueList
		 */
		public LinkedList<String> getValueList() {
			return valueList;
		}
	}
}
