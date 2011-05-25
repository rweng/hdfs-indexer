package com.freshbourne.hdfs.index;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

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

public class IndexedRecordReader extends
		RecordReader<LongWritable, Text> {
	private static final Log LOG = LogFactory.getLog(IndexedRecordReader.class);

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;
	private LongWritable key = null;
	private Text value = null;
	private Text tmpInputLine = new Text();
	private static String delimiter = "(\t| +)";
	private static Index<String, String> index;
	private String[] splits;
	private Configuration conf;
	private Properties properties;
	private String fileName;
	private String[] indexFiles;
	private short indexFilesPointer = 0;
	private Shared shared;
	private boolean doneReadingFromIndex = false;
	private String hdfsPath;
	
	private Iterator<String> indexIterator;

	private String indexFolder;

	private File propertiesFile;

	public static void setDelimiter(String d) {
		delimiter = d;
	}

	public static void setIndex(Index<String, String> i) {
		index = i;
	}

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

		// create the index
		conf = context.getConfiguration();
		hdfsPath = inputToFileSplit(inputSplit).getPath().toString();
		
		File dir = new File(generateIndexFolder(conf.get("indexSavePath")));
		dir.mkdirs();
		
		indexFolder = dir.getAbsolutePath() + "/";
		
		// fill index Files array
		if (dir.exists()) {
			LOG.debug("dir exists: " + dir.getAbsolutePath());
			FilenameFilter filter = new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					if (name.startsWith(fileName))
						return true;
					return false;
				}
			};
			
			// getting files in the right order
			String[] tmpIndexFiles = dir.list(filter);
			indexFiles = new String[tmpIndexFiles.length];
			HashMap<Integer, String> map = new HashMap<Integer, String>();
			for(String name : tmpIndexFiles){
				map.put(Integer.parseInt(name.split("_")[2]), name);
			}
			
			Integer[] keys = new Integer[map.keySet().size()];
			map.keySet().toArray(keys);
			java.util.Arrays.sort(keys);
			for(int i = 0;i<keys.length;i++)
				indexFiles[i] = map.get(keys[i]);
			
		} else {
			LOG.debug("dir doesn't exist: " + dir.getAbsolutePath());
			indexFiles = new String[0];
		}

		LOG.debug("indexFiles: ");
		for (String s : indexFiles)
			LOG.debug("indexFile: " + s);		
		LOG.debug("delimiter: " + delimiter);
		
		// load the properties
		properties = new Properties();
		propertiesFile = new File(indexFolder + "properties");
		if(propertiesFile.exists()){
			try{
				FileInputStream fis = new FileInputStream(propertiesFile);
				properties.loadFromXML(fis);	
			} catch (Exception e) {
				LOG.debug("deleting properties file");
				propertiesFile.delete();
			}
		}
		
		if(properties == null){
			throw new IllegalStateException("properties should not be null");
		}
		
		
	}
	
	private static FileSplit inputToFileSplit(InputSplit inputSplit){
		FileSplit split;
		try {
			split = (FileSplit) inputSplit;
		} catch (Exception e) {
			throw new IllegalArgumentException(
					"InputSplit must be an instance of FileSplit");
		}
		return split;
	}
	
	private String generateIndexFolder(String folder){
		String file = generateIndexPath(folder, "", "");
		int index = file.lastIndexOf('/');
		String result = file.substring(0, index);
		LOG.debug("generated index folder: " + result);
		return result;
	}

	/**
	 * @param string
	 * @param inputSplit
	 * @return
	 */
	private String generateIndexPath(String folder, String startPos, String columnIdentifier) {
		
		File folderFile = (new File(folder));
		if (!(folderFile.isDirectory() || !folderFile.exists()))
			throw new IllegalArgumentException("savePath must be a folder: "
					+ folderFile.getAbsolutePath());

		if (!hdfsPath.startsWith("hdfs://")) {
			throw new IllegalArgumentException(
					"The File for the Index must be in the hdfs");
		}

		String path = hdfsPath.replaceFirst("hdfs://[^\\/]+", "");
		String[] splits = path.split("\\/");
		fileName = splits[splits.length-1];
		LOG.debug("path: " + path);
		String path2 = folderFile.getAbsolutePath() + path;
		
		String path3 = path2 + "_" + columnIdentifier + "_"
				+ startPos;
		LOG.debug("FILE NAME: " + path3);
		(new File(path3).getParentFile()).mkdirs();

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
			value = new Text();
		}

		int newSize = 0;
		value.clear();
		
		// get next value from index as long as we have
		if(!doneReadingFromIndex){
			LOG.debug("READING FROM INDEX");
			String next = getNextFromIndex();
			if (next != null) {
				value.set(tmpInputLine);
				return true;
			}
		}
		
		// we are here so we cant read furth from index
		while (pos < end) {
			newSize = in.readLine(tmpInputLine, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
							maxLineLength));

			LOG.info("READING LINE FROM HDFS: " + tmpInputLine);

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
			
			LOG.debug("END OF INPUT");
			
			key = null;
			value = null;
			if(shared != null) {
				synchronized (shared) {
					shared.notifyAll();
				}
			}else {
				LOG.debug("shared is null!");
			}
			return false;
		}

		// put it in the Index, if already there it just returns
		// lets create a write thread if we need
		if(index == null){
			createIndexAndThread(pos);
		}
		if(shared != null){
			shared.add(splits, tmpInputLine.toString(), pos);
		} else {
			LOG.debug("SHARED == NULL");
		}
		
		if (this.splits == null)
			LOG.info("splits are null");

		value.set(tmpInputLine);
		
		return true;
	}

	
	private static boolean indexThreadCreationFailed = false;
	@SuppressWarnings("unchecked")
	private void createIndexAndThread(long pos) {
		LOG.debug("createIndexAndThread()");
		// make sure we dont try the same shit over and over again
		if(indexThreadCreationFailed){
			LOG.debug("return since index creation already failed");
			return;
		}
		// get the class from the configuration
		Class<?> c = conf.getClass("Index", null);
		if (c == null)
			throw new IllegalArgumentException(
					"Index class must be set in config");

		// lets try to create the index and the thread
		try {
			index = (Index<String, String>) (c.getConstructor().newInstance());
			index.createIndex(generateIndexPath(conf.get("indexSavePath"), ""+pos, "" + index.getIdentifier()));
			shared = new Shared(index, properties);
			(new IndexWriterThread(shared)).start();
			LOG.debug("--- index thread successfully created!");
			
		} catch (Exception e) {
			indexThreadCreationFailed = true;
			LOG.debug("--- index thread creation failed: " + e.getMessage());
		}
	}

	/**
	 * also sets pos
	 */
	@SuppressWarnings("unchecked")
	private String getNextFromIndex() {
		LOG.debug("getNextFromIndex()");
		if(doneReadingFromIndex)
			return null;
		
		LOG.debug("not done");
		
		// return next if index and iterator are loaded
		if(indexIterator != null && indexIterator.hasNext()){
			LOG.debug("returning next from index");
			return indexIterator.next();
		}
		
		LOG.debug("didnt return next");
		
		// return null if there is no more index
		if(indexFilesPointer >= indexFiles.length){
			doneReadingFromIndex = true;
			index = null;
			indexIterator = null;
			return null;
		}
		
		LOG.debug("didnt return null, loading index");
		
		
		// otherwise load index
		Class<?> c = conf.getClass("Index", null);
		if (c == null)
			throw new IllegalArgumentException(
					"Index class must be set in config");

		try {

			index = (Index<String, String>) (c.getConstructor().newInstance());

			// try to load the index
			indexFilesPointer++;
			LOG.debug("loading offset");
			if(!properties.containsKey(indexFiles[indexFilesPointer - 1])){
				File f = new File(indexFolder + indexFiles[indexFilesPointer - 1]);
				LOG.debug("deleting file: " + f.getAbsolutePath());
				f.delete();
				index = null;
				indexIterator = null;
				LOG.debug("no data of this index partial in the properties file: " + indexFiles[indexFilesPointer - 1]);
				throw new IllegalStateException("no data of this index partial in the properties file");
			}
			
			String offset = properties.getProperty(indexFiles[indexFilesPointer - 1]);
			
			LOG.debug("Loading index " + indexFolder + indexFiles[indexFilesPointer - 1]);
			index.createIndex(indexFolder + indexFiles[indexFilesPointer - 1]);
			indexIterator = index.getIterator();
			
			LOG.debug("Adjusting POS: from " + pos + " to " + offset);
			pos = Long.parseLong(offset);
		} catch (Exception e) {
			LOG.debug("exception in loading index: " + e.getMessage());
			index = null;
			indexIterator = null;
		}

		return getNextFromIndex();
	}

	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	@Override
	public Text getCurrentValue() {
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
		private Index<String, String> index;
		private Properties properties;
		
		private LinkedBlockingQueue<String[]> splitsList = new LinkedBlockingQueue<String[]>();
		private LinkedBlockingQueue<String> valueList = new LinkedBlockingQueue<String>();
		private long offset = 0;
		
		private int counter = 0;
		
		public void add(String[] splits, String string, long offset) {
			if(counter++ >= 50)
				return;
			
			LOG.debug("shared adding with offset: " + offset);
			
			splitsList.add(splits);
			valueList.add(string);
			this.offset = offset;
		}
		
		public void save(){
			LOG.debug("saving index");
			index.save();
			LOG.debug("saving properties");
			String[] indexPathSplit = index.getPath().split("/"); 
			String indexPath = indexPathSplit[indexPathSplit.length - 1 ];
			properties.setProperty(indexPath, "" + offset);
			try {
				properties.storeToXML(new FileOutputStream(propertiesFile), "comment");
			} catch (Exception e) {
				LOG.debug("Storing properties failed: " + e.toString());
				e.printStackTrace();
			}
			LOG.debug("properties saved");
		};

		Shared(Index<String, String> index, Properties p){
			this.setIndex(index);
			this.properties = p;
		}

		/**
		 * @param index the index to set
		 */
		public void setIndex(Index<String, String> index) {
			this.index = index;
		}

		/**
		 * @return the index
		 */
		public Index<String, String> getIndex() {
			return index;
		}

		/**
		 * @param splitsList the splitsList to set
		 */
		public void setSplitsList(LinkedBlockingQueue<String[]> splitsList) {
			this.splitsList = splitsList;
		}

		/**
		 * @return the splitsList
		 */
		public LinkedBlockingQueue<String[]> getSplitsList() {
			return splitsList;
		}

		/**
		 * @param valueList the valueList to set
		 */
		public void setValueList(LinkedBlockingQueue<String> valueList) {
			this.valueList = valueList;
		}

		/**
		 * @return the valueList
		 */
		public LinkedBlockingQueue<String> getValueList() {
			return valueList;
		}

	}
}
