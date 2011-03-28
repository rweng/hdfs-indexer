package com.freshbourne.hdfs.index;

import java.io.IOException;
import java.util.ArrayList;

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

import com.freshbourne.hdfs.index.TreeMapIndex.EntryIterator;


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
	private static String delimiter = " ";
	private static Index index;
	private String[] splits;
	private EntryIterator iterator;
	private Configuration conf;
	
	public static void setPredicate(Select s) {
		//TODO: would like to make .select static, too, but dunno how with interfaces.
		IndexedRecordReader.selectable = s;
		LOG.info("selectable set");
		if(selectable == null)
			LOG.info("bug null!!");
	}
	
	public static void setDelimiter(String d){ delimiter = d; }
	public static void setIndex(Index i){
		index = i;
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
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
		
		// try to load the index
		Class<?> c = conf.getClass("Index", null);
		if(c != null){
			try{
				index = (Index)(c.getConstructor().newInstance());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			LOG.info("Index set!");
		}
		
		
		if(index != null){
			iterator = (EntryIterator) index.getIterator();
			if(selectable != null)
				iterator.setSelect(selectable);
		}
		
		String savePath = conf.get("indexSavePath");
		if (savePath != null) {
			try {
				index = index.load(savePath);
			} catch (Exception e) {
				LOG.info("Could not load index: " + e.getMessage());
			}
		}		
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
	    
	    if(iterator != null){
	    	if(iterator.hasNext()){
	    		pos = iterator.next().getValue();
	    		fromIndex = true;
	    		LOG.info("Using index for pos" + pos);
	    	} else {
	    		pos = iterator.getHighestOffset(); // this one is read double
	    		iterator = null;
	    	}
	    }
	    
	    // we almost always break from this loop, it is only for making sure
	    // that we are in maxLineLength
	    while (pos < end) {
	    	
	      newSize = in.readLine(tmpInputLine, maxLineLength,
	                            Math.max( (int)Math.min(Integer.MAX_VALUE, end-pos),
	                                     maxLineLength) );
	      
	      LOG.info("READING LINE: " + tmpInputLine);
	      
	      
	      this.splits = tmpInputLine.toString().split(delimiter);
	      LOG.info("Splitsize: " + splits.length);
			
			
	      pos += newSize;
	      if (newSize == 0 || newSize < maxLineLength) {
	        break;
	      }

	      // line too long. try again
	      LOG.info("Skipped line of size " + newSize + " at pos " + 
	               (pos - newSize));
	    }
	    
	    // return false if we didnt read anything, end of input
		if (newSize == 0) {
			String sp = conf.get("indexSavePath");
			if (index != null && sp != null)
				index.save(sp);
			
			key = null;
			value = null;
			return false;
		}

		// put it in the Index, if already there it just returns
		if (index != null) {
			index.add(this.splits, pos - newSize);
		}
		
		if(selectable == null)
			LOG.info("selectable is null");
		//TODO: selectable is null even if set above.
		
		if(this.splits == null)
			LOG.info("splits are null");

		// if the predicate is matched, return, otherwise return nextKeyValue();
		if (fromIndex || selectable == null ||
				selectable.select(this.splits)) {
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
	      return Math.min(1.0f, (pos - start) / (float)(end - start));
	    }
	  }
	  
	  @Override
	public synchronized void close() throws IOException {
	    if (in != null) {
	      in.close(); 
	    }
	  }
}
