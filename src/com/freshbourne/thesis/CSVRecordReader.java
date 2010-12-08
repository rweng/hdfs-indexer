package com.freshbourne.thesis;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

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

import com.freshbourne.thesis.Index.EntryIterator;

import edu.umd.cloud9.io.ArrayListWritableComparable;


public class CSVRecordReader extends
		RecordReader<LongWritable, ArrayListWritableComparable<Text>> {
	private static final Log LOG = LogFactory.getLog(LineRecordReader.class);

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;
	private LongWritable key = null;
	private ArrayListWritableComparable<Text> value = null;
	private Text tmpInputLine = new Text();
	private static Select selectable;
	private static String delimiter = " ";
	private static Index index;
	private static String indexSavePath;
	private String[] splits;
	private EntryIterator iterator;
	
	public static void setPredicate(Select s) {
		//TODO: would like to make .select static, too, but dunno how with interfaces.
		selectable = s;
	}
	
	public static void setDelimiter(String d){ delimiter = d; }
	public static void setIndex(Index i){
		index = i;
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
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
					(int) Math.min((long) Integer.MAX_VALUE, end - start));
		}
		this.pos = start;
		
		if(indexSavePath != null){
			try{
			index = Index.load(indexSavePath);
			} catch (Exception e) {
				LOG.info("Could not load index: " + e.getMessage());
			}
		}
		
		if(index != null){
			iterator = index.getIterator();
			if(selectable != null)
				iterator.setSelect(selectable);
		}
	}

	public boolean nextKeyValue() throws IOException {
		
	    if (key == null) {
	      key = new LongWritable();
	    }
	    key.set(pos);
	    if (value == null) {
	      value = new ArrayListWritableComparable<Text>();
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
	      
	      
	      
	      this.splits = tmpInputLine.toString().split(delimiter);
			
			
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
			index.save(indexSavePath == null ? index.getSavePath() : indexSavePath);
			key = null;
			value = null;
			return false;
		}

		// put it in the Index, if already there it just returns
		if (index != null) {
			index.add(this.splits, pos - newSize);
		}

		// if the predicate is matched, return, otherwise return nextKeyValue();
		if (fromIndex || selectable.select(this.splits)) {
			for (String s : this.splits) {
				LOG.info("adding to arraylist: " + s);
				value.add(new Text(s));
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
	  public ArrayListWritableComparable<Text> getCurrentValue() {
	    return value;
	  }

	  /**
	   * Get the progress within the split
	   */
	  public float getProgress() {
	    if (start == end) {
	      return 0.0f;
	    } else {
	      return Math.min(1.0f, (pos - start) / (float)(end - start));
	    }
	  }
	  
	  public synchronized void close() throws IOException {
	    if (in != null) {
	      in.close(); 
	    }
	  }

	public static void setIndexSavePath(String string) {
		indexSavePath = string;
	}
}
