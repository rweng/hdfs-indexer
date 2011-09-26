package com.freshbourne.hdfs.index;

import java.io.IOException;

import com.freshbourne.hdfs.index.mapreduce.LineRecordReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class IndexedRecordReader extends LineRecordReader {
	private static final Log LOG = LogFactory.getLog(IndexedRecordReader.class);

	private SharedContainer<String, String> sharedContainer;
	private RecordReaderIndexExtension indexExtension;
	private boolean doneReadingFromIndex = false;

	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		
		super.initialize(genericSplit, context);
		indexExtension = new RecordReaderIndexExtension(this, genericSplit,
				context);
	}

	public boolean nextKeyValue() throws IOException {
		// get next value from index as long as we have
		if (!doneReadingFromIndex) {
			LOG.debug("READING FROM INDEX");
			String next = indexExtension.nextFromIndex();
			if (next != null) {
				value.set(indexExtension.getCurrentValue());
				return true;
			} else {
				doneReadingFromIndex = true;
				pos = indexExtension.getPos();
			}
		}

		boolean result = super.nextKeyValue();

        if(result)
		    indexExtension.addKeyValue(this.getCurrentKey(), this.getCurrentValue());

		return result;
	}

	public long getPos() {
		return pos;
	}
}
