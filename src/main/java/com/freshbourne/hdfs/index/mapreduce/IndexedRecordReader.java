package com.freshbourne.hdfs.index.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import com.freshbourne.hdfs.index.Index;
import com.freshbourne.hdfs.index.IndexBuilderImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class IndexedRecordReader extends LineRecordReader {
	private static final Log LOG = LogFactory.getLog(IndexedRecordReader.class);

	private Index index;
	private Iterator<String> indexIterator;
	private boolean doneReadingFromIndex = false;


	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		super.initialize(genericSplit, context);
		index = new IndexBuilderImpl().create(genericSplit, context);
		value = new Text();
	}

	public Iterator<String> getIndexIterator() {
		if (indexIterator == null && index != null) {
			indexIterator = index.getIterator();
		}

		return indexIterator;
	}

	public boolean nextKeyValue() throws IOException {
		// if no index is set, return the value of the super method since
		// all code after this depends on index
		if (index == null) {
			if (LOG.isDebugEnabled())
				LOG.debug("index is null, returning nextKeyValue()");
			return super.nextKeyValue();
		}

		// if we finished reading from index, start writing to it
		if (doneReadingFromIndex) {
			boolean result = super.nextKeyValue();

			if (result) {
				if (LOG.isDebugEnabled())
					LOG.debug("adding to index:" + this.getCurrentValue().toString());
				index.addLine(this.getCurrentValue().toString(), pos);
			} else
				index.close();
			return result;
		}

		// get next value from index as long as we have
		if (getIndexIterator().hasNext()) {

			// get index for file if not set
			// read from index
			String next = getIndexIterator().next();
			if (LOG.isDebugEnabled())
				LOG.debug("got next: " + next);
			if (next != null) {
				value.set(next);
				return true;
			} else {
				return false;
			}
		} else {
			if (LOG.isDebugEnabled())
				LOG.debug("Replacing current pos (" + pos + ") with index.getMaxPos(" + index.getMaxPos() + ")");
			pos = index.getMaxPos();

			doneReadingFromIndex = true;
			return nextKeyValue();
		}
	}
}
