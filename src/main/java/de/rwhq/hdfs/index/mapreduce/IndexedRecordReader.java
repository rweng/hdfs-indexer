package de.rwhq.hdfs.index.mapreduce;

import de.rwhq.hdfs.index.BTreeIndexBuilder;
import de.rwhq.hdfs.index.Index;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexedRecordReader extends LineRecordReader {
	private static final Log LOG = LogFactory.getLog(IndexedRecordReader.class);

	private Index index;
	private Iterator<String> indexIterator;
	private boolean doneReadingFromIndex = false;


	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		super.initialize(genericSplit, context);

		// get conf
		Class<?> builderClass = context.getConfiguration().getClass("builder", null);
		checkNotNull(builderClass, "in your job configuration, you must set 'builder' to the class which should be used to build the Index");

		try {
			BTreeIndexBuilder builder = (BTreeIndexBuilder) builderClass.getConstructor().newInstance();
			index = builder.hdfsFilePath(inputToFileSplit(genericSplit).getPath().toString()).build();
		} catch (Exception e) {
			LOG.error("could not create index", e);
		}

		if(index != null && !index.isOpen())
			index.open();

		value = new Text();
	}

	public Iterator<String> getIndexIterator() {
		if (indexIterator == null && index != null) {
			indexIterator = index.getIterator();
		}

		return indexIterator;
	}

	private static FileSplit inputToFileSplit(InputSplit inputSplit) {
		FileSplit split;
		try {
			split = (FileSplit) inputSplit;
		} catch (Exception e) {
			throw new IllegalArgumentException(
					"InputSplit must be an instance of FileSplit");
		}
		return split;
	}

	public boolean nextKeyValue() throws IOException {
		// if no index is set, return the value of the super method since
		// all code after this depends on index
		if (index == null) {
			return super.nextKeyValue();
		}

		// if we finished reading from index, start writing to it
		if (doneReadingFromIndex) {
			boolean result = super.nextKeyValue();

			if (result) {
				index.addLine(this.getCurrentValue().toString(), pos);
			} else {
				index.close();
			}
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
