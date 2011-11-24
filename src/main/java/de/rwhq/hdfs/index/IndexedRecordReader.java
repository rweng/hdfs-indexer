package de.rwhq.hdfs.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

/** Special kind of LineRecordReader. It tries to create an index over the hdfs-file. Therefore, */
public class IndexedRecordReader extends LineRecordReader {
	private static final Log LOG = LogFactory.getLog(IndexedRecordReader.class);

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

	private Index            index;
	private Iterator<String> indexIterator;
	private boolean doneReadingFromIndex = false;


	/** {@inheritDoc} */
	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		super.initialize(genericSplit, context);

		try {
			LOG.info("genericSplit.getLocations(): " + Arrays.toString(genericSplit.getLocations()));
			LOG.info("generic Split length: " + genericSplit.getLength());
		} catch (InterruptedException e) {
				LOG.warn("error when fetching genericSplit", e);
		}

		// get conf
		Class<?> builderClass = context.getConfiguration().getClass("indexBuilder", null);
		checkNotNull(builderClass,
				"in your job configuration, you must set 'indexBuilder' to the class which should be used to build the Index");


		// try to build index
		try {
			IndexBuilder builder = (IndexBuilder) builderClass.getConstructor().newInstance();
			index = builder
					.hdfsFilePath(inputToFileSplit(genericSplit).getPath().toString())
					.jobConfiguration(context.getConfiguration())
					.inputStream(fileIn)
					.fileSplit(inputToFileSplit(genericSplit))
					.build();
		} catch (Exception e) {
			LOG.error("could not create index", e);
		}

		// try to open index
		if (index != null && !index.isOpen())
			index.open();

		// create a text object for efficiency
		value = new Text();
	}

	public boolean nextKeyValue() throws IOException {
		// if no index is set, return the value of the super method since
		// all code after this depends on index
		if (index == null) {
			return super.nextKeyValue();
		}

		// if we finished reading from index, start writing to it
		if (doneReadingFromIndex) {
			do {
				long startPos = pos;
				boolean result = super.nextKeyValue();

				if (result) {
					if (index.addLine(getCurrentValue().toString(), startPos, pos - 1)) {
						return result;
					} else {
						// next iteration
					}
				} else {
					index.close();
					return result;
				}
			} while (true);
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

	private Iterator<String> getIndexIterator() {
		if (indexIterator == null && index != null) {
			indexIterator = index.getIterator();
		}

		return indexIterator;
	}
}
