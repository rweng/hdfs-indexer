package de.rwhq.hdfs.index;

import de.rwhq.btree.Range;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.hsqldb.index.RowIterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

/** Special kind of LineRecordReader. It tries to create an index over the hdfs-file. Therefore, */
public class IndexedRecordReader extends LineRecordReader {
	private static final Log                   LOG            = LogFactory.getLog(IndexedRecordReader.class);

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
	
	private              Iterator<Range<Long>> rangesIterator;
	private Range<Long>      currentRange;
	private Iterator<String> currentIterator;
	private Index            index;
	

	/** {@inheritDoc} */
	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		super.initialize(genericSplit, context);

		int mb = 1024 * 1024;
		LOG.info("Max memory: " + (Runtime.getRuntime().maxMemory() / mb));
		LOG.info("Total Memory:" + (Runtime.getRuntime().totalMemory() / mb));

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
		if (index != null) {
			if (!index.isOpen())
				index.open();

			// initialize ranges and iterator
			rangesIterator = index.toRanges().iterator();
			if(rangesIterator.hasNext())
				currentRange = rangesIterator.next();
			if(currentRange != null)
				currentIterator = index.getIterator(currentRange);
		}
		// create a text object for efficiency
		value = new Text();
	}

	public boolean nextKeyValue() throws IOException {
		// if no index is set, return the value of the super method since
		// all code after this depends on index
		if (index == null) {
			return super.nextKeyValue();
		}

		// if we cant read from the index
		String next = nextFromIndex();
		
		if(next == null){ // read from hdfs
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

		// next is set
		value.set(next);
		return true;
	}

	private String nextFromIndex() throws IOException {
		// if we can no longer read from index, currentRange gets null
		if(currentRange == null)
			return null;

		// if the currentRange did not yet start
		if(pos < currentRange.getFrom())
			return null;

		// if the current iterator does not have any more values, set to next range
		if(!currentIterator.hasNext()){
			// reset pos
			pos = currentRange.getTo() + 1;

			currentRange = rangesIterator.hasNext() ? rangesIterator.next() : null;
			currentIterator = currentRange == null ? null : index.getIterator(currentRange);
			return nextFromIndex();
		}

		// if the currentIterator has more values
		return currentIterator.next();
	}
}
