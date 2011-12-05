package de.rwhq.hdfs.index;

import de.rwhq.btree.Range;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Special kind of LineRecordReader. It tries to create an index over the hdfs-file. Therefore,
 */
public class IndexedRecordReader extends LineRecordReader {
	private static final Log LOG = LogFactory.getLog(IndexedRecordReader.class);
	private Configuration conf;

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

	private Iterator<Range<Long>> rangesIterator;
	private Range<Long> currentRange;
	private Iterator<String> currentRangeIterator;
	private Index index;
	private FileSplit split;


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {

		super.initialize(genericSplit, context);

		// we need to remember the split and configuration for later recreating the LineReader
		this.split = inputToFileSplit(genericSplit);
		this.conf = context.getConfiguration();

		// some general debugging information
		if (LOG.isDebugEnabled()) {
			try {
				LOG.debug("genericSplit.getLocations(): " + Arrays.toString(genericSplit.getLocations()));
				LOG.debug("generic Split length: " + genericSplit.getLength());
			} catch (InterruptedException e) {
				LOG.error("error when fetching genericSplit", e);
			}

		}
		int mb = 1024 * 1024;

		LOG.info("Max memory: " + (Runtime.getRuntime().maxMemory() / mb));
		LOG.info("Total Memory:" + (Runtime.getRuntime().totalMemory() / mb));


		// get the index Builder class
		Class<?> builderClass = context.getConfiguration().getClass("indexBuilder", null);
		checkNotNull(builderClass,
				"in your job configuration, you must set 'indexBuilder' to the class which should be used to build the Index");


		// try to build index
		try {
			IndexBuilder builder = (IndexBuilder) builderClass.getConstructor().newInstance();
			index = builder
					.jobConfiguration(conf)
					.inputStream(fileIn)
					.fileSplit(split)
					.build();
		} catch (Exception e) {
			LOG.error("could not create index", e);
		}

		// try to open index
		if (index != null) {
			if (!index.isOpen())
				index.open();

			// initialize ranges and iterator
			if (LOG.isDebugEnabled())
				LOG.debug("index ranges: " + index.toRanges());

			rangesIterator = index.toRanges().iterator();
			if (rangesIterator.hasNext())
				currentRange = rangesIterator.next();
			if (currentRange != null)
				currentRangeIterator = index.getIterator(currentRange);
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

		do {
			// if we cant read from the index
			String next = nextFromIndex();

			if(LOG.isDebugEnabled())
				LOG.debug("nextFromIndex() returned: " + next);

						

			if (next == null) { // read from hdfs
				long startPos = pos;
				boolean result = super.nextKeyValue();

				if (result) {
					if (index.addLine(getCurrentValue().toString(), startPos, pos - 1)) {
						return result;
					} else {
						// ignore these
					}
				} else {
					index.close();
					return result;
				}
			} else {
				value.set(next);
				return true;
			}
		} while (true);


	}

	private String nextFromIndex() throws IOException {
		// if we can no longer read from index, currentRange gets null
		if (currentRange == null)
			return null;

		if (LOG.isDebugEnabled())
			LOG.debug("nextFromIndex(): currentRange: " + currentRange + " - pos: " + pos);

		// if the currentRange did not yet start
		if (pos < currentRange.getFrom())
			return null;

		// if the current iterator does not have any more values, set to next range
		if (!currentRangeIterator.hasNext()) {
			// set pos so that it can be compared when we call nextFromIndex() later
			// only if this returns null, we are going to adjust the LineReader
			if (LOG.isDebugEnabled())
				LOG.debug("resetting pos from " + pos + " to " + (currentRange.getTo() + 1));

			pos = currentRange.getTo() + 1;

			currentRange = rangesIterator.hasNext() ? rangesIterator.next() : null;
			currentRangeIterator = currentRange == null ? null : index.getIterator(currentRange);

			String next = nextFromIndex();

			// if the next index does not directly continue, reset pos etc
			if (next == null) {
				// reset pos
				fileIn.seek(pos);

				CompressionCodec codec = compressionCodecs.getCodec(split.getPath());
				if (codec == null)
					in = new LineReader(fileIn, conf);
				else
					in = new LineReader(codec.createInputStream(fileIn), conf);
			}


			return next;
		}

		// if the currentIterator has more values
		return currentRangeIterator.next();
	}
}
