package de.rwhq.hdfs.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IndexedInputFormat extends
		FileInputFormat<LongWritable, Text> {

	private static final double SPLIT_SLOP = 1.1;   // 10% slop
	private static       Log    LOG        = LogFactory.getLog(IndexedInputFormat.class);

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {

		return new IndexedRecordReader();
	}

	/** Generate the list of files and make them into FileSplits. */
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		LOG.info("getFormatMinSplitSize(): " + getFormatMinSplitSize());
		LOG.info("getMinSplitSize(job): " + getMinSplitSize(job));

		long maxSize = getMaxSplitSize(job);

		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for (FileStatus file : listStatus(job)) {
			Path path = file.getPath();
			if (LOG.isDebugEnabled())
				LOG.debug("Path of the file: " + path);
			FileSystem fs = path.getFileSystem(job.getConfiguration());
			long length = file.getLen();
			if (LOG.isDebugEnabled())
				LOG.debug("length of the file: " + length);
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0,
					length);
			if ((length != 0) && isSplitable(job, path)) {
				long blockSize = file.getBlockSize();
				long splitSize = computeSplitSize(blockSize, minSize, maxSize);

				long bytesRemaining = length;
				while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
					int blkIndex = getBlockIndex(blkLocations, length
							- bytesRemaining);
					splits.add(new FileSplit(path, length - bytesRemaining,
							splitSize, blkLocations[blkIndex].getHosts()));
					bytesRemaining -= splitSize;
				}

				if (bytesRemaining != 0) {
					splits.add(new FileSplit(path, length - bytesRemaining,
							bytesRemaining,
							blkLocations[blkLocations.length - 1].getHosts()));
				}
			} else if (length != 0) {
				splits.add(new FileSplit(path, 0, length, blkLocations[0]
						.getHosts()));
			} else {
				// Create empty hosts array for zero length files
				splits.add(new FileSplit(path, 0, length, new String[0]));
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Total # of splits: " + splits.size());
			for (InputSplit fs : splits) {
				LOG.debug("Path: " + ((FileSplit) fs).getPath());
				LOG.debug("Start: " + ((FileSplit) fs).getStart());
				LOG.debug("Length: " + ((FileSplit) fs).getLength());
			}
		}
		return splits;
	}

	protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
		return Math.max(minSize, Math.min(maxSize, blockSize));
	}

	protected int getBlockIndex(BlockLocation[] blkLocations, long offset) {
		for (int i = 0; i < blkLocations.length; i++) {
			// is the offset inside this block?
			if ((blkLocations[i].getOffset() <= offset)
					&& (offset < blkLocations[i].getOffset()
					+ blkLocations[i].getLength())) {
				return i;
			}
		}
		BlockLocation last = blkLocations[blkLocations.length - 1];
		long fileLength = last.getOffset() + last.getLength() - 1;
		throw new IllegalArgumentException("Offset " + offset
				+ " is outside of file (0.." + fileLength + ")");
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(
				context.getConfiguration()).getCodec(file);
		return codec == null;
	}

}
