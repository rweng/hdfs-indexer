package de.rwhq.hdfs.index;

import com.sun.istack.internal.Builder;

/**
 * used by IndexedRecordReader to create an Index.
 * Must have a public no-argument constructor.
 *
 * We need a class implementing this interface for creating the index,
 * because it is not possible to set objects in Hadoop's configuration anymore.
 * Therefore, all configuration has to be done in a separate class which then get's
 * initialized in the IndexedRecordReader.
 */
public interface IndexBuilder {
	/**
	 * called in the IndexedRecordReader to set the current HdfsFile
	 * @param path to the hdfs file
	 * @return this, for chaining {@code build()}
	 */
	IndexBuilder hdfsFilePath(String path);

	/**
	 * @return the index, or null if the index could not be build
	 */
	public Index build();

	IndexBuilder recordReader(IndexedRecordReader indexedRecordReader);
}
