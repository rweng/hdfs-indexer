package com.freshbourne.hdfs.index;

import org.apache.hadoop.io.Text;

import java.util.AbstractMap.SimpleEntry;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;


/**
 *
 * This interface is the most abstract description of an index.
 * It usually contains one ore more storage structures to which data is written.
 *
 * Note: It is important that opened indexes are closed. close() is the only method that
 * actually ensures that the data is persisted.
 *
 */
public interface Index {

	/**
	 * @return iterator over all key/value pairs in the storage
	 */
	public Iterator<SimpleEntry<String, String>> getIterator();

	/**
	 * @param start key
	 * @param end key
	 * @return iterator over all key/value pairs in the storage from the start key to the end key
	 */
	public Iterator<SimpleEntry<String, String>> getIterator(String start, String end);

	/**
	 * closes the index after we are done writing to it
     * This is the only method that actually ensures that the data is saved permanently.
	 */
	public void close();

    /**
     * adds a line from a line-based record-reader to the index.
     *
     * For an index based on a cvs file and a b-tree index, this method could
     * split the line by a delimiter (e.g ,) and then add the column-to-be-indexed as key
     * and the hole line (in case of a primary index) or the position (in case of a secondary index)
     * to the storage.
     *
     * @param line
     * @param pos
     */
    void addLine(String line, long pos);
}
