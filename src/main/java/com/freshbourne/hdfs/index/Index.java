package com.freshbourne.hdfs.index;

import com.freshbourne.io.MustBeOpened;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;


/**
 *
 * This interface is the most abstract description of an index.
 * It usually contains one ore more storage structures to which data is written.
 *
 * The index is usually in a key-value format. There are two ways the index can be used:
 * First as primary index. In this case, the hdfs entry is completly stored in the index.
 * This would look like this:
 *
 * 25 => 2,Robin,25
 * 55 => 1,Franz,55
 * ...
 *
 * In the alternative case, a secondary index, only the position in the hdfs is stored:
 *
 * 22 => 4253564
 * 55 => 3452563
 *
 * Also, the implementation of indexes can differ.
 * Some indexes are adding values inline to the index storage, others
 * add seperate indexes and merge them together later.
 * Thus, this interface as to be as generic as possible.
 *
 * Note: It is important that opened indexes are closed. close() is the only method that
 * actually ensures that the data is persisted.
 *
 */
public interface Index {

    /**
     *  Open or create the index. This could be the files or directory for example.
     *
     * @throws java.io.IOException
     */
    public void open() throws IOException;


    /**
     * @return if the index was opened
     */
    public boolean isOpen();


    /**
     * @return if the index does alread exist or if it would be created on #open()
     */
    public boolean exists();

	/**
	 * @return iterator over all key/value pairs in the storage
	 */
	public Iterator<String> getIterator();

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
     * @param line extracted from the hdfs file
     * @param pos in the hdfs file
     */
    void addLine(String line, long pos);

	/**
	 * @return max position of the index
	 */
	long getMaxPos();
}
