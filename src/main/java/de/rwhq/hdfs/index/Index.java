package de.rwhq.hdfs.index;

import java.io.IOException;
import java.util.Iterator;


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
public interface Index<K,V> {

    /**
     *  Open or build the index. This could be the files or directory for example.
     *
     * @throws java.io.IOException
     */
    public void open() throws IOException;

	/**
	 * forces anything cached to storage (basically like calling {@code close()} and then {@code open()}
	 */
	public void sync();


    /**
     * @return if the index was opened
     */
    public boolean isOpen();


	/**
	 * @return iterator over all lines matching the default search range
	 */
	public Iterator<String> getIterator();

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
     *
     * @param line extracted from the hdfs file
     * @param startPos in the hdfs file
     * @param endPos in the hdfs file
     * @return whether the line matches the default search range
     */
    boolean addLine(String line, long startPos, long endPos);

	/**
	 * @return max position of the index
	 */
	long getMaxPos();

	/**
	 *
	 * @param pos
	 * @return -1 if pos is not covered by the index, otherwise the end pos of this partial index
	 */
	public long partialEndForPos(long pos);
}
