package de.rwhq.hdfs.index;

import de.rwhq.btree.Range;

import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;


/**
 *
 * This interface is the most abstract description of an index used in the {@code IndexedRecordReader}.
 *
 * Note: It is important that opened indexes are closed.
 * 
 */
public interface Index {

    /**
     *  Open or build the index. This method should be called before using the index.
     *
     * @throws java.io.IOException
     */
    public void open() throws IOException;

	/**
	 * forces anything cached to storage
	 */
	public void sync();


    /**
     * @return if the index was opened
     */
    public boolean isOpen();


	/**
	 * @return iterator over all lines matching the search range
	 */
	public Iterator<String> getIterator();

	/**
	 * closes the index after we are done using to it.
     */
	public void close();

    /**
     * adds a line from to the index.
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
     * @return whether the line matches the search range
     */
    boolean addLine(String line, long startPos, long endPos);

	/**
	 * @return the ranges that are covered by the index.
	 */
	public SortedSet<Range<Long>> toRanges();

	/**
	 *
	 * @param range
	 * @return iterator over a index partial range. The range can contain null (open end).
	 * The indexes search range is applied.
	 * @throws IOException
	 */
	Iterator<String> getIterator(Range<Long> range) throws IOException;
}
