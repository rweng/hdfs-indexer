package com.freshbourne.hdfs.index;

import java.util.Comparator;
import java.util.Iterator;

public interface Index<K, V> {

	/**
	 * @return iterator over all key/value pairs in the storage
	 */
	public Iterator<V> getIterator();

	/**
	 * @param start key
	 * @param end key
	 * @return iterator over all key/value pairs in the storage from the start key to the end key
	 */
	public Iterator<V> getIterator(K start, K end);

	/**
	 * closes the index after we are done writing to it
	 */
	public void close();

	/**
	 * adds one key value entry to the index
	 * @param key
	 * @param value
	 */
	public void add(K key, V value);

	/**
	 * @return a string to place in the index file which identifies file and column
	 */
	public String getIdentifier();

	/**
	 * Prepares the index for usage. This way, the constructor can be empty and doesn't throw any errors,
	 * and this standardized method does the work.
	 *
	 * @param indexFile
	 */
	void initialize(String indexFile);


	/**
	 * transforms an entry of the hdfs file (which can be a line, a json path, ...) into a key
	 * and value which can be fetched using getCurrentParsedKey() and getCurrentParsedValue();
	 *
	 * @param entry
	 */
	void parseEntry(String entry);

	/**
	 * utility method for parseEntry()
	 */
	String getCurrentParsedKey();


	/**
	 * utility method for parseEntry()
	 */
	String getCurrentParsedValue();

	/**
	 * returns a comparator for the keys
	 *
	 * @return comparator for keys
	 */
	Comparator<K> getKeyComparator();
}
