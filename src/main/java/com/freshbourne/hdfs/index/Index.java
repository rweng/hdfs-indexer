package com.freshbourne.hdfs.index;

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
}
