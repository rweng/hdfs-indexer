package com.freshbourne.hdfs.index;

import java.util.Iterator;

public interface Index<K, V> {
	public Iterator<V> getIterator();
	public Iterator<V> getIterator(K start, K end);
	public void save(String path);
	public void add(K key, V value);
	public void add(String[] splits, V value);
	public void load(String path);
	
	/**
	 * @return a string to place in the index file which identifies file and column
	 */
	public String getIdentifier();
}
