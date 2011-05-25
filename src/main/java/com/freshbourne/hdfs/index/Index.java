package com.freshbourne.hdfs.index;

import java.util.Iterator;

public interface Index<K, V> {
	public Iterator<V> getIterator();
	public Iterator<V> getIterator(K start, K end);
	public void save();
	public void add(K key, V value);
	public Index<K, V> createIndex(String path);
	public String getPath();
	public void add(String[] splits, V value);
	// public Index<K,V> load(String path);
	
	/**
	 * @return a string to place in the index file which identifies file and column
	 */
	public String getIdentifier();
}
