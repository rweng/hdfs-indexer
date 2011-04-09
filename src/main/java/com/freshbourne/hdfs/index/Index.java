package com.freshbourne.hdfs.index;

import java.util.Iterator;

public interface Index<V> {
	public Iterator<?> getIterator();
	public void save(String path);
	public void add(String[] splits, V value);
}
