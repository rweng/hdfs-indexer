package com.freshbourne.hdfs.index;

import java.util.Iterator;

public interface Index {
	public Index load(String path);
	public Iterator<?> getIterator();
	public void save(String path);
	public void add(String[] splits, long offset);
}
