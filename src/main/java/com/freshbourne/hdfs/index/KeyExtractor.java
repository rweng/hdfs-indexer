package com.freshbourne.hdfs.index;

public interface KeyExtractor<T> {
	T extract(String line);
}
