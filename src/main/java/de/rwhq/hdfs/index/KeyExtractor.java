package de.rwhq.hdfs.index;

public interface KeyExtractor<T> {
	T extract(String line);
}
