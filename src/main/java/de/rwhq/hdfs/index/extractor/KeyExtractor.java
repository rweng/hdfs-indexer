package de.rwhq.hdfs.index.extractor;

public interface KeyExtractor<T> {
	T extract(String line) throws ExtractionException;
	String getId();
}
