package com.freshbourne.hdfs.index;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

@Singleton
public class CSVIndexBuilder<K> extends BTreeIndexBuilder<K> {
	@Inject @Named("csvColumn") int    column;
	@Inject @Named("delimiter") String delimiter;
}