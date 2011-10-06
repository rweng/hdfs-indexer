package com.freshbourne.hdfs.index;

import com.freshbourne.btree.BTreeFactory;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.File;

public class IntegerCSVIndex extends CSVIndex<Integer> {

	@Inject
	protected IntegerCSVIndex(CSVIndexBuilder<Integer> b) {
		super(b);
	}

	@Override protected Integer transformToKeyType(String key) {
		return Integer.parseInt(key);
	}

}
