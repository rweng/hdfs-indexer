package com.freshbourne.hdfs.index;

import com.freshbourne.btree.BTreeFactory;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.File;
import java.util.List;

public class StringCSVIndex extends CSVIndex<String> {

	@Inject
	protected StringCSVIndex(CSVIndexBuilder<String> b) {
		super(b);
	}

	@Override protected String transformToKeyType(String key) {
		return key;
	}
}
