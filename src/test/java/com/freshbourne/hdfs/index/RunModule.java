package com.freshbourne.hdfs.index;

public class RunModule extends CSVModule {

	@java.lang.Override protected String hdfsFile() {
		return "hdfs:///path/to/file.csv";
	}

	@java.lang.Override protected String indexRootFolder() {
		return "/tmp/hdfs-indexer-test";
	}

	@java.lang.Override protected int csvColumn() {
		return 0;
	}

	@java.lang.Override protected String delimiter() {
		return "|";
	}
}
