package com.freshbourne.hdfs.index;

import com.freshbourne.btree.Range;
import com.freshbourne.serializer.FixLengthSerializer;

import java.io.File;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.*;

public class BTreeIndexBuilder {
	int getCacheSize() {
		return cacheSize;
	}

	String getHdfsPath() {
		return hdfsPath;
	}

	File getIndexFolder() {
		return indexFolder;
	}

	private int cacheSize = 1000;
	private File indexFolder;
	String              indexId;
	FixLengthSerializer keySerializer;
	Comparator          comparater;
	List<Range>         defaultSearchRanges;
	public KeyExtractor keyExtractor;
	private String hdfsPath;

	public BTreeIndexBuilder cacheSize(int cacheSize) {
		checkArgument(cacheSize > 0, "cacheSize must be > 0");
		this.cacheSize = cacheSize;
		return this;
	}

	public BTreeIndexBuilder indexFolder(File folder) {
		checkNotNull(folder);
		checkArgument(folder.exists(), "indexFolder must exist");

		this.indexFolder = folder;

		return this;
	}

	public BTreeIndexBuilder hdfsFilePath(String path) {
		checkNotNull(path);

		// if hdfsFile doesn't start with /, the server name is before the path
		this.hdfsPath = path.replaceAll("^hdfs://[^/]*", "");

		return this;
	}


	public BTreeIndex build(){
		checkState(hdfsPath != null && hdfsPath.startsWith("/"), "hdfsPath must start with /");
		return new BTreeIndex(this);
	}

	public BTreeIndexBuilder indexFolder(String path) {
		return indexFolder(new File(path));
	}
}
