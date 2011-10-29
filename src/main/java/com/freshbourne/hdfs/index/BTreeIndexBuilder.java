package com.freshbourne.hdfs.index;

import com.freshbourne.btree.Range;
import com.freshbourne.serializer.FixLengthSerializer;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.sun.istack.internal.Nullable;

import java.io.File;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class BTreeIndexBuilder {
	private int cacheSize = 1000;
	private File indexFolder;
	String              indexId;
	FixLengthSerializer keySerializer;
	Comparator          comparater;
	List<Range>         defaultSearchRanges;
	public KeyExtractor keyExtractor;

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

	public BTreeIndex build(){
		return new BTreeIndex(this);
	}
}
