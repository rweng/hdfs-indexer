package de.rwhq.hdfs.index;

import com.google.common.collect.Lists;
import de.rwhq.btree.Range;
import de.rwhq.serializer.FixLengthSerializer;

import java.io.File;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.*;

public class BTreeIndexBuilder<K,V> {

	private int cacheSize = 1000;
	private File indexFolder;
	private String indexId;
	private FixLengthSerializer<K,byte[]> keySerializer;
	private Comparator<K>  comparator;
	private List<Range<K>> defaultSearchRanges;



	int getCacheSize() {
		return cacheSize;
	}

	String getHdfsPath() {
		return hdfsPath;
	}

	File getIndexFolder() {
		return indexFolder;
	}

	public Comparator getComparator() {
		return comparator;
	}

	public BTreeIndexBuilder comparator(Comparator comparator) {
		this.comparator = comparator;
		return this;
	}

	public BTreeIndexBuilder keyExtractor(KeyExtractor keyExtractor) {
		this.keyExtractor = keyExtractor;
		return this;
	}

	public KeyExtractor getKeyExtractor() {
		return keyExtractor;
	}

	public  KeyExtractor keyExtractor;
	private String       hdfsPath;

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


	public BTreeIndex build() {
		checkNotNull(hdfsPath, "hdfsPath is null");
		checkNotNull(keyExtractor, "keyExtractor is null");
		checkNotNull(keySerializer, "keySerializer is null");
		checkNotNull(comparator, "comparator is null");
		
		checkState(hdfsPath.startsWith("/"), "hdfsPath must start with /");

		return new BTreeIndex(this);
	}

	public BTreeIndexBuilder indexFolder(String path) {
		return indexFolder(new File(path));
	}

	public BTreeIndexBuilder keySerializer(FixLengthSerializer ks) {
		this.keySerializer = ks;
		return this;
	}

	public FixLengthSerializer getKeySerializer() {
		return keySerializer;
	}

	public List<Range<K>> getDefaultSearchRanges() {
		if(defaultSearchRanges == null)
			defaultSearchRanges = Lists.newArrayList();
		
		return defaultSearchRanges;
	}

	public BTreeIndexBuilder addDefaultRange(Range r){
		getDefaultSearchRanges().add(r);
		return this;
	}
}
